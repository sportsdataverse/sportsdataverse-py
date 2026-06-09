---
title: WBB tutorial
sidebar_label: WBB
sidebar_position: 5
---

# Women's college basketball intro — sportsdataverse-py

A tour of the NCAA women's basketball (`sdv.wbb`) submodule: teams, rosters, schedules, play-by-play, team stats, standings, conferences, and the parquet data loaders. The wrappers wrap ESPN's women's-college-basketball endpoints and return tidy [polars](https://pola.rs) frames (pass `return_as_pandas=True` for pandas).

R companion: [wehoop](https://wehoop.sportsdataverse.org). Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem).

## Setup

```sh
pip install sportsdataverse
```


```python
import polars as pl
import sportsdataverse as sdv
import sportsdataverse.wbb as wbb
```

## Teams

`espn_wbb_teams()` returns one wide row per D-I program. Note NCAA team frames carry no conference column — conference membership comes from `espn_wbb_standings()` or `espn_wbb_conferences()` below.


```python
teams = wbb.espn_wbb_teams()
print(teams.shape)
teams.select(['team_id', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name']).head()
```

    (361, 14)





    shape: (5, 5)
    ┌─────────┬───────────────────┬──────────────┬───────────────────┬────────────────────────────┐
    │ team_id ┆ team_location     ┆ team_name    ┆ team_abbreviation ┆ team_display_name          │
    │ ---     ┆ ---               ┆ ---          ┆ ---               ┆ ---                        │
    │ str     ┆ str               ┆ str          ┆ str               ┆ str                        │
    ╞═════════╪═══════════════════╪══════════════╪═══════════════════╪════════════════════════════╡
    │ 2000    ┆ Abilene Christian ┆ Wildcats     ┆ ACU               ┆ Abilene Christian Wildcats │
    │ 2005    ┆ Air Force         ┆ Falcons      ┆ AF                ┆ Air Force Falcons          │
    │ 2006    ┆ Akron             ┆ Zips         ┆ AKR               ┆ Akron Zips                 │
    │ 2010    ┆ Alabama A&M       ┆ Bulldogs     ┆ AAMU              ┆ Alabama A&M Bulldogs       │
    │ 333     ┆ Alabama           ┆ Crimson Tide ┆ ALA               ┆ Alabama Crimson Tide       │
    └─────────┴───────────────────┴──────────────┴───────────────────┴────────────────────────────┘



## Team roster

`espn_wbb_team_roster(team_id=..., season=...)` returns one row per player with unprefixed athlete columns. Here is the 2024-25 UConn Huskies (`team_id=2509`), the eventual national champions.


```python
uconn_roster = wbb.espn_wbb_team_roster(team_id=2509, season=2025)
print(uconn_roster.shape)
uconn_roster.select(
    ['athlete_id', 'full_name', 'jersey', 'position_abbreviation', 'display_height', 'display_weight']
).head(10)
```

    (13, 24)





    shape: (10, 6)
    ┌────────────┬────────────────────┬────────┬─────────────────────┬────────────────┬────────────────┐
    │ athlete_id ┆ full_name          ┆ jersey ┆ position_abbreviati ┆ display_height ┆ display_weight │
    │ ---        ┆ ---                ┆ ---    ┆ on                  ┆ ---            ┆ ---            │
    │ str        ┆ str                ┆ str    ┆ ---                 ┆ str            ┆ str            │
    │            ┆                    ┆        ┆ str                 ┆                ┆                │
    ╞════════════╪════════════════════╪════════╪═════════════════════╪════════════════╪════════════════╡
    │ 5311737    ┆ Carley Barrett     ┆ 24     ┆ G                   ┆ 5' 7"          ┆ null           │
    │ 5106182    ┆ Tara Daye          ┆ 44     ┆ G                   ┆ 5' 10"         ┆ null           │
    │ 5107710    ┆ Taylor Feldman     ┆ 5      ┆ G                   ┆ 5' 8"          ┆ null           │
    │ 5311739    ┆ Avery Gordon       ┆ 55     ┆ F                   ┆ 6' 7"          ┆ null           │
    │ 5108895    ┆ Taylor Henderson   ┆ 2      ┆ G                   ┆ 5' 11"         ┆ null           │
    │ 5311736    ┆ Hila Karsh         ┆ 8      ┆ G                   ┆ 5' 8"          ┆ null           │
    │ 5175722    ┆ McKenna Layden     ┆ 11     ┆ G                   ┆ 6' 2"          ┆ null           │
    │ 4433438    ┆ Madison Layden-Zay ┆ 33     ┆ G                   ┆ 6' 1"          ┆ null           │
    │ 5240041    ┆ Lana McCarthy      ┆ 35     ┆ F                   ┆ 6' 4"          ┆ null           │
    │ 5240040    ┆ Kendall Puryear    ┆ 22     ┆ F                   ┆ 6' 3"          ┆ null           │
    └────────────┴────────────────────┴────────┴─────────────────────┴────────────────┴────────────────┘




```python
# South Carolina Gamecocks (team_id=2579), the runners-up
scar_roster = wbb.espn_wbb_team_roster(team_id=2579, season=2025)
scar_roster.select(['athlete_id', 'full_name', 'jersey', 'position_abbreviation']).head()
```




    shape: (5, 4)
    ┌────────────┬────────────────┬────────┬───────────────────────┐
    │ athlete_id ┆ full_name      ┆ jersey ┆ position_abbreviation │
    │ ---        ┆ ---            ┆ ---    ┆ ---                   │
    │ str        ┆ str            ┆ str    ┆ str                   │
    ╞════════════╪════════════════╪════════╪═══════════════════════╡
    │ 5239100    ┆ Joyce Edwards  ┆ 8      ┆ F                     │
    │ 5174284    ┆ Tessa Johnson  ┆ 5      ┆ G                     │
    │ 5121055    ┆ Chloe Kitts    ┆ 21     ┆ F                     │
    │ 5311577    ┆ Agot Makeer    ┆ 44     ┆ G                     │
    │ 5239099    ┆ Maddy McDaniel ┆ 1      ┆ G                     │
    └────────────┴────────────────┴────────┴───────────────────────┘



## Schedule — single date

`espn_wbb_schedule(dates=YYYYMMDD)` returns one row per game. Team-name columns are `home_display_name` / `away_display_name`; `home_score` / `away_score` are **strings**, so cast before arithmetic. April 4, 2025 was the women's Final Four.


```python
final_four = wbb.espn_wbb_schedule(dates=20250404)
final_four.select(
    ['id', 'date', 'away_display_name', 'away_score', 'home_display_name', 'home_score', 'status_type_completed']
)
```




    shape: (2, 7)
    ┌───────────┬───────────────┬──────────────┬────────────┬──────────────┬────────────┬──────────────┐
    │ id        ┆ date          ┆ away_display ┆ away_score ┆ home_display ┆ home_score ┆ status_type_ │
    │ ---       ┆ ---           ┆ _name        ┆ ---        ┆ _name        ┆ ---        ┆ completed    │
    │ str       ┆ str           ┆ ---          ┆ str        ┆ ---          ┆ str        ┆ ---          │
    │           ┆               ┆ str          ┆            ┆ str          ┆            ┆ bool         │
    ╞═══════════╪═══════════════╪══════════════╪════════════╪══════════════╪════════════╪══════════════╡
    │ 401746073 ┆ 2025-04-04T23 ┆ Texas        ┆ 57         ┆ South        ┆ 74         ┆ true         │
    │           ┆ :00Z          ┆ Longhorns    ┆            ┆ Carolina     ┆            ┆              │
    │           ┆               ┆              ┆            ┆ Gamecocks    ┆            ┆              │
    │ 401746074 ┆ 2025-04-05T01 ┆ UConn        ┆ 85         ┆ UCLA Bruins  ┆ 51         ┆ true         │
    │           ┆ :30Z          ┆ Huskies      ┆            ┆              ┆            ┆              │
    └───────────┴───────────────┴──────────────┴────────────┴──────────────┴────────────┴──────────────┘



## Schedule — date range

Pass a `'YYYYMMDD-YYYYMMDD'` string to span multiple days. Here is the Final Four through the national championship (April 4–6, 2025).


```python
title_weekend = wbb.espn_wbb_schedule(dates='20250404-20250406')
title_weekend.select(
    ['id', 'date', 'away_display_name', 'away_score', 'home_display_name', 'home_score']
).with_columns(
    pl.col('home_score').cast(pl.Int64, strict=False),
    pl.col('away_score').cast(pl.Int64, strict=False),
)
```




    shape: (4, 6)
    ┌───────────┬───────────────────┬───────────────────┬────────────┬────────────────────┬────────────┐
    │ id        ┆ date              ┆ away_display_name ┆ away_score ┆ home_display_name  ┆ home_score │
    │ ---       ┆ ---               ┆ ---               ┆ ---        ┆ ---                ┆ ---        │
    │ str       ┆ str               ┆ str               ┆ i64        ┆ str                ┆ i64        │
    ╞═══════════╪═══════════════════╪═══════════════════╪════════════╪════════════════════╪════════════╡
    │ 401746073 ┆ 2025-04-04T23:00Z ┆ Texas Longhorns   ┆ 57         ┆ South Carolina     ┆ 74         │
    │           ┆                   ┆                   ┆            ┆ Gamecocks          ┆            │
    │ 401746074 ┆ 2025-04-05T01:30Z ┆ UConn Huskies     ┆ 85         ┆ UCLA Bruins        ┆ 51         │
    │ 401746075 ┆ 2025-04-06T19:00Z ┆ UConn Huskies     ┆ 82         ┆ South Carolina     ┆ 59         │
    │           ┆                   ┆                   ┆            ┆ Gamecocks          ┆            │
    │ 401762436 ┆ 2025-04-05T21:00Z ┆ Troy Trojans      ┆ 84         ┆ Buffalo Bulls      ┆ 88         │
    └───────────┴───────────────────┴───────────────────┴────────────┴────────────────────┴────────────┘



## Play-by-play

`espn_wbb_pbp(game_id=...)` returns a **dict** of game components (keys like `plays`, `boxscore`, `header`, `winprobability`, ...). The `plays` value is a list of dicts — build a frame with `pl.DataFrame(pbp['plays'], infer_schema_length=None)`. Columns use ESPN dot-notation (`period.number`, `clock.displayValue`, `type.text`, `scoringPlay`).

Game `401746075` is the 2025 national championship: South Carolina vs. UConn.


```python
pbp = wbb.espn_wbb_pbp(game_id=401746075)
list(pbp.keys())[:10]
```




    ['gameId',
     'plays',
     'winprobability',
     'boxscore',
     'header',
     'format',
     'broadcasts',
     'videos',
     'playByPlaySource',
     'standings']




```python
plays = pl.DataFrame(pbp['plays'], infer_schema_length=None)
print(plays.shape)
plays.select(['period.number', 'clock.displayValue', 'type.text', 'scoringPlay', 'text']).head()
```

    (443, 58)





    shape: (5, 5)
    ┌───────────────┬────────────────────┬───────────────────┬─────────────┬───────────────────────────┐
    │ period.number ┆ clock.displayValue ┆ type.text         ┆ scoringPlay ┆ text                      │
    │ ---           ┆ ---                ┆ ---               ┆ ---         ┆ ---                       │
    │ i64           ┆ str                ┆ str               ┆ bool        ┆ str                       │
    ╞═══════════════╪════════════════════╪═══════════════════╪═════════════╪═══════════════════════════╡
    │ 1             ┆ 10:00              ┆ Jumpball          ┆ false       ┆ Start game                │
    │ 1             ┆ 9:57               ┆ Jumpball          ┆ false       ┆ Jump Ball won by UConn    │
    │ 1             ┆ 9:57               ┆ Jumpball          ┆ false       ┆ Jump Ball lost by South   │
    │               ┆                    ┆                   ┆             ┆ Caroli…                   │
    │ 1             ┆ 9:40               ┆ JumpShot          ┆ false       ┆ Kaitlyn Chen missed Three │
    │               ┆                    ┆                   ┆             ┆ Poin…                     │
    │ 1             ┆ 9:33               ┆ Offensive Rebound ┆ false       ┆ Paige Bueckers Offensive  │
    │               ┆                    ┆                   ┆             ┆ Rebou…                    │
    └───────────────┴────────────────────┴───────────────────┴─────────────┴───────────────────────────┘




```python
# Scoring plays only, with the running score
plays.filter(pl.col('scoringPlay') == True).select(
    ['period.number', 'clock.displayValue', 'awayScore', 'homeScore', 'text']
).head(8)
```




    shape: (8, 5)
    ┌───────────────┬────────────────────┬───────────┬───────────┬─────────────────────────────────┐
    │ period.number ┆ clock.displayValue ┆ awayScore ┆ homeScore ┆ text                            │
    │ ---           ┆ ---                ┆ ---       ┆ ---       ┆ ---                             │
    │ i64           ┆ str                ┆ i64       ┆ i64       ┆ str                             │
    ╞═══════════════╪════════════════════╪═══════════╪═══════════╪═════════════════════════════════╡
    │ 1             ┆ 9:18               ┆ 0         ┆ 3         ┆ Te-Hina Paopao made Three Poin… │
    │ 1             ┆ 8:58               ┆ 2         ┆ 3         ┆ Sarah Strong made Jumper.       │
    │ 1             ┆ 8:36               ┆ 2         ┆ 5         ┆ Chloe Kitts made Jumper.        │
    │ 1             ┆ 8:13               ┆ 4         ┆ 5         ┆ Paige Bueckers made Jumper.     │
    │ 1             ┆ 7:24               ┆ 6         ┆ 5         ┆ Azzi Fudd made Jumper. Assiste… │
    │ 1             ┆ 7:00               ┆ 6         ┆ 7         ┆ Raven Johnson made Layup. Assi… │
    │ 1             ┆ 6:40               ┆ 8         ┆ 7         ┆ Kaitlyn Chen made Jumper.       │
    │ 1             ┆ 6:23               ┆ 8         ┆ 9         ┆ Bree Hall made Jumper.          │
    └───────────────┴────────────────────┴───────────┴───────────┴─────────────────────────────────┘



## Team season stats

`espn_wbb_team_stats(team_id=..., season=...)` returns a **dict keyed by category** — `{'Averages', 'Totals', 'Misc'}` — each a tidy long frame of `stat_name` / `value` rows. (ESPN's per-player NCAA season stats are usually unavailable, so team stats are the reliable season-level source here.)


```python
team_stats = wbb.espn_wbb_team_stats(team_id=2509, season=2025)
{k: v.shape for k, v in team_stats.items()}
```




    {'Averages': (9, 8), 'Totals': (30, 8), 'Misc': (6, 8)}




```python
# Per-game averages for UConn
team_stats['Averages'].select(['stat_name', 'abbreviation', 'display_value', 'value'])
```




    shape: (9, 4)
    ┌──────────────────────────┬──────────────┬───────────────┬───────────┐
    │ stat_name                ┆ abbreviation ┆ display_value ┆ value     │
    │ ---                      ┆ ---          ┆ ---           ┆ ---       │
    │ str                      ┆ str          ┆ str           ┆ f64       │
    ╞══════════════════════════╪══════════════╪═══════════════╪═══════════╡
    │ Rebounds Per Game        ┆ REB          ┆ 33.8          ┆ 33.827587 │
    │ Assist To Turnover Ratio ┆ AST/TO       ┆ 0.8           ┆ 0.8326693 │
    │ Fouls Per Game           ┆ PF           ┆ 18.2          ┆ 18.241379 │
    │ Games Played             ┆ GP           ┆ 29            ┆ 29.0      │
    │ Games Started            ┆ GS           ┆ 0             ┆ 0.0       │
    │ Minutes                  ┆ MIN          ┆ 5800          ┆ 5800.0    │
    │ Minutes Per Game         ┆ MIN          ┆ 200.0         ┆ 200.0     │
    │ Rebounds                 ┆ REB          ┆ 981           ┆ 981.0     │
    │ Rebounds                 ┆ REB          ┆ 981           ┆ 981.0     │
    └──────────────────────────┴──────────────┴───────────────┴───────────┘




```python
team_stats['Totals'].select(['stat_name', 'abbreviation', 'display_value']).head(10)
```




    shape: (10, 3)
    ┌─────────────────────────────────┬──────────────┬───────────────┐
    │ stat_name                       ┆ abbreviation ┆ display_value │
    │ ---                             ┆ ---          ┆ ---           │
    │ str                             ┆ str          ┆ str           │
    ╞═════════════════════════════════╪══════════════╪═══════════════╡
    │ Free Throw Percentage           ┆ FT%          ┆ 70.2          │
    │ Average Field Goals Made        ┆ FGM          ┆ 24.6          │
    │ Average Field Goals Attempted   ┆ FGA          ┆ 58.0          │
    │ Average 3-Point Field Goals Ma… ┆ 3PM          ┆ 5.9           │
    │ Average 3-Point Field Goals At… ┆ 3PA          ┆ 18.3          │
    │ Average Free Throws Made        ┆ FTM          ┆ 8.4           │
    │ Average Free Throws Attempted   ┆ FTA          ┆ 12.0          │
    │ Points Per Game                 ┆ PTS          ┆ 63.4          │
    │ Offensive Rebounds Per Game     ┆ OR           ┆ 8.9           │
    │ Assists Per Game                ┆ AST          ┆ 14.4          │
    └─────────────────────────────────┴──────────────┴───────────────┘



## Standings

`espn_wbb_standings(season=...)` returns one wide row per team with win/loss records, conference membership, and points-for/against.


```python
standings = wbb.espn_wbb_standings(season=2025)
print(standings.shape)
standings.select(
    ['team_display_name', 'conference_abbreviation', 'wins', 'losses', 'win_percent', 'points_for', 'points_against']
).sort('win_percent', descending=True).head(10)
```

    (362, 29)





    shape: (10, 7)
    ┌───────────────────┬──────────────────┬──────┬────────┬─────────────┬────────────┬────────────────┐
    │ team_display_name ┆ conference_abbre ┆ wins ┆ losses ┆ win_percent ┆ points_for ┆ points_against │
    │ ---               ┆ viation          ┆ ---  ┆ ---    ┆ ---         ┆ ---        ┆ ---            │
    │ str               ┆ ---              ┆ i64  ┆ i64    ┆ f64         ┆ f64        ┆ f64            │
    │                   ┆ str              ┆      ┆        ┆             ┆            ┆                │
    ╞═══════════════════╪══════════════════╪══════╪════════╪═════════════╪════════════╪════════════════╡
    │ Florida Gulf      ┆ ASUN             ┆ 18   ┆ 0      ┆ 1.0         ┆ 1367.0     ┆ 983.0          │
    │ Coast Eagles      ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ UConn Huskies     ┆ bige             ┆ 18   ┆ 0      ┆ 1.0         ┆ 1480.0     ┆ 866.0          │
    │ Norfolk State     ┆ meac             ┆ 14   ┆ 0      ┆ 1.0         ┆ 1145.0     ┆ 747.0          │
    │ Spartans          ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ Fairleigh         ┆ neast            ┆ 16   ┆ 0      ┆ 1.0         ┆ 1086.0     ┆ 805.0          │
    │ Dickinson Knights ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ South Dakota      ┆ summ             ┆ 16   ┆ 0      ┆ 1.0         ┆ 1258.0     ┆ 933.0          │
    │ State Jackrabbits ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ James Madison     ┆ belt             ┆ 18   ┆ 0      ┆ 1.0         ┆ 1358.0     ┆ 1072.0         │
    │ Dukes             ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ Grand Canyon      ┆ wac              ┆ 16   ┆ 0      ┆ 1.0         ┆ 1219.0     ┆ 894.0          │
    │ Lopes             ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ Green Bay Phoenix ┆ hor              ┆ 19   ┆ 1      ┆ 0.95        ┆ 1408.0     ┆ 1037.0         │
    │ Fairfield Stags   ┆ maac             ┆ 19   ┆ 1      ┆ 0.95        ┆ 1498.0     ┆ 1034.0         │
    │ SE Louisiana Lady ┆ land             ┆ 19   ┆ 1      ┆ 0.95        ┆ 1330.0     ┆ 1013.0         │
    │ Lions             ┆                  ┆      ┆        ┆             ┆            ┆                │
    └───────────────────┴──────────────────┴──────┴────────┴─────────────┴────────────┴────────────────┘



## Conferences

`espn_wbb_conferences()` lists the conference groups ESPN tracks, with their group ids — useful for filtering schedules and standings by league.


```python
conferences = wbb.espn_wbb_conferences()
print(conferences.shape)
conferences.select(['group_id', 'name', 'abbreviation', 'short_name']).head(12)
```

    (27, 8)





    shape: (12, 4)
    ┌──────────┬───────────────────────────┬──────────────┬────────────┐
    │ group_id ┆ name                      ┆ abbreviation ┆ short_name │
    │ ---      ┆ ---                       ┆ ---          ┆ ---        │
    │ str      ┆ str                       ┆ str          ┆ str        │
    ╞══════════╪═══════════════════════════╪══════════════╪════════════╡
    │ null     ┆ NCAA Division I           ┆ NCAA         ┆ null       │
    │ null     ┆ America East Conference   ┆ aeast        ┆ null       │
    │ null     ┆ American Conference       ┆ American     ┆ null       │
    │ null     ┆ Atlantic 10 Conference    ┆ atl10        ┆ null       │
    │ null     ┆ Atlantic Coast Conference ┆ acc          ┆ null       │
    │ …        ┆ …                         ┆ …            ┆ …          │
    │ null     ┆ Big East Conference       ┆ bige         ┆ null       │
    │ null     ┆ Big Sky Conference        ┆ bsky         ┆ null       │
    │ null     ┆ Big South Conference      ┆ bsou         ┆ null       │
    │ null     ┆ Big Ten Conference        ┆ big10        ┆ null       │
    │ null     ┆ Big West Conference       ┆ bigw         ┆ null       │
    └──────────┴───────────────────────────┴──────────────┴────────────┘



## Data loaders (parquet releases)

`load_wbb_*(seasons=[...])` read pre-built parquet releases from the [wehoop-wbb-data](https://github.com/sportsdataverse/wehoop-wbb-data) repo and return polars frames — far faster than scraping season-long history through the ESPN endpoints. Loaders include `load_wbb_schedule`, `load_wbb_team_boxscore`, `load_wbb_player_boxscore`, `load_wbb_pbp`, `load_wbb_rosters`, `load_wbb_standings`, and more (`dir(sdv.wbb)` shows the full set).


```python
schedule_2024 = wbb.load_wbb_schedule(seasons=[2024])
print(schedule_2024.shape)
schedule_2024.select(['id', 'date', 'home_display_name', 'away_display_name']).head()
```

    (5923, 84)





    shape: (5, 4)
    ┌───────────┬───────────────────┬──────────────────────────┬──────────────────────────┐
    │ id        ┆ date              ┆ home_display_name        ┆ away_display_name        │
    │ ---       ┆ ---               ┆ ---                      ┆ ---                      │
    │ i32       ┆ str               ┆ str                      ┆ str                      │
    ╞═══════════╪═══════════════════╪══════════════════════════╪══════════════════════════╡
    │ 401637613 ┆ 2024-04-07T19:00Z ┆ South Carolina Gamecocks ┆ Iowa Hawkeyes            │
    │ 401641587 ┆ 2024-04-06T19:00Z ┆ Saint Louis Billikens    ┆ Minnesota Golden Gophers │
    │ 401637612 ┆ 2024-04-06T01:32Z ┆ Iowa Hawkeyes            ┆ UConn Huskies            │
    │ 401637611 ┆ 2024-04-05T23:00Z ┆ South Carolina Gamecocks ┆ NC State Wolfpack        │
    │ 401641629 ┆ 2024-04-03T23:00Z ┆ Villanova Wildcats       ┆ Illinois Fighting Illini │
    └───────────┴───────────────────┴──────────────────────────┴──────────────────────────┘




```python
team_box_2024 = wbb.load_wbb_team_boxscore(seasons=[2024])
print(team_box_2024.shape)
team_box_2024.select(
    ['game_id', 'team_display_name', 'team_home_away', 'team_score', 'field_goal_pct', 'total_rebounds', 'assists']
).head()
```

    (11796, 56)





    shape: (5, 7)
    ┌───────────┬───────────────┬───────────────┬────────────┬───────────────┬───────────────┬─────────┐
    │ game_id   ┆ team_display_ ┆ team_home_awa ┆ team_score ┆ field_goal_pc ┆ total_rebound ┆ assists │
    │ ---       ┆ name          ┆ y             ┆ ---        ┆ t             ┆ s             ┆ ---     │
    │ i32       ┆ ---           ┆ ---           ┆ i32        ┆ ---           ┆ ---           ┆ i32     │
    │           ┆ str           ┆ str           ┆            ┆ f64           ┆ i32           ┆         │
    ╞═══════════╪═══════════════╪═══════════════╪════════════╪═══════════════╪═══════════════╪═════════╡
    │ 401637613 ┆ Iowa Hawkeyes ┆ away          ┆ 75         ┆ 39.7          ┆ 29            ┆ 13      │
    │ 401637613 ┆ South         ┆ home          ┆ 87         ┆ 47.9          ┆ 51            ┆ 16      │
    │           ┆ Carolina      ┆               ┆            ┆               ┆               ┆         │
    │           ┆ Gamecocks     ┆               ┆            ┆               ┆               ┆         │
    │ 401641587 ┆ Minnesota     ┆ away          ┆ 50         ┆ 33.9          ┆ 41            ┆ 12      │
    │           ┆ Golden        ┆               ┆            ┆               ┆               ┆         │
    │           ┆ Gophers       ┆               ┆            ┆               ┆               ┆         │
    │ 401641587 ┆ Saint Louis   ┆ home          ┆ 69         ┆ 43.3          ┆ 38            ┆ 14      │
    │           ┆ Billikens     ┆               ┆            ┆               ┆               ┆         │
    │ 401637612 ┆ UConn Huskies ┆ away          ┆ 69         ┆ 46.0          ┆ 29            ┆ 21      │
    └───────────┴───────────────┴───────────────┴────────────┴───────────────┴───────────────┴─────────┘




```python
player_box_2024 = wbb.load_wbb_player_boxscore(seasons=[2024])
print(player_box_2024.shape)
player_box_2024.select(
    ['game_id', 'athlete_display_name', 'team_short_display_name', 'minutes', 'points', 'rebounds', 'assists']
).head()
```

    (167412, 55)





    shape: (5, 7)
    ┌───────────┬──────────────────────┬───────────────────────┬─────────┬────────┬──────────┬─────────┐
    │ game_id   ┆ athlete_display_name ┆ team_short_display_na ┆ minutes ┆ points ┆ rebounds ┆ assists │
    │ ---       ┆ ---                  ┆ me                    ┆ ---     ┆ ---    ┆ ---      ┆ ---     │
    │ i32       ┆ str                  ┆ ---                   ┆ f64     ┆ i32    ┆ i32      ┆ i32     │
    │           ┆                      ┆ str                   ┆         ┆        ┆          ┆         │
    ╞═══════════╪══════════════════════╪═══════════════════════╪═════════╪════════╪══════════╪═════════╡
    │ 401637613 ┆ Hannah Stuelke       ┆ Iowa                  ┆ 27.0    ┆ 11     ┆ 3        ┆ 2       │
    │ 401637613 ┆ Sydney Affolter      ┆ Iowa                  ┆ 35.0    ┆ 12     ┆ 3        ┆ 3       │
    │ 401637613 ┆ Caitlin Clark        ┆ Iowa                  ┆ 40.0    ┆ 30     ┆ 8        ┆ 5       │
    │ 401637613 ┆ Gabbie Marshall      ┆ Iowa                  ┆ 40.0    ┆ 6      ┆ 3        ┆ 1       │
    │ 401637613 ┆ Kate Martin          ┆ Iowa                  ┆ 40.0    ┆ 16     ┆ 5        ┆ 0       │
    └───────────┴──────────────────────┴───────────────────────┴─────────┴────────┴──────────┴─────────┘



## Pipeline example: top scorers of the 2023-24 season

Load the season-long player boxscore, then aggregate with polars to find the highest per-game scorers (minimum 20 games played).


```python
top_scorers = (
    player_box_2024
    .group_by(['athlete_id', 'athlete_display_name', 'team_short_display_name'])
    .agg(
        games=pl.len(),
        total_points=pl.col('points').sum(),
        ppg=pl.col('points').mean().round(1),
    )
    .filter(pl.col('games') >= 20)
    .sort('ppg', descending=True)
    .head(10)
)
top_scorers
```




    shape: (10, 6)
    ┌────────────┬──────────────────────┬─────────────────────────┬───────┬──────────────┬──────┐
    │ athlete_id ┆ athlete_display_name ┆ team_short_display_name ┆ games ┆ total_points ┆ ppg  │
    │ ---        ┆ ---                  ┆ ---                     ┆ ---   ┆ ---          ┆ ---  │
    │ i32        ┆ str                  ┆ str                     ┆ u32   ┆ i32          ┆ f64  │
    ╞════════════╪══════════════════════╪═════════════════════════╪═══════╪══════════════╪══════╡
    │ 5177925    ┆ Ty'Mesha Reed        ┆ Miss Valley St          ┆ 31    ┆ 0            ┆ null │
    │ 5178455    ┆ Paris Bass           ┆ North Dakota            ┆ 30    ┆ 0            ┆ null │
    │ 5177967    ┆ Cameron Dill         ┆ Lamar                   ┆ 31    ┆ 0            ┆ null │
    │ 5178387    ┆ Adriana Arroyo       ┆ E Illinois              ┆ 33    ┆ 0            ┆ null │
    │ 4400119    ┆ Taylor Caldwell      ┆ Bakersfield             ┆ 29    ┆ 0            ┆ null │
    │ 4704165    ┆ Lindsey Syrek        ┆ NJIT                    ┆ 30    ┆ 0            ┆ null │
    │ 5175542    ┆ Nia Anderson         ┆ CA Baptist              ┆ 32    ┆ 0            ┆ null │
    │ 4900575    ┆ Hannah Hartley       ┆ Nevada                  ┆ 32    ┆ 0            ┆ null │
    │ 5174965    ┆ Sahana Kanagasabay   ┆ Providence              ┆ 34    ┆ 0            ┆ null │
    │ 4898948    ┆ Koi Sims             ┆ Loyola MD               ┆ 31    ┆ 0            ┆ null │
    └────────────┴──────────────────────┴─────────────────────────┴───────┴──────────────┴──────┘



## Pipeline example: best scoring offenses

Aggregate the team boxscore to rank programs by average points scored, then join back to the standings to attach each team's record.


```python
team_offense = (
    team_box_2024
    .group_by(['team_id', 'team_display_name'])
    .agg(
        games=pl.len(),
        ppg=pl.col('team_score').mean().round(1),
    )
    .filter(pl.col('games') >= 20)
    .sort('ppg', descending=True)
    .head(10)
)
team_offense
```




    shape: (10, 4)
    ┌─────────┬────────────────────────────┬───────┬──────┐
    │ team_id ┆ team_display_name          ┆ games ┆ ppg  │
    │ ---     ┆ ---                        ┆ ---   ┆ ---  │
    │ i32     ┆ str                        ┆ u32   ┆ f64  │
    ╞═════════╪════════════════════════════╪═══════╪══════╡
    │ 2294    ┆ Iowa Hawkeyes              ┆ 39    ┆ 91.0 │
    │ 99      ┆ LSU Tigers                 ┆ 37    ┆ 85.9 │
    │ 2579    ┆ South Carolina Gamecocks   ┆ 38    ┆ 85.4 │
    │ 276     ┆ Marshall Thundering Herd   ┆ 33    ┆ 85.3 │
    │ 93      ┆ Murray State Racers        ┆ 32    ┆ 84.5 │
    │ 127     ┆ Michigan State Spartans    ┆ 31    ┆ 82.8 │
    │ 213     ┆ Penn State Lady Lions      ┆ 35    ┆ 82.7 │
    │ 198     ┆ Oral Roberts Golden Eagles ┆ 32    ┆ 82.1 │
    │ 2181    ┆ Drake Bulldogs             ┆ 35    ┆ 81.2 │
    │ 2653    ┆ Troy Trojans               ┆ 34    ┆ 80.9 │
    └─────────┴────────────────────────────┴───────┴──────┘



## Cross-references

- R companion: [wehoop](https://wehoop.sportsdataverse.org)
- Data source: ESPN (women's college basketball)
- Data releases: [wehoop-wbb-data](https://github.com/sportsdataverse/wehoop-wbb-data)
- Plotting: [matplotlib](https://matplotlib.org), [plotnine](https://plotnine.org)

## Where to go next

- API docs: [`docs/docs/wbb/index.md`](../wbb/index.md)
- Next notebook: [`06_mbb_intro.ipynb`](06_mbb_intro.md) — the parallel men's college basketball surface
