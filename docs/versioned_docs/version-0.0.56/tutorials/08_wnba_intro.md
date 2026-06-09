---
title: WNBA tutorial
sidebar_label: WNBA
sidebar_position: 3
---

# WNBA intro — sportsdataverse-py

A focused tour of the `sdv.wnba` submodule: teams, rosters, schedules, play-by-play, player and team season stats, standings, the draft, and the `load_wnba_*` parquet loaders. Most calls hit ESPN's public API and return tidy [polars](https://pola.rs) frames.

R companion: [wehoop](https://wehoop.sportsdataverse.org). Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem).

## Setup

```sh
pip install sportsdataverse
```


```python
import polars as pl
import sportsdataverse as sdv
```

## Teams

`espn_wnba_teams()` returns one row per franchise. The id, location, name, and abbreviation are the keys you'll reuse to fetch rosters, schedules, and stats.


```python
teams = sdv.wnba.espn_wnba_teams()
teams.shape
```




    (15, 14)




```python
teams.select([
    'team_id', 'team_location', 'team_name',
    'team_abbreviation', 'team_display_name'
]).head(10)
```




    shape: (10, 5)
    ┌─────────┬───────────────┬───────────┬───────────────────┬────────────────────────┐
    │ team_id ┆ team_location ┆ team_name ┆ team_abbreviation ┆ team_display_name      │
    │ ---     ┆ ---           ┆ ---       ┆ ---               ┆ ---                    │
    │ str     ┆ str           ┆ str       ┆ str               ┆ str                    │
    ╞═════════╪═══════════════╪═══════════╪═══════════════════╪════════════════════════╡
    │ 20      ┆ Atlanta       ┆ Dream     ┆ ATL               ┆ Atlanta Dream          │
    │ 19      ┆ Chicago       ┆ Sky       ┆ CHI               ┆ Chicago Sky            │
    │ 18      ┆ Connecticut   ┆ Sun       ┆ CON               ┆ Connecticut Sun        │
    │ 3       ┆ Dallas        ┆ Wings     ┆ DAL               ┆ Dallas Wings           │
    │ 129689  ┆ Golden State  ┆ Valkyries ┆ GS                ┆ Golden State Valkyries │
    │ 5       ┆ Indiana       ┆ Fever     ┆ IND               ┆ Indiana Fever          │
    │ 17      ┆ Las Vegas     ┆ Aces      ┆ LV                ┆ Las Vegas Aces         │
    │ 6       ┆ Los Angeles   ┆ Sparks    ┆ LA                ┆ Los Angeles Sparks     │
    │ 8       ┆ Minnesota     ┆ Lynx      ┆ MIN               ┆ Minnesota Lynx         │
    │ 9       ┆ New York      ┆ Liberty   ┆ NY                ┆ New York Liberty       │
    └─────────┴───────────────┴───────────┴───────────────────┴────────────────────────┘



## Team roster — Las Vegas Aces

`espn_wnba_team_roster(team_id=..., season=...)` lists active players for one team. The Aces are `team_id=17`. Player columns are unprefixed (`athlete_id`, `full_name`, `jersey`, `position_abbreviation`).


```python
aces_roster = sdv.wnba.espn_wnba_team_roster(team_id=17, season=2024)
aces_roster.select([
    'athlete_id', 'full_name', 'jersey',
    'position_abbreviation', 'display_height', 'age'
]).head(12)
```




    shape: (12, 6)
    ┌────────────┬──────────────────┬────────┬───────────────────────┬────────────────┬─────┐
    │ athlete_id ┆ full_name        ┆ jersey ┆ position_abbreviation ┆ display_height ┆ age │
    │ ---        ┆ ---              ┆ ---    ┆ ---                   ┆ ---            ┆ --- │
    │ str        ┆ str              ┆ str    ┆ str                   ┆ str            ┆ i64 │
    ╞════════════╪══════════════════╪════════╪═══════════════════════╪════════════════╪═════╡
    │ 4565501    ┆ Janiah Barker    ┆ 2      ┆ F                     ┆ 6' 4"          ┆ 22  │
    │ 4433633    ┆ Kierstan Bell    ┆ 1      ┆ F                     ┆ 6' 1"          ┆ 26  │
    │ 4280892    ┆ Chennedy Carter  ┆ 23     ┆ G                     ┆ 5' 9"          ┆ 27  │
    │ 4281190    ┆ Dana Evans       ┆ 11     ┆ G                     ┆ 5' 6"          ┆ 27  │
    │ 2529122    ┆ Chelsea Gray     ┆ 12     ┆ G                     ┆ 5' 11"         ┆ 33  │
    │ …          ┆ …                ┆ …      ┆ …                     ┆ …              ┆ …   │
    │ 4398776    ┆ NaLyssa Smith    ┆ 3      ┆ F                     ┆ 6' 4"          ┆ 25  │
    │ 3099736    ┆ Stephanie Talbot ┆ 7      ┆ F                     ┆ 6' 2"          ┆ 31  │
    │ 3142086    ┆ Brianna Turner   ┆ 21     ┆ F                     ┆ 6' 3"          ┆ 29  │
    │ 3149391    ┆ A'ja Wilson      ┆ 22     ┆ C                     ┆ 6' 4"          ┆ 29  │
    │ 4065870    ┆ Jackie Young     ┆ 0      ┆ G                     ┆ 6' 0"          ┆ 28  │
    └────────────┴──────────────────┴────────┴───────────────────────┴────────────────┴─────┘



## Schedule

`espn_wnba_schedule(dates=YYYYMMDD)` pulls one day; pass a `'YYYYMMDD-YYYYMMDD'` string for a range. Team-name columns are `home_display_name` / `away_display_name`, and `home_score` / `away_score` come back as **strings** — cast before doing arithmetic.

The single date below (Oct 20, 2024) is Game 5 of the 2024 WNBA Finals.


```python
one_day = sdv.wnba.espn_wnba_schedule(dates=20241020)
one_day.select([
    'id', 'home_display_name', 'away_display_name',
    'home_score', 'away_score', 'status_type_description'
])
```




    shape: (1, 6)
    ┌───────────┬───────────────────┬───────────────────┬────────────┬────────────┬────────────────────┐
    │ id        ┆ home_display_name ┆ away_display_name ┆ home_score ┆ away_score ┆ status_type_descri │
    │ ---       ┆ ---               ┆ ---               ┆ ---        ┆ ---        ┆ ption              │
    │ str       ┆ str               ┆ str               ┆ str        ┆ str        ┆ ---                │
    │           ┆                   ┆                   ┆            ┆            ┆ str                │
    ╞═══════════╪═══════════════════╪═══════════════════╪════════════╪════════════╪════════════════════╡
    │ 401726992 ┆ New York Liberty  ┆ Minnesota Lynx    ┆ 67         ┆ 62         ┆ Final              │
    └───────────┴───────────────────┴───────────────────┴────────────┴────────────┴────────────────────┘



### Schedule over a date range

Cast the score strings to integers and derive a winning-margin column to show a small polars transform.


```python
finals = sdv.wnba.espn_wnba_schedule(dates='20241016-20241020')
finals.select([
    'id', 'home_display_name', 'away_display_name', 'home_score', 'away_score'
]).with_columns([
    pl.col('home_score').cast(pl.Int64, strict=False).alias('home_pts'),
    pl.col('away_score').cast(pl.Int64, strict=False).alias('away_pts'),
]).with_columns(
    (pl.col('home_pts') - pl.col('away_pts')).abs().alias('margin')
)
```




    shape: (3, 8)
    ┌───────────┬──────────────┬──────────────┬────────────┬────────────┬──────────┬──────────┬────────┐
    │ id        ┆ home_display ┆ away_display ┆ home_score ┆ away_score ┆ home_pts ┆ away_pts ┆ margin │
    │ ---       ┆ _name        ┆ _name        ┆ ---        ┆ ---        ┆ ---      ┆ ---      ┆ ---    │
    │ str       ┆ ---          ┆ ---          ┆ str        ┆ str        ┆ i64      ┆ i64      ┆ i64    │
    │           ┆ str          ┆ str          ┆            ┆            ┆          ┆          ┆        │
    ╞═══════════╪══════════════╪══════════════╪════════════╪════════════╪══════════╪══════════╪════════╡
    │ 401726990 ┆ Minnesota    ┆ New York     ┆ 77         ┆ 80         ┆ 77       ┆ 80       ┆ 3      │
    │           ┆ Lynx         ┆ Liberty      ┆            ┆            ┆          ┆          ┆        │
    │ 401726991 ┆ Minnesota    ┆ New York     ┆ 82         ┆ 80         ┆ 82       ┆ 80       ┆ 2      │
    │           ┆ Lynx         ┆ Liberty      ┆            ┆            ┆          ┆          ┆        │
    │ 401726992 ┆ New York     ┆ Minnesota    ┆ 67         ┆ 62         ┆ 67       ┆ 62       ┆ 5      │
    │           ┆ Liberty      ┆ Lynx         ┆            ┆            ┆          ┆          ┆        │
    └───────────┴──────────────┴──────────────┴────────────┴────────────┴──────────┴──────────┴────────┘



## Play-by-play — 2024 WNBA Finals Game 5

`espn_wnba_pbp(game_id=...)` returns a **dict** of component pieces (`plays`, `boxscore`, `header`, `winprobability`, ...). The `plays` entry is a list of raw ESPN dicts; build a frame with `pl.DataFrame(..., infer_schema_length=None)`. Its columns use raw **dot-notation** (`period.number`, `clock.displayValue`, `scoringPlay`, `type.text`).


```python
pbp = sdv.wnba.espn_wnba_pbp(game_id=401726992)
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
plays.select([
    'period.number', 'clock.displayValue', 'type.text', 'text', 'scoringPlay'
]).head(10)
```




    shape: (10, 5)
    ┌───────────────┬────────────────────┬───────────────────────┬───────────────────────┬─────────────┐
    │ period.number ┆ clock.displayValue ┆ type.text             ┆ text                  ┆ scoringPlay │
    │ ---           ┆ ---                ┆ ---                   ┆ ---                   ┆ ---         │
    │ i64           ┆ str                ┆ str                   ┆ str                   ┆ bool        │
    ╞═══════════════╪════════════════════╪═══════════════════════╪═══════════════════════╪═════════════╡
    │ 1             ┆ 10:00              ┆ Jumpball              ┆ Napheesa Collier vs.  ┆ false       │
    │               ┆                    ┆                       ┆ Jonquel J…            ┆             │
    │ 1             ┆ 9:35               ┆ Cutting Layup Shot    ┆ Napheesa Collier      ┆ true        │
    │               ┆                    ┆                       ┆ makes 3-foot …        ┆             │
    │ 1             ┆ 9:12               ┆ Pullup Jump Shot      ┆ Sabrina Ionescu       ┆ false       │
    │               ┆                    ┆                       ┆ misses 24-foot…       ┆             │
    │ 1             ┆ 9:09               ┆ Defensive Rebound     ┆ Bridget Carleton      ┆ false       │
    │               ┆                    ┆                       ┆ defensive reb…        ┆             │
    │ 1             ┆ 8:55               ┆ Personal Foul         ┆ Betnijah              ┆ false       │
    │               ┆                    ┆                       ┆ Laney-Hamilton        ┆             │
    │               ┆                    ┆                       ┆ person…               ┆             │
    │ 1             ┆ 8:50               ┆ Out of Bounds - Lost  ┆ Courtney Williams out ┆ false       │
    │               ┆                    ┆ Ball Turn…            ┆ of bound…             ┆             │
    │ 1             ┆ 8:31               ┆ Jump Shot             ┆ Breanna Stewart       ┆ false       │
    │               ┆                    ┆                       ┆ misses 13-foot…       ┆             │
    │ 1             ┆ 8:28               ┆ Defensive Rebound     ┆ Napheesa Collier      ┆ false       │
    │               ┆                    ┆                       ┆ defensive reb…        ┆             │
    │ 1             ┆ 8:07               ┆ Cutting Layup Shot    ┆ Napheesa Collier      ┆ true        │
    │               ┆                    ┆                       ┆ makes 3-foot …        ┆             │
    │ 1             ┆ 8:00               ┆ Lost Ball Turnover    ┆ Breanna Stewart lost  ┆ false       │
    │               ┆                    ┆                       ┆ ball turn…            ┆             │
    └───────────────┴────────────────────┴───────────────────────┴───────────────────────┴─────────────┘



Filter to scoring plays only to see how the lead changed.


```python
(plays
    .filter(pl.col('scoringPlay'))
    .select(['period.number', 'clock.displayValue', 'homeScore', 'awayScore', 'text'])
    .tail(8))
```




    shape: (8, 5)
    ┌───────────────┬────────────────────┬───────────┬───────────┬─────────────────────────────────┐
    │ period.number ┆ clock.displayValue ┆ homeScore ┆ awayScore ┆ text                            │
    │ ---           ┆ ---                ┆ ---       ┆ ---       ┆ ---                             │
    │ i64           ┆ str                ┆ i64       ┆ i64       ┆ str                             │
    ╞═══════════════╪════════════════════╪═══════════╪═══════════╪═════════════════════════════════╡
    │ 4             ┆ 0:5.0              ┆ 59        ┆ 60        ┆ Breanna Stewart makes free thr… │
    │ 4             ┆ 0:5.0              ┆ 60        ┆ 60        ┆ Breanna Stewart makes free thr… │
    │ 5             ┆ 4:52               ┆ 63        ┆ 60        ┆ Leonie Fiebich makes 23-foot t… │
    │ 5             ┆ 3:14               ┆ 65        ┆ 60        ┆ Nyara Sabally makes two point … │
    │ 5             ┆ 1:51               ┆ 65        ┆ 61        ┆ Kayla McBride makes free throw… │
    │ 5             ┆ 1:51               ┆ 65        ┆ 62        ┆ Kayla McBride makes free throw… │
    │ 5             ┆ 0:10.0             ┆ 66        ┆ 62        ┆ Breanna Stewart makes free thr… │
    │ 5             ┆ 0:10.0             ┆ 67        ┆ 62        ┆ Breanna Stewart makes free thr… │
    └───────────────┴────────────────────┴───────────┴───────────┴─────────────────────────────────┘



## Player season stats — Caitlin Clark

`espn_wnba_player_stats(athlete_id=..., season=...)` works for the WNBA and returns a single wide row covering ESPN's `general` / `offensive` / `defensive` stat groups (averages and totals). Caitlin Clark is `athlete_id=4433403`.


```python
cc = sdv.wnba.espn_wnba_player_stats(athlete_id=4433403, season=2024)
cc.shape
```




    (1, 138)




```python
cc.select([
    'full_name', 'team_abbreviation', 'general_games_played',
    'offensive_avg_points', 'offensive_avg_assists',
    'general_avg_rebounds', 'offensive_three_point_field_goal_pct'
])
```




    shape: (1, 7)
    ┌──────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
    │ full_name    ┆ team_abbrev ┆ general_gam ┆ offensive_a ┆ offensive_a ┆ general_avg ┆ offensive_t │
    │ ---          ┆ iation      ┆ es_played   ┆ vg_points   ┆ vg_assists  ┆ _rebounds   ┆ hree_point_ │
    │ str          ┆ ---         ┆ ---         ┆ ---         ┆ ---         ┆ ---         ┆ field_go…   │
    │              ┆ str         ┆ f64         ┆ f64         ┆ f64         ┆ f64         ┆ ---         │
    │              ┆             ┆             ┆             ┆             ┆             ┆ f64         │
    ╞══════════════╪═════════════╪═════════════╪═════════════╪═════════════╪═════════════╪═════════════╡
    │ Caitlin      ┆ IND         ┆ 40.0        ┆ 19.225      ┆ 8.425       ┆ 5.675       ┆ 34.366196   │
    │ Clark        ┆             ┆             ┆             ┆             ┆             ┆             │
    └──────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘



## Team season stats

`espn_wnba_team_stats(team_id=..., season=...)` returns a **dict** keyed by category: `{'Averages', 'Totals', 'Misc'}`. Each value is a long frame of `stat_name` / `display_value` rows — index into the dict, don't call `.head()` on it directly.


```python
aces_stats = sdv.wnba.espn_wnba_team_stats(team_id=17, season=2024)
list(aces_stats.keys())
```




    ['Averages', 'Totals', 'Misc']




```python
aces_stats['Averages'].select(['stat_name', 'abbreviation', 'display_value']).head(10)
```




    shape: (8, 3)
    ┌──────────────────────────┬──────────────┬───────────────┐
    │ stat_name                ┆ abbreviation ┆ display_value │
    │ ---                      ┆ ---          ┆ ---           │
    │ str                      ┆ str          ┆ str           │
    ╞══════════════════════════╪══════════════╪═══════════════╡
    │ Rebounds Per Game        ┆ REB          ┆ 34.1          │
    │ Assist To Turnover Ratio ┆ AST/TO       ┆ 1.9           │
    │ Fouls Per Game           ┆ PF           ┆ 16.5          │
    │ Games Played             ┆ GP           ┆ 40            │
    │ Games Started            ┆ GS           ┆ 0             │
    │ Minutes Per Game         ┆ MIN          ┆ 0.0           │
    │ Rebounds                 ┆ REB          ┆ 1364          │
    │ Rebounds                 ┆ REB          ┆ 1364          │
    └──────────────────────────┴──────────────┴───────────────┘



## Standings

`espn_wnba_standings(season=...)` returns one row per team with wins, losses, win percentage, and point differential.


```python
standings = sdv.wnba.espn_wnba_standings(season=2024)
(standings
    .select(['team_display_name', 'wins', 'losses', 'win_percent', 'point_differential'])
    .sort('win_percent', descending=True)
    .head(8))
```




    shape: (8, 5)
    ┌───────────────────┬──────┬────────┬─────────────┬────────────────────┐
    │ team_display_name ┆ wins ┆ losses ┆ win_percent ┆ point_differential │
    │ ---               ┆ ---  ┆ ---    ┆ ---         ┆ ---                │
    │ str               ┆ i64  ┆ i64    ┆ f64         ┆ f64                │
    ╞═══════════════════╪══════╪════════╪═════════════╪════════════════════╡
    │ New York Liberty  ┆ 32   ┆ 8      ┆ 0.8         ┆ 366.0              │
    │ Minnesota Lynx    ┆ 30   ┆ 10     ┆ 0.75        ┆ 255.0              │
    │ Connecticut Sun   ┆ 28   ┆ 12     ┆ 0.7         ┆ 260.0              │
    │ Las Vegas Aces    ┆ 27   ┆ 13     ┆ 0.675       ┆ 219.0              │
    │ Seattle Storm     ┆ 25   ┆ 15     ┆ 0.625       ┆ 179.0              │
    │ Indiana Fever     ┆ 20   ┆ 20     ┆ 0.5         ┆ -107.0             │
    │ Phoenix Mercury   ┆ 19   ┆ 21     ┆ 0.475       ┆ -132.0             │
    │ Atlanta Dream     ┆ 15   ┆ 25     ┆ 0.375       ┆ -110.0             │
    └───────────────────┴──────┴────────┴─────────────┴────────────────────┘



## Draft

`espn_wnba_draft(season=...)` lists every pick. The 2024 draft headlined with Caitlin Clark going first overall to the Indiana Fever.


```python
draft = sdv.wnba.espn_wnba_draft(season=2024)
draft.select([
    'overall_pick', 'team_display_name',
    'athlete_display_name', 'athlete_position_abbreviation', 'school_name'
]).head(10)
```




    shape: (10, 5)
    ┌──────────────┬───────────────────┬──────────────────────┬──────────────────────┬─────────────────┐
    │ overall_pick ┆ team_display_name ┆ athlete_display_name ┆ athlete_position_abb ┆ school_name     │
    │ ---          ┆ ---               ┆ ---                  ┆ reviation            ┆ ---             │
    │ i64          ┆ str               ┆ str                  ┆ ---                  ┆ str             │
    │              ┆                   ┆                      ┆ str                  ┆                 │
    ╞══════════════╪═══════════════════╪══════════════════════╪══════════════════════╪═════════════════╡
    │ 1            ┆ null              ┆ Caitlin Clark        ┆ null                 ┆ Hawkeyes        │
    │ 2            ┆ null              ┆ Cameron Brink        ┆ null                 ┆ Cardinal        │
    │ 3            ┆ null              ┆ Kamilla Cardoso      ┆ null                 ┆ Gamecocks       │
    │ 4            ┆ null              ┆ Rickea Jackson       ┆ null                 ┆ Lady Volunteers │
    │ 5            ┆ null              ┆ Jacy Sheldon         ┆ null                 ┆ Buckeyes        │
    │ 6            ┆ null              ┆ Aaliyah Edwards      ┆ null                 ┆ Huskies         │
    │ 7            ┆ null              ┆ Angel Reese          ┆ null                 ┆ Tigers          │
    │ 8            ┆ null              ┆ Alissa Pili          ┆ null                 ┆ Utes            │
    │ 9            ┆ null              ┆ Carla Leite          ┆ null                 ┆ France          │
    │ 10           ┆ null              ┆ Leila Lacan          ┆ null                 ┆ France          │
    └──────────────┴───────────────────┴──────────────────────┴──────────────────────┴─────────────────┘



## Bulk loaders (`load_wnba_*`)

The `load_wnba_*` functions read pre-built parquet releases (whole seasons at once) instead of calling the live API per game. They return polars frames; pass `return_as_pandas=True` for pandas. Available loaders include `load_wnba_schedule`, `load_wnba_pbp`, `load_wnba_player_boxscore`, `load_wnba_team_boxscore`, `load_wnba_player_season_stats`, `load_wnba_rosters`, `load_wnba_standings`, and `load_wnba_shots`.


```python
sched_2024 = sdv.wnba.load_wnba_schedule(seasons=[2024])
sched_2024.shape
```




    (264, 77)




```python
box_2024 = sdv.wnba.load_wnba_player_boxscore(seasons=[2024])
box_2024.select([
    'game_id', 'game_date', 'athlete_display_name',
    'team_abbreviation', 'minutes', 'points', 'rebounds', 'assists'
]).head()
```




    shape: (5, 8)
    ┌───────────┬────────────┬────────────────┬────────────────┬─────────┬────────┬──────────┬─────────┐
    │ game_id   ┆ game_date  ┆ athlete_displa ┆ team_abbreviat ┆ minutes ┆ points ┆ rebounds ┆ assists │
    │ ---       ┆ ---        ┆ y_name         ┆ ion            ┆ ---     ┆ ---    ┆ ---      ┆ ---     │
    │ i32       ┆ date       ┆ ---            ┆ ---            ┆ f64     ┆ i32    ┆ i32      ┆ i32     │
    │           ┆            ┆ str            ┆ str            ┆         ┆        ┆          ┆         │
    ╞═══════════╪════════════╪════════════════╪════════════════╪═════════╪════════╪══════════╪═════════╡
    │ 401726992 ┆ 2024-10-20 ┆ Bridget        ┆ MIN            ┆ 41.0    ┆ 3      ┆ 6        ┆ 2       │
    │           ┆            ┆ Carleton       ┆                ┆         ┆        ┆          ┆         │
    │ 401726992 ┆ 2024-10-20 ┆ Alanna Smith   ┆ MIN            ┆ 36.0    ┆ 6      ┆ 8        ┆ 2       │
    │ 401726992 ┆ 2024-10-20 ┆ Napheesa       ┆ MIN            ┆ 44.0    ┆ 22     ┆ 7        ┆ 2       │
    │           ┆            ┆ Collier        ┆                ┆         ┆        ┆          ┆         │
    │ 401726992 ┆ 2024-10-20 ┆ Kayla McBride  ┆ MIN            ┆ 43.0    ┆ 21     ┆ 5        ┆ 5       │
    │ 401726992 ┆ 2024-10-20 ┆ Courtney       ┆ MIN            ┆ 30.0    ┆ 4      ┆ 4        ┆ 3       │
    │           ┆            ┆ Williams       ┆                ┆         ┆        ┆          ┆         │
    └───────────┴────────────┴────────────────┴────────────────┴─────────┴────────┴──────────┴─────────┘



## Pipeline example: top 10 WNBA scorers of 2024

Load the full-season player boxscore parquet, drop did-not-play rows, then aggregate points and assists per player with polars.


```python
top_scorers = (
    box_2024
    .filter(~pl.col('did_not_play'))
    .group_by(['athlete_display_name', 'team_abbreviation'])
    .agg([
        pl.len().alias('games'),
        pl.col('points').sum().alias('total_points'),
        pl.col('points').mean().round(1).alias('ppg'),
        pl.col('assists').mean().round(1).alias('apg'),
    ])
    .filter(pl.col('games') >= 20)
    .sort('ppg', descending=True)
    .head(10)
)
top_scorers
```




    shape: (10, 6)
    ┌──────────────────────┬───────────────────┬───────┬──────────────┬──────┬─────┐
    │ athlete_display_name ┆ team_abbreviation ┆ games ┆ total_points ┆ ppg  ┆ apg │
    │ ---                  ┆ ---               ┆ ---   ┆ ---          ┆ ---  ┆ --- │
    │ str                  ┆ str               ┆ u32   ┆ i32          ┆ f64  ┆ f64 │
    ╞══════════════════════╪═══════════════════╪═══════╪══════════════╪══════╪═════╡
    │ A'ja Wilson          ┆ LV                ┆ 44    ┆ 1149         ┆ 26.1 ┆ 2.4 │
    │ Arike Ogunbowale     ┆ DAL               ┆ 38    ┆ 845          ┆ 22.2 ┆ 5.1 │
    │ Napheesa Collier     ┆ MIN               ┆ 47    ┆ 1000         ┆ 21.3 ┆ 3.4 │
    │ Kahleah Copper       ┆ PHX               ┆ 39    ┆ 811          ┆ 20.8 ┆ 2.3 │
    │ Breanna Stewart      ┆ NY                ┆ 50    ┆ 1014         ┆ 20.3 ┆ 3.5 │
    │ Kelsey Mitchell      ┆ IND               ┆ 42    ┆ 805          ┆ 19.2 ┆ 1.9 │
    │ Caitlin Clark        ┆ IND               ┆ 42    ┆ 805          ┆ 19.2 ┆ 8.4 │
    │ Jewell Loyd          ┆ SEA               ┆ 39    ┆ 744          ┆ 19.1 ┆ 3.6 │
    │ Sabrina Ionescu      ┆ NY                ┆ 50    ┆ 900          ┆ 18.0 ┆ 5.9 │
    │ Brittney Griner      ┆ PHX               ┆ 32    ┆ 568          ┆ 17.8 ┆ 2.2 │
    └──────────────────────┴───────────────────┴───────┴──────────────┴──────┴─────┘



## Cross-references

- R companion: [wehoop](https://wehoop.sportsdataverse.org)
- Data source: ESPN, WNBA Stats API
- Stats-API alternative (Python): [nba_api](https://github.com/swar/nba_api) (also covers the WNBA)
- Plotting: matplotlib, plotnine

## Where to go next

- API docs: `docs/docs/wnba/index.md`
- Next notebook: `09_mlb_intro.ipynb`
