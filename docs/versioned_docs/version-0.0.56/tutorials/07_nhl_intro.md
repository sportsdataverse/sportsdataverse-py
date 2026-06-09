---
title: NHL tutorial
sidebar_label: NHL
sidebar_position: 9
---

# NHL intro — sportsdataverse-py

Two hockey surfaces live under `sportsdataverse.nhl`:

1. **NHL api-web (native, `nhl_*`)** — the league's own modern feed: schedule, play-by-play (`nhl_web_pbp`), boxscores, standings, rosters, player game logs, plus player tracking (`nhl_edge_*`) and the stats-rest / records APIs (`nhl_stats_rest_*`, `nhl_records_*`). These return tidy frames directly (`return_as_pandas=True`).
2. **ESPN NHL (`espn_nhl_*`)** — the same ESPN conventions used across every other league: `espn_nhl_teams`, `espn_nhl_schedule`, `espn_nhl_pbp` (a dict), `espn_nhl_standings`, etc.

Plus the **`load_nhl_*` parquet loaders** that read pre-built data releases.

R companion: [fastRhockey](https://fastRhockey.sportsdataverse.org) (NHL + PWHL). Python neighbor: [nhl-api-py](https://github.com/coreyjs/nhl-api-py). Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem).

## Setup

```sh
pip install sportsdataverse
```


```python
import polars as pl
import sportsdataverse as sdv
```

## NHL api-web (native)

The native wrappers hit the league's `api-web.nhle.com` feed. Every one accepts `return_as_pandas=True` to get a tidy frame back (the default `return_parsed=True` already shapes the JSON into rows). Native game IDs look like `2023030417` (season + game-type + sequence), which is **different** from ESPN's `401675111` for the same game.

We'll use the 2024 Stanley Cup Final Game 7 throughout: Florida Panthers 2, Edmonton Oilers 1 (June 24, 2024).

### Native: schedule

`nhl_web_schedule(date='YYYY-MM-DD')` returns the day's games with `home_team_*` / `away_team_*` columns and the native `id`.


```python
nat_sched = sdv.nhl.nhl_web_schedule(date='2024-06-24', return_as_pandas=True)
pl.from_pandas(nat_sched).select([
    'id', 'game_state',
    'home_team_abbrev', 'home_team_score',
    'away_team_abbrev', 'away_team_score',
]).head()
```




    shape: (1, 6)
    ┌────────────┬────────────┬──────────────────┬─────────────────┬─────────────────┬─────────────────┐
    │ id         ┆ game_state ┆ home_team_abbrev ┆ home_team_score ┆ away_team_abbre ┆ away_team_score │
    │ ---        ┆ ---        ┆ ---              ┆ ---             ┆ v               ┆ ---             │
    │ i64        ┆ str        ┆ str              ┆ i64             ┆ ---             ┆ i64             │
    │            ┆            ┆                  ┆                 ┆ str             ┆                 │
    ╞════════════╪════════════╪══════════════════╪═════════════════╪═════════════════╪═════════════════╡
    │ 2023030417 ┆ OFF        ┆ FLA              ┆ 2               ┆ EDM             ┆ 1               │
    └────────────┴────────────┴──────────────────┴─────────────────┴─────────────────┴─────────────────┘



### Native: play-by-play

`nhl_web_pbp(game_id=...)` returns one row per event. Columns use `snake_case` (`type_desc_key`, `time_in_period`, `period_descriptor_number`) — not ESPN dot-notation.


```python
nat_pbp = pl.from_pandas(sdv.nhl.nhl_web_pbp(game_id=2023030417, return_as_pandas=True))
print(nat_pbp.shape)
nat_pbp.select([
    'period_descriptor_number', 'time_in_period', 'type_desc_key',
    'details_event_owner_team_id', 'details_x_coord', 'details_y_coord',
]).head()
```

    (331, 48)





    shape: (5, 6)
    ┌────────────────┬────────────────┬───────────────┬────────────────┬───────────────┬───────────────┐
    │ period_descrip ┆ time_in_period ┆ type_desc_key ┆ details_event_ ┆ details_x_coo ┆ details_y_coo │
    │ tor_number     ┆ ---            ┆ ---           ┆ owner_team_id  ┆ rd            ┆ rd            │
    │ ---            ┆ str            ┆ str           ┆ ---            ┆ ---           ┆ ---           │
    │ i64            ┆                ┆               ┆ f64            ┆ f64           ┆ f64           │
    ╞════════════════╪════════════════╪═══════════════╪════════════════╪═══════════════╪═══════════════╡
    │ 1              ┆ 00:00          ┆ period-start  ┆ null           ┆ null          ┆ null          │
    │ 1              ┆ 00:00          ┆ faceoff       ┆ 22.0           ┆ 0.0           ┆ 0.0           │
    │ 1              ┆ 00:21          ┆ shot-on-goal  ┆ 22.0           ┆ 82.0          ┆ -3.0          │
    │ 1              ┆ 00:31          ┆ missed-shot   ┆ 13.0           ┆ -81.0         ┆ 30.0          │
    │ 1              ┆ 00:40          ┆ blocked-shot  ┆ 13.0           ┆ -64.0         ┆ -36.0         │
    └────────────────┴────────────────┴───────────────┴────────────────┴───────────────┴───────────────┘




```python
# Event-type mix for the game (native uses `type_desc_key`, e.g. shot-on-goal, goal, hit)
(nat_pbp
    .group_by('type_desc_key')
    .agg(pl.len().alias('events'))
    .sort('events', descending=True)
    .head(10))
```




    shape: (10, 2)
    ┌─────────────────┬────────┐
    │ type_desc_key   ┆ events │
    │ ---             ┆ ---    │
    │ str             ┆ u32    │
    ╞═════════════════╪════════╡
    │ faceoff         ┆ 59     │
    │ hit             ┆ 53     │
    │ stoppage        ┆ 52     │
    │ shot-on-goal    ┆ 42     │
    │ missed-shot     ┆ 36     │
    │ blocked-shot    ┆ 32     │
    │ giveaway        ┆ 22     │
    │ takeaway        ┆ 19     │
    │ delayed-penalty ┆ 3      │
    │ period-start    ┆ 3      │
    └─────────────────┴────────┘



### Native: boxscore

`nhl_boxscore(game_id=...)` returns one row per player (skaters + goalies) with `home_away`, `position`, and per-player stats.


```python
box = pl.from_pandas(sdv.nhl.nhl_boxscore(game_id=2023030417, return_as_pandas=True))
(box
    .filter(pl.col('position') != 'G')
    .select(['name_default', 'home_away', 'position', 'goals', 'assists', 'points', 'sog', 'toi'])
    .sort('points', descending=True)
    .head())
```




    shape: (5, 8)
    ┌──────────────┬───────────┬──────────┬───────┬─────────┬────────┬─────┬───────┐
    │ name_default ┆ home_away ┆ position ┆ goals ┆ assists ┆ points ┆ sog ┆ toi   │
    │ ---          ┆ ---       ┆ ---      ┆ ---   ┆ ---     ┆ ---    ┆ --- ┆ ---   │
    │ str          ┆ str       ┆ str      ┆ f64   ┆ f64     ┆ f64    ┆ f64 ┆ str   │
    ╞══════════════╪═══════════╪══════════╪═══════╪═════════╪════════╪═════╪═══════╡
    │ C. Verhaeghe ┆ home      ┆ C        ┆ 1.0   ┆ 1.0     ┆ 2.0    ┆ 3.0 ┆ 19:23 │
    │ M. Janmark   ┆ away      ┆ C        ┆ 1.0   ┆ 0.0     ┆ 1.0    ┆ 1.0 ┆ 12:40 │
    │ C. Ceci      ┆ away      ┆ D        ┆ 0.0   ┆ 1.0     ┆ 1.0    ┆ 1.0 ┆ 18:35 │
    │ S. Reinhart  ┆ home      ┆ C        ┆ 1.0   ┆ 0.0     ┆ 1.0    ┆ 2.0 ┆ 21:42 │
    │ A. Lundell   ┆ home      ┆ C        ┆ 0.0   ┆ 1.0     ┆ 1.0    ┆ 0.0 ┆ 14:39 │
    └──────────────┴───────────┴──────────┴───────┴─────────┴────────┴─────┴───────┘



### Native: standings

`nhl_standings(date='YYYY-MM-DD')` returns one row per team with conference/division context and points.


```python
standings = pl.from_pandas(sdv.nhl.nhl_standings(date='2024-04-15', return_as_pandas=True))
(standings
    .select(['team_name_default', 'conference_name', 'division_name', 'games_played', 'wins', 'losses', 'points'])
    .sort('points', descending=True)
    .head())
```




    shape: (5, 7)
    ┌─────────────────────┬─────────────────┬───────────────┬──────────────┬──────┬────────┬────────┐
    │ team_name_default   ┆ conference_name ┆ division_name ┆ games_played ┆ wins ┆ losses ┆ points │
    │ ---                 ┆ ---             ┆ ---           ┆ ---          ┆ ---  ┆ ---    ┆ ---    │
    │ str                 ┆ str             ┆ str           ┆ i64          ┆ i64  ┆ i64    ┆ i64    │
    ╞═════════════════════╪═════════════════╪═══════════════╪══════════════╪══════╪════════╪════════╡
    │ New York Rangers    ┆ Eastern         ┆ Metropolitan  ┆ 82           ┆ 55   ┆ 23     ┆ 114    │
    │ Carolina Hurricanes ┆ Eastern         ┆ Metropolitan  ┆ 81           ┆ 52   ┆ 22     ┆ 111    │
    │ Dallas Stars        ┆ Western         ┆ Central       ┆ 81           ┆ 51   ┆ 21     ┆ 111    │
    │ Boston Bruins       ┆ Eastern         ┆ Atlantic      ┆ 81           ┆ 47   ┆ 19     ┆ 109    │
    │ Florida Panthers    ┆ Eastern         ┆ Atlantic      ┆ 81           ┆ 51   ┆ 24     ┆ 108    │
    └─────────────────────┴─────────────────┴───────────────┴──────────────┴──────┴────────┴────────┘



### Native: roster & player game log

`nhl_roster(team=ABBREV, season=...)` lists a club's roster; `nhl_player_game_log(player_id=..., season=...)` returns a player's game-by-game line. Connor McDavid is `8478402`; the season string is `20232024`.


```python
roster = pl.from_pandas(sdv.nhl.nhl_roster(team='FLA', season=20232024, return_as_pandas=True))
roster.select(['id', 'first_name_default', 'last_name_default', 'sweater_number', 'position_code', 'shoots_catches']).head()
```




    shape: (5, 6)
    ┌─────────┬───────────────────┬──────────────────┬────────────────┬───────────────┬────────────────┐
    │ id      ┆ first_name_defaul ┆ last_name_defaul ┆ sweater_number ┆ position_code ┆ shoots_catches │
    │ ---     ┆ t                 ┆ t                ┆ ---            ┆ ---           ┆ ---            │
    │ i64     ┆ ---               ┆ ---              ┆ i64            ┆ str           ┆ str            │
    │         ┆ str               ┆ str              ┆                ┆               ┆                │
    ╞═════════╪═══════════════════╪══════════════════╪════════════════╪═══════════════╪════════════════╡
    │ 8477493 ┆ Aleksander        ┆ Barkov           ┆ 16             ┆ C             ┆ L              │
    │ 8477935 ┆ Sam               ┆ Bennett          ┆ 9              ┆ C             ┆ L              │
    │ 8479981 ┆ Jonah             ┆ Gadjovich        ┆ 12             ┆ L             ┆ L              │
    │ 8480825 ┆ Patrick           ┆ Giles            ┆ 36             ┆ R             ┆ R              │
    │ 8479367 ┆ William           ┆ Lockwood         ┆ 67             ┆ R             ┆ R              │
    └─────────┴───────────────────┴──────────────────┴────────────────┴───────────────┴────────────────┘




```python
gamelog = pl.from_pandas(sdv.nhl.nhl_player_game_log(player_id=8478402, season=20232024, return_as_pandas=True))
print(gamelog.shape)
gamelog.select(['game_date', 'opponent_abbrev', 'goals', 'assists', 'points', 'shots', 'toi']).head()
```

    (76, 22)





    shape: (5, 7)
    ┌────────────┬─────────────────┬───────┬─────────┬────────┬───────┬───────┐
    │ game_date  ┆ opponent_abbrev ┆ goals ┆ assists ┆ points ┆ shots ┆ toi   │
    │ ---        ┆ ---             ┆ ---   ┆ ---     ┆ ---    ┆ ---   ┆ ---   │
    │ str        ┆ str             ┆ i64   ┆ i64     ┆ i64    ┆ i64   ┆ str   │
    ╞════════════╪═════════════════╪═══════╪═════════╪════════╪═══════╪═══════╡
    │ 2024-04-17 ┆ ARI             ┆ 0     ┆ 0       ┆ 0      ┆ 2     ┆ 18:10 │
    │ 2024-04-15 ┆ SJS             ┆ 1     ┆ 1       ┆ 2      ┆ 3     ┆ 15:45 │
    │ 2024-04-06 ┆ CGY             ┆ 0     ┆ 2       ┆ 2      ┆ 3     ┆ 20:39 │
    │ 2024-04-05 ┆ COL             ┆ 2     ┆ 0       ┆ 2      ┆ 9     ┆ 20:11 │
    │ 2024-04-03 ┆ DAL             ┆ 0     ┆ 0       ┆ 0      ┆ 8     ┆ 20:05 │
    └────────────┴─────────────────┴───────┴─────────┴────────┴───────┴───────┘



### Native: NHL EDGE (player tracking)

`nhl_edge_*` wraps the NHL EDGE puck-and-player tracking surface. The `*_landing` calls return a wide single-row leaderboard frame; per-player `*_detail` calls return a player's tracked values alongside the league average and percentile. Here's McDavid's skating-speed detail for 2023-24.


```python
edge = pl.from_pandas(
    sdv.nhl.nhl_edge_skater_skating_speed_detail(player_id=8478402, season=20232024, return_as_pandas=True)
)
edge.select([
    'skating_speed_details_max_skating_speed_imperial',
    'skating_speed_details_max_skating_speed_league_avg_imperial',
    'skating_speed_details_bursts_over22_value',
    'skating_speed_details_bursts_over22_percentile',
])
```




    shape: (1, 4)
    ┌────────────────────────┬────────────────────────┬────────────────────────┬───────────────────────┐
    │ skating_speed_details_ ┆ skating_speed_details_ ┆ skating_speed_details_ ┆ skating_speed_details │
    │ max_skat…              ┆ max_skat…              ┆ bursts_o…              ┆ _bursts_o…            │
    │ ---                    ┆ ---                    ┆ ---                    ┆ ---                   │
    │ f64                    ┆ f64                    ┆ i64                    ┆ f64                   │
    ╞════════════════════════╪════════════════════════╪════════════════════════╪═══════════════════════╡
    │ 24.191                 ┆ 22.0904                ┆ 66                     ┆ 0.9984                │
    └────────────────────────┴────────────────────────┴────────────────────────┴───────────────────────┘



### Native: stats-rest leaders

`nhl_stats_rest_leaders_skaters(attribute=...)` taps the `api.nhle.com/stats/rest` leaderboards — a clean top-10 frame per attribute (e.g. `goals`, `points`, `assists`).


```python
leaders = pl.from_pandas(
    sdv.nhl.nhl_stats_rest_leaders_skaters(attribute='goals', return_as_pandas=True)
)
leaders.select(['player_full_name', 'player_position_code', 'team_tri_code', 'goals']).head(10)
```




    shape: (10, 4)
    ┌───────────────────┬──────────────────────┬───────────────┬───────┐
    │ player_full_name  ┆ player_position_code ┆ team_tri_code ┆ goals │
    │ ---               ┆ ---                  ┆ ---           ┆ ---   │
    │ str               ┆ str                  ┆ str           ┆ i64   │
    ╞═══════════════════╪══════════════════════╪═══════════════╪═══════╡
    │ Wayne Gretzky     ┆ C                    ┆ EDM           ┆ 92    │
    │ Wayne Gretzky     ┆ C                    ┆ EDM           ┆ 87    │
    │ Brett Hull        ┆ R                    ┆ STL           ┆ 86    │
    │ Mario Lemieux     ┆ C                    ┆ PIT           ┆ 85    │
    │ Phil Esposito     ┆ C                    ┆ BOS           ┆ 76    │
    │ Teemu Selanne     ┆ R                    ┆ WIN           ┆ 76    │
    │ Alexander Mogilny ┆ R                    ┆ BUF           ┆ 76    │
    │ Wayne Gretzky     ┆ C                    ┆ EDM           ┆ 73    │
    │ Brett Hull        ┆ R                    ┆ STL           ┆ 72    │
    │ Wayne Gretzky     ┆ C                    ┆ EDM           ┆ 71    │
    └───────────────────┴──────────────────────┴───────────────┴───────┘



## ESPN NHL (`espn_nhl_*`)

ESPN's surface follows the same conventions as every other league in the package: team-name columns are `home_display_name` / `away_display_name`, scores come back as **strings**, and `espn_nhl_pbp` returns a **dict** whose `plays` use raw ESPN dot-notation. ESPN game IDs look like `401675111`.

### ESPN: teams


```python
teams = sdv.nhl.espn_nhl_teams()
print(teams.shape)
teams.select(['team_id', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name']).head()
```

    (32, 14)





    shape: (5, 5)
    ┌─────────┬───────────────┬────────────┬───────────────────┬─────────────────────┐
    │ team_id ┆ team_location ┆ team_name  ┆ team_abbreviation ┆ team_display_name   │
    │ ---     ┆ ---           ┆ ---        ┆ ---               ┆ ---                 │
    │ str     ┆ str           ┆ str        ┆ str               ┆ str                 │
    ╞═════════╪═══════════════╪════════════╪═══════════════════╪═════════════════════╡
    │ 25      ┆ Anaheim       ┆ Ducks      ┆ ANA               ┆ Anaheim Ducks       │
    │ 1       ┆ Boston        ┆ Bruins     ┆ BOS               ┆ Boston Bruins       │
    │ 2       ┆ Buffalo       ┆ Sabres     ┆ BUF               ┆ Buffalo Sabres      │
    │ 3       ┆ Calgary       ┆ Flames     ┆ CGY               ┆ Calgary Flames      │
    │ 7       ┆ Carolina      ┆ Hurricanes ┆ CAR               ┆ Carolina Hurricanes │
    └─────────┴───────────────┴────────────┴───────────────────┴─────────────────────┘



### ESPN: schedule

`espn_nhl_schedule(dates=YYYYMMDD)`. Team names are `home_display_name` / `away_display_name`; scores are strings, so cast before doing arithmetic.


```python
schedule = sdv.nhl.espn_nhl_schedule(dates=20240624)
schedule.select([
    'id', 'home_display_name', 'away_display_name',
    pl.col('home_score').cast(pl.Int64, strict=False).alias('home_score'),
    pl.col('away_score').cast(pl.Int64, strict=False).alias('away_score'),
]).head()
```




    shape: (1, 5)
    ┌───────────┬───────────────────┬───────────────────┬────────────┬────────────┐
    │ id        ┆ home_display_name ┆ away_display_name ┆ home_score ┆ away_score │
    │ ---       ┆ ---               ┆ ---               ┆ ---        ┆ ---        │
    │ str       ┆ str               ┆ str               ┆ i64        ┆ i64        │
    ╞═══════════╪═══════════════════╪═══════════════════╪════════════╪════════════╡
    │ 401675111 ┆ Florida Panthers  ┆ Edmonton Oilers   ┆ 2          ┆ 1          │
    └───────────┴───────────────────┴───────────────────┴────────────┴────────────┘



### ESPN: play-by-play

`espn_nhl_pbp(game_id=...)` returns a **dict** (keys like `plays`, `boxscore`, `header`, ...). `pbp['plays']` is a **list of raw dicts** — build a frame with `pl.DataFrame(..., infer_schema_length=None)`. Columns use ESPN dot-notation: `period.number`, `clock.displayValue`, `scoringPlay`, `shootingPlay`, `type.text`, `coordinate.x` / `coordinate.y`.


```python
pbp = sdv.nhl.espn_nhl_pbp(game_id=401675111)
list(pbp.keys())[:8]
```




    ['gameId',
     'plays',
     'boxscore',
     'header',
     'format',
     'broadcasts',
     'videos',
     'playByPlaySource']




```python
plays = pl.DataFrame(pbp['plays'], infer_schema_length=None)
print(plays.shape)
plays.select(['period.number', 'clock.displayValue', 'text', 'type.text', 'scoringPlay']).head()
```

    (328, 74)





    shape: (5, 5)
    ┌───────────────┬────────────────────┬────────────────────────────────┬──────────────┬─────────────┐
    │ period.number ┆ clock.displayValue ┆ text                           ┆ type.text    ┆ scoringPlay │
    │ ---           ┆ ---                ┆ ---                            ┆ ---          ┆ ---         │
    │ i64           ┆ str                ┆ str                            ┆ str          ┆ bool        │
    ╞═══════════════╪════════════════════╪════════════════════════════════╪══════════════╪═════════════╡
    │ 1             ┆ 0:00               ┆ Start of 1st Period            ┆ Period Start ┆ false       │
    │ 1             ┆ 0:00               ┆ Adam Henrique faceoff won      ┆ Face Off     ┆ false       │
    │               ┆                    ┆ agai…                          ┆              ┆             │
    │ 1             ┆ 0:21               ┆ Adam Henrique Tip-In saved by  ┆ Shot         ┆ false       │
    │               ┆                    ┆ …                              ┆              ┆             │
    │ 1             ┆ 0:31               ┆ Anton Lundell Backhand Wide    ┆ Missed       ┆ false       │
    │               ┆                    ┆ Le…                            ┆              ┆             │
    │ 1             ┆ 0:40               ┆ Matthew Tkachuk shot blocked   ┆ Blocked      ┆ false       │
    │               ┆                    ┆ b…                             ┆              ┆             │
    └───────────────┴────────────────────┴────────────────────────────────┴──────────────┴─────────────┘



### ESPN: shot-event filter

Filter the ESPN play-by-play to shooting plays — the simplest analysis primitive in hockey. `shootingPlay` is a Boolean column.


```python
shots = plays.filter(pl.col('shootingPlay') == True)
shots.shape
```




    (328, 74)




```python
(shots
    .group_by('type.text')
    .agg(pl.len().alias('events'))
    .sort('events', descending=True)
    .head(10))
```




    shape: (10, 2)
    ┌────────────┬────────┐
    │ type.text  ┆ events │
    │ ---        ┆ ---    │
    │ str        ┆ u32    │
    ╞════════════╪════════╡
    │ Face Off   ┆ 59     │
    │ Hit        ┆ 53     │
    │ Stoppage   ┆ 52     │
    │ Shot       ┆ 42     │
    │ Missed     ┆ 36     │
    │ Blocked    ┆ 32     │
    │ Giveaway   ┆ 22     │
    │ Takeaway   ┆ 19     │
    │ Penalty    ┆ 3      │
    │ Period End ┆ 3      │
    └────────────┴────────┘



## Parquet loaders (`load_nhl_*`)

The `load_nhl_*` loaders read pre-built parquet data releases (fastRhockey-era schema) and return polars frames — fast for multi-season work and offline-friendly. Pass `seasons=[...]`; add `return_as_pandas=True` for a pandas frame.


```python
schedule_2024 = sdv.nhl.load_nhl_schedule(seasons=[2024])
print(schedule_2024.shape)
schedule_2024.select(['game_id', 'game_date', 'home_team_name', 'away_team_name', 'home_score', 'away_score']).head()
```

    (179, 28)





    shape: (5, 6)
    ┌────────────┬────────────┬─────────────────────┬───────────────────────┬────────────┬────────────┐
    │ game_id    ┆ game_date  ┆ home_team_name      ┆ away_team_name        ┆ home_score ┆ away_score │
    │ ---        ┆ ---        ┆ ---                 ┆ ---                   ┆ ---        ┆ ---        │
    │ i32        ┆ date       ┆ str                 ┆ str                   ┆ i32        ┆ i32        │
    ╞════════════╪════════════╪═════════════════════╪═══════════════════════╪════════════╪════════════╡
    │ 2023020176 ┆ 2023-11-07 ┆ Florida Panthers    ┆ Columbus Blue Jackets ┆ 5          ┆ 4          │
    │ 2023020177 ┆ 2023-11-07 ┆ Toronto Maple Leafs ┆ Tampa Bay Lightning   ┆ 6          ┆ 5          │
    │ 2023020178 ┆ 2023-11-07 ┆ Dallas Stars        ┆ Boston Bruins         ┆ 2          ┆ 3          │
    │ 2023020179 ┆ 2023-11-07 ┆ Vancouver Canucks   ┆ Edmonton Oilers       ┆ 6          ┆ 2          │
    │ 2023020174 ┆ 2023-11-06 ┆ Chicago Blackhawks  ┆ New Jersey Devils     ┆ 2          ┆ 4          │
    └────────────┴────────────┴─────────────────────┴───────────────────────┴────────────┴────────────┘




```python
team_box = sdv.nhl.load_nhl_team_box(seasons=[2024])
print(team_box.shape)
team_box.select(['game_id', 'team_name', 'goals', 'shots', 'hits', 'power_play_goals']).head()
```

    (358, 18)





    shape: (5, 6)
    ┌────────────┬───────────────────────┬───────┬───────┬──────┬──────────────────┐
    │ game_id    ┆ team_name             ┆ goals ┆ shots ┆ hits ┆ power_play_goals │
    │ ---        ┆ ---                   ┆ ---   ┆ ---   ┆ ---  ┆ ---              │
    │ i32        ┆ str                   ┆ i32   ┆ i32   ┆ i32  ┆ i32              │
    ╞════════════╪═══════════════════════╪═══════╪═══════╪══════╪══════════════════╡
    │ 2023020176 ┆ Columbus Blue Jackets ┆ 4     ┆ 22    ┆ 28   ┆ 1                │
    │ 2023020176 ┆ Florida Panthers      ┆ 5     ┆ 47    ┆ 8    ┆ 0                │
    │ 2023020177 ┆ Tampa Bay Lightning   ┆ 5     ┆ 31    ┆ 36   ┆ 2                │
    │ 2023020177 ┆ Toronto Maple Leafs   ┆ 6     ┆ 33    ┆ 32   ┆ 0                │
    │ 2023020178 ┆ Boston Bruins         ┆ 3     ┆ 29    ┆ 10   ┆ 1                │
    └────────────┴───────────────────────┴───────┴───────┴──────┴──────────────────┘



## Pipeline example: goals per period

Combine the ESPN play-by-play with polars: filter to scoring plays and group by period. (Game 7 ended 2-1, so this is a small, easy-to-read frame.)


```python
(plays
    .filter(pl.col('scoringPlay') == True)
    .group_by('period.number')
    .agg(pl.len().alias('goals'))
    .sort('period.number'))
```




    shape: (2, 2)
    ┌───────────────┬───────┐
    │ period.number ┆ goals │
    │ ---           ┆ ---   │
    │ i64           ┆ u32   │
    ╞═══════════════╪═══════╡
    │ 1             ┆ 2     │
    │ 2             ┆ 1     │
    └───────────────┴───────┘



## Cross-references

- R companion: [fastRhockey](https://fastRhockey.sportsdataverse.org) (NHL + PWHL)
- Data sources: NHL api-web (`api-web.nhle.com`), NHL stats-rest (`api.nhle.com`), NHL EDGE, and the ESPN NHL API
- Python alternative: [nhl-api-py](https://github.com/coreyjs/nhl-api-py)
- Plotting: matplotlib, plotnine

## Where to go next

- API docs: [`docs/docs/nhl/index.md`](../nhl/index.md)
- Women's hockey: [`10_pwhl_intro.ipynb`](10_pwhl_intro.md) (PWHL, also via fastRhockey)
- Cross-sport overview: `01_quickstart.ipynb`
