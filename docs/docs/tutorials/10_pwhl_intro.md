---
title: PWHL tutorial
sidebar_label: PWHL
sidebar_position: 10
---

# 🏒 The PWHL with `sportsdataverse-py`

Welcome to **professional women's hockey**! The Professional Women's Hockey League (**PWHL**) dropped its first puck in January 2024 with six clubs — Boston, Minnesota, Montréal, New York, Ottawa and Toronto — and it's been must-watch hockey ever since. 🎉

`sportsdataverse.pwhl` gives you the whole league two ways:

1. 📦 **`load_pwhl_*` release loaders** — fast, reliable parquet snapshots (schedules, boxscores, play-by-play, scoring & penalty summaries, rosters). Perfect for season-long analysis, and they work great offline.
2. 🛰️ **`pwhl_*` live wrappers + analytics** — straight off the HockeyTech stats feed (standings, leaders, rosters, stats, single-game PBP) **plus** derived on-ice metrics (Corsi, time-on-ice, shifts).

And the best part: **no API key needed** — the public HockeyTech client key ships with the package. R companion: [fastRhockey](https://fastRhockey.sportsdataverse.org). Let's drop the puck! 🥅

## 🧰 The toolbox

Everything returns a tidy **polars** `DataFrame` by default — pass `return_as_pandas=True` for pandas. The 📦 **loaders** read pre-built release parquets (one season per call); the 🛰️ **live** wrappers hit the HockeyTech API in real time. Both are *premium* PWHL sources. Click any name for the full reference:

| Function | What it gives you | Source |
|---|---|---|
| [`load_pwhl_schedule`](../pwhl/reference/loaders.md#load_pwhl_schedules) | Games + results, one row per game | 📦 loader |
| [`load_pwhl_rosters`](../pwhl/reference/loaders.md#load_pwhl_rosters) | One row per player per team (skaters + goalies) | 📦 loader |
| [`load_pwhl_skater_box`](../pwhl/reference/loaders.md#load_pwhl_skater_boxscores) | Skater boxscore, one row per player per game | 📦 loader |
| [`load_pwhl_goalie_box`](../pwhl/reference/loaders.md#load_pwhl_goalie_boxscores) | Goalie boxscore (saves, shots against, GAA inputs) | 📦 loader |
| [`load_pwhl_team_box`](../pwhl/reference/loaders.md#load_pwhl_team_boxscores) | Team boxscore (shots, PP, faceoffs) | 📦 loader |
| [`load_pwhl_pbp`](../pwhl/reference/loaders.md#load_pwhl_pbp) | Event-level play-by-play (wide, with coordinates) | 📦 loader |
| [`load_pwhl_scoring_summary`](../pwhl/reference/loaders.md#load_pwhl_scoring_summary) | Tidy goal log (scorer + assists + situation flags) | 📦 loader |
| [`load_pwhl_penalty_summary`](../pwhl/reference/loaders.md#load_pwhl_penalty_summary) | Tidy penalty log (infraction, minutes, who took it) | 📦 loader |
| [`load_pwhl_shots_by_period`](../pwhl/reference/loaders.md#load_pwhl_shots_by_period) | Per-period shot & goal totals per game | 📦 loader |
| [`load_pwhl_three_stars`](../pwhl/reference/loaders.md#load_pwhl_three_stars) | Post-game three-star selections | 📦 loader |
| [`pwhl_schedule`](../pwhl/reference/additional.md#pwhl_schedule) | Live schedule, one row per game | 🛰️ live |
| [`pwhl_standings`](../pwhl/reference/additional.md#pwhl_standings) | Live standings, one row per team | 🛰️ live |
| [`pwhl_teams`](../pwhl/reference/additional.md#pwhl_teams) | Teams in a season (grab `team_id`s) | 🛰️ live |
| [`pwhl_team_roster`](../pwhl/reference/additional.md#pwhl_team_roster) | A team's roster | 🛰️ live |
| [`pwhl_leaders`](../pwhl/reference/additional.md#pwhl_leaders) | Statistical leaders | 🛰️ live |
| [`pwhl_stats`](../pwhl/reference/additional.md#pwhl_stats) | Aggregate skater/goalie stats | 🛰️ live |
| [`pwhl_player_search`](../pwhl/reference/additional.md#pwhl_player_search) | Find a player_id by name | 🛰️ live |
| [`pwhl_player_stats`](../pwhl/reference/additional.md#pwhl_player_stats) | A player's season-by-season stat lines | 🛰️ live |
| [`pwhl_pbp`](../pwhl/reference/additional.md#pwhl_pbp) | Enriched single-game play-by-play | 🛰️ live |
| [`pwhl_game_corsi`](../pwhl/reference/additional.md#pwhl_game_corsi) | On-ice Corsi / Fenwick per player | 🛰️ live |
| [`pwhl_player_toi`](../pwhl/reference/additional.md#pwhl_player_toi) | Time-on-ice per player | 🛰️ live |
| [`pwhl_game_shifts`](../pwhl/reference/additional.md#pwhl_game_shifts) | Raw shift stints | 🛰️ live |
| [`most_recent_pwhl_season`](../pwhl/reference/additional.md#most_recent_pwhl_season) · [`pwhl_season_id`](../pwhl/reference/additional.md#pwhl_season_id) | Season helpers | 🛰️ live |


## 🔌 Setup

```sh
pip install sportsdataverse
```

No key, no config — just import and go.


```python
import polars as pl
import sportsdataverse.pwhl as pwhl

# The inaugural season is 2024; this helper tracks the latest known season.
print("most recent PWHL season:", pwhl.most_recent_pwhl_season())
```

    most recent PWHL season: 2027


The 🛰️ **live** HockeyTech feed is seasonal and occasionally rate-limited, so a tiny `safe()` helper runs those calls defensively — you get the frame when the feed is up, and a friendly one-liner when it isn't (never a scary traceback). The 📦 **loaders** read release parquets and are rock-solid, so they don't need the wrapper. 🛟


```python
def safe(label, thunk):
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None
```

## 📅 The schedule (loader)

[`load_pwhl_schedule`](../pwhl/reference/loaders.md#load_pwhl_schedules) returns one row per game with the result and a set of flag/URL columns pointing at the per-game feeds. Pass `seasons=[2024]` (a list — you can stack multiple seasons). ⚠️ Heads up: `home_score`/`away_score` come back as **strings**, so cast them before doing arithmetic.


```python
schedule = pwhl.load_pwhl_schedule(seasons=[2024])
schedule.shape
```




    (85, 29)




```python
schedule.select([
    'game_id', 'game_date', 'home_team', 'away_team',
    'home_score', 'away_score', 'winner', 'game_type',
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



## 👥 Rosters (loader)

[`load_pwhl_rosters`](../pwhl/reference/loaders.md#load_pwhl_rosters) gives one row per player per team, split into skaters and goalies via the `player_type` column.


```python
rosters = pwhl.load_pwhl_rosters(seasons=[2024])
rosters.select([
    'team', 'team_abbr', 'player_type', 'first_name', 'last_name',
    'jersey_number', 'position',
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



## 📊 Boxscores (loader)

Boxscores come in three flavours — `team_box`, `skater_box`, and `goalie_box` — each one row per team/player per game.

| Function | One row per… |
|---|---|
| [`load_pwhl_team_box`](../pwhl/reference/loaders.md#load_pwhl_team_boxscores) | team per game |
| [`load_pwhl_skater_box`](../pwhl/reference/loaders.md#load_pwhl_skater_boxscores) | skater per game |
| [`load_pwhl_goalie_box`](../pwhl/reference/loaders.md#load_pwhl_goalie_boxscores) | goalie per game |



```python
skater_box = pwhl.load_pwhl_skater_box(seasons=[2024])
skater_box.select([
    'game_id', 'first_name', 'last_name', 'position',
    'goals', 'assists', 'points', 'shots', 'plus_minus', 'time_on_ice',
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
    'saves', 'shots_against', 'goals_against', 'time_on_ice',
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



## 🎬 Play-by-play (loader)

[`load_pwhl_pbp`](../pwhl/reference/loaders.md#load_pwhl_pbp) returns a wide event log. The `event` column tags each row as `faceoff`, `shot`, `goal`, or `penalty` — and there are several coordinate systems (`x_coord`/`y_coord` plus rink-normalized `*_fixed` / `*_right` variants) for drawing rink plots.


```python
pbp = pwhl.load_pwhl_pbp(seasons=[2024])
pbp.shape
```




    (14186, 104)




```python
(pbp
    .group_by('event')
    .agg(pl.len().alias('events'))
    .sort('events', descending=True))
```




    shape: (9, 2)
    ┌───────────────┬────────┐
    │ event         ┆ events │
    │ ---           ┆ ---    │
    │ str           ┆ u32    │
    ╞═══════════════╪════════╡
    │ shot          ┆ 4922   │
    │ faceoff       ┆ 4631   │
    │ hit           ┆ 2123   │
    │ blocked_shot  ┆ 1243   │
    │ penalty       ┆ 518    │
    │ goal          ┆ 385    │
    │ goalie_change ┆ 354    │
    │ shootout      ┆ 7      │
    │ penaltyshot   ┆ 3      │
    └───────────────┴────────┘



## 🍳 Cookbook: common PWHL tasks

Now the fun part — a baker's dozen of recipes you'll reach for constantly. Recipes **1–11** lean on the rock-solid 📦 loaders (great offline); recipes **12–13** tour the 🛰️ live wrappers, wrapped in `safe()` so an offseason or a flaky feed never breaks your run. Every recipe ends in a tidy, ready-to-read frame.

### Recipe 1 — Standings from the schedule 🏆

No loader is needed for a quick standings table: the schedule's `winner` column makes a regular-season win count a one-liner.


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



### Recipe 2 — Season scoring leaders 🥇

Aggregate the skater boxscore across every game to build a points leaderboard — the inaugural-season top of the table.


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
    │ Brianne      ┆ Jenner          ┆ 9     ┆ 11      ┆ 20     │
    │ Kendall      ┆ Coyne Schofield ┆ 7     ┆ 13      ┆ 20     │
    │ Erin         ┆ Ambrose         ┆ 4     ┆ 16      ┆ 20     │
    └──────────────┴─────────────────┴───────┴─────────┴────────┘



### Recipe 3 — Goalie save-percentage leaders 🧤

Sum saves and shots-against from the goalie boxscore, then compute a season save percentage. We require a minimum shot volume so a one-game cameo doesn't top the list.


```python
(goalie_box
    .group_by(['player_id', 'first_name', 'last_name'])
    .agg(
        pl.col('saves').sum().alias('saves'),
        pl.col('shots_against').sum().alias('shots_against'),
        pl.col('goals_against').sum().alias('goals_against'),
    )
    .filter(pl.col('shots_against') >= 100)
    .with_columns(
        (pl.col('saves') / pl.col('shots_against')).round(3).alias('save_pct')
    )
    .sort('save_pct', descending=True)
    .select(['first_name', 'last_name', 'shots_against', 'goals_against', 'save_pct'])
    .head(10))
```




    shape: (10, 5)
    ┌────────────┬────────────┬───────────────┬───────────────┬──────────┐
    │ first_name ┆ last_name  ┆ shots_against ┆ goals_against ┆ save_pct │
    │ ---        ┆ ---        ┆ ---           ┆ ---           ┆ ---      │
    │ str        ┆ str        ┆ i32           ┆ i32           ┆ f64      │
    ╞════════════╪════════════╪═══════════════╪═══════════════╪══════════╡
    │ Elaine     ┆ Chuli      ┆ 253           ┆ 13            ┆ 0.949    │
    │ Aerin      ┆ Frankel    ┆ 790           ┆ 49            ┆ 0.938    │
    │ Kristen    ┆ Campbell   ┆ 718           ┆ 48            ┆ 0.933    │
    │ Corinne    ┆ Schroeder  ┆ 511           ┆ 36            ┆ 0.93     │
    │ Nicole     ┆ Hensley    ┆ 492           ┆ 37            ┆ 0.925    │
    │ Maddie     ┆ Rooney     ┆ 362           ┆ 27            ┆ 0.925    │
    │ Ann-Renée  ┆ Desbiens   ┆ 580           ┆ 44            ┆ 0.924    │
    │ Emerance   ┆ Maschmeyer ┆ 599           ┆ 51            ┆ 0.915    │
    │ Abbey      ┆ Levy       ┆ 254           ┆ 24            ┆ 0.906    │
    │ Emma       ┆ Söderberg  ┆ 170           ┆ 17            ┆ 0.9      │
    └────────────┴────────────┴───────────────┴───────────────┴──────────┘



### Recipe 4 — Biggest blowouts of the season 💥

Cast the string scores to integers, compute the margin, and sort — the season's most lopsided games fall right out.


```python
(schedule
    .with_columns(
        pl.col('home_score').cast(pl.Int32),
        pl.col('away_score').cast(pl.Int32),
    )
    .with_columns(
        (pl.col('home_score') - pl.col('away_score')).abs().alias('margin')
    )
    .sort('margin', descending=True)
    .select(['game_date', 'home_team', 'home_score',
             'away_score', 'away_team', 'winner', 'margin'])
    .head(10))
```




    shape: (10, 7)
    ┌─────────────┬───────────┬────────────┬────────────┬───────────┬───────────┬────────┐
    │ game_date   ┆ home_team ┆ home_score ┆ away_score ┆ away_team ┆ winner    ┆ margin │
    │ ---         ┆ ---       ┆ ---        ┆ ---        ┆ ---       ┆ ---       ┆ ---    │
    │ str         ┆ str       ┆ i32        ┆ i32        ┆ str       ┆ str       ┆ i32    │
    ╞═════════════╪═══════════╪════════════╪════════════╪═══════════╪═══════════╪════════╡
    │ Wed, May 8  ┆ Toronto   ┆ 4          ┆ 0          ┆ Minnesota ┆ Toronto   ┆ 4      │
    │ Wed, Mar 13 ┆ Minnesota ┆ 4          ┆ 0          ┆ Boston    ┆ Minnesota ┆ 4      │
    │ Sun, Apr 28 ┆ New York  ┆ 2          ┆ 6          ┆ Toronto   ┆ Toronto   ┆ 4      │
    │ Sat, Mar 16 ┆ Minnesota ┆ 5          ┆ 1          ┆ New York  ┆ Minnesota ┆ 4      │
    │ Sat, Jan 13 ┆ Toronto   ┆ 1          ┆ 5          ┆ Ottawa    ┆ Ottawa    ┆ 4      │
    │ Sat, Apr 20 ┆ Ottawa    ┆ 4          ┆ 0          ┆ Minnesota ┆ Ottawa    ┆ 4      │
    │ Mon, Jan 1  ┆ Toronto   ┆ 0          ┆ 4          ┆ New York  ┆ New York  ┆ 4      │
    │ Wed, May 29 ┆ Boston    ┆ 0          ┆ 3          ┆ Minnesota ┆ Minnesota ┆ 3      │
    │ Wed, May 1  ┆ Toronto   ┆ 4          ┆ 1          ┆ Minnesota ┆ Toronto   ┆ 3      │
    │ Wed, Mar 20 ┆ New York  ┆ 0          ┆ 3          ┆ Ottawa    ┆ Ottawa    ┆ 3      │
    └─────────────┴───────────┴────────────┴────────────┴───────────┴───────────┴────────┘



### Recipe 5 — Team offense: shots & shooting % ⚡

Roll the **team** boxscore up to the club level for a quick offensive profile — total goals, shot volume, and finishing rate.


```python
# Map each team_id to its abbreviation (both Int32-keyed), then roll up the
# skater box to the club level for a quick offensive profile.
team_lookup = (pwhl.load_pwhl_team_box(seasons=[2024])
    .select(['team_id', 'team_abbr']).unique())

(skater_box
    .join(team_lookup, on='team_id', how='left')
    .group_by('team_abbr')
    .agg(
        pl.col('goals').sum().alias('goals'),
        pl.col('shots').sum().alias('shots'),
    )
    .with_columns(
        (pl.col('goals') / pl.col('shots') * 100).round(1).alias('shooting_pct')
    )
    .filter(pl.col('team_abbr').is_not_null())
    .sort('goals', descending=True))
```




    shape: (6, 4)
    ┌───────────┬───────┬───────┬──────────────┐
    │ team_abbr ┆ goals ┆ shots ┆ shooting_pct │
    │ ---       ┆ ---   ┆ ---   ┆ ---          │
    │ str       ┆ i32   ┆ i32   ┆ f64          │
    ╞═══════════╪═══════╪═══════╪══════════════╡
    │ TOR       ┆ 74    ┆ 790   ┆ 9.4          │
    │ MIN       ┆ 72    ┆ 1025  ┆ 7.0          │
    │ MTL       ┆ 64    ┆ 814   ┆ 7.9          │
    │ BOS       ┆ 62    ┆ 907   ┆ 6.8          │
    │ OTT       ┆ 61    ┆ 721   ┆ 8.5          │
    │ NY        ┆ 52    ┆ 667   ┆ 7.8          │
    └───────────┴───────┴───────┴──────────────┘



### Recipe 6 — Power-play conversion leaders 🔌

The team boxscore carries `pp_goals` and `pp_opportunities`, so a season power-play percentage is a single division.


```python
team_box = pwhl.load_pwhl_team_box(seasons=[2024])

(team_box
    .group_by('team_abbr')
    .agg(
        pl.col('pp_goals').sum().alias('pp_goals'),
        pl.col('pp_opportunities').sum().alias('pp_opportunities'),
    )
    .with_columns(
        (pl.col('pp_goals') / pl.col('pp_opportunities') * 100).round(1).alias('pp_pct')
    )
    .sort('pp_pct', descending=True))
```




    shape: (6, 4)
    ┌───────────┬──────────┬──────────────────┬────────┐
    │ team_abbr ┆ pp_goals ┆ pp_opportunities ┆ pp_pct │
    │ ---       ┆ ---      ┆ ---              ┆ ---    │
    │ str       ┆ i32      ┆ i32              ┆ f64    │
    ╞═══════════╪══════════╪══════════════════╪════════╡
    │ OTT       ┆ 16       ┆ 64               ┆ 25.0   │
    │ NY        ┆ 19       ┆ 78               ┆ 24.4   │
    │ MTL       ┆ 16       ┆ 94               ┆ 17.0   │
    │ TOR       ┆ 11       ┆ 80               ┆ 13.8   │
    │ MIN       ┆ 7        ┆ 87               ┆ 8.0    │
    │ BOS       ┆ 4        ┆ 68               ┆ 5.9    │
    └───────────┴──────────┴──────────────────┴────────┘



### Recipe 7 — Faceoff specialists 🎯

The skater boxscore tracks faceoff wins and attempts. Aggregate, gate on a minimum-draw threshold, and the dot-dominators rise to the top.


```python
(skater_box
    .group_by(['first_name', 'last_name'])
    .agg(
        pl.col('faceoff_wins').sum().alias('fo_wins'),
        pl.col('faceoff_attempts').sum().alias('fo_attempts'),
    )
    .filter(pl.col('fo_attempts') >= 200)
    .with_columns(
        (pl.col('fo_wins') / pl.col('fo_attempts') * 100).round(1).alias('fo_pct')
    )
    .sort('fo_pct', descending=True)
    .head(10))
```




    shape: (10, 5)
    ┌──────────────┬───────────────┬─────────┬─────────────┬────────┐
    │ first_name   ┆ last_name     ┆ fo_wins ┆ fo_attempts ┆ fo_pct │
    │ ---          ┆ ---           ┆ ---     ┆ ---         ┆ ---    │
    │ str          ┆ str           ┆ i32     ┆ i32         ┆ f64    │
    ╞══════════════╪═══════════════╪═════════╪═════════════╪════════╡
    │ Abby         ┆ Roque         ┆ 205     ┆ 339         ┆ 60.5   │
    │ Marie-Philip ┆ Poulin        ┆ 326     ┆ 546         ┆ 59.7   │
    │ Alex         ┆ Carpenter     ┆ 245     ┆ 415         ┆ 59.0   │
    │ Kelly        ┆ Pannek        ┆ 344     ┆ 630         ┆ 54.6   │
    │ Brianne      ┆ Jenner        ┆ 125     ┆ 230         ┆ 54.3   │
    │ Taylor       ┆ Heise         ┆ 264     ┆ 495         ┆ 53.3   │
    │ Hannah       ┆ Brandt        ┆ 270     ┆ 510         ┆ 52.9   │
    │ Kristin      ┆ O'Neill       ┆ 240     ┆ 460         ┆ 52.2   │
    │ Jade         ┆ Downie-Landry ┆ 116     ┆ 225         ┆ 51.6   │
    │ Jesse        ┆ Compher       ┆ 119     ┆ 233         ┆ 51.1   │
    └──────────────┴───────────────┴─────────┴─────────────┴────────┘



### Recipe 8 — Two-way workhorses: hits + blocks 🧱

Not every contribution shows up on the scoresheet. Sum hits and blocked shots from the skater box to surface the players doing the dirty work — defenders usually own this list.


```python
(skater_box
    .group_by(['first_name', 'last_name', 'position'])
    .agg(
        pl.col('hits').sum().alias('hits'),
        pl.col('blocked_shots').sum().alias('blocks'),
    )
    .with_columns(
        (pl.col('hits') + pl.col('blocks')).alias('hits_plus_blocks')
    )
    .sort('hits_plus_blocks', descending=True)
    .head(10))
```




    shape: (10, 6)
    ┌────────────┬────────────┬──────────┬──────┬────────┬──────────────────┐
    │ first_name ┆ last_name  ┆ position ┆ hits ┆ blocks ┆ hits_plus_blocks │
    │ ---        ┆ ---        ┆ ---      ┆ ---  ┆ ---    ┆ ---              │
    │ str        ┆ str        ┆ str      ┆ i32  ┆ i32    ┆ i32              │
    ╞════════════╪════════════╪══════════╪══════╪════════╪══════════════════╡
    │ Renata     ┆ Fast       ┆ RD       ┆ 77   ┆ 23     ┆ 100              │
    │ Megan      ┆ Keller     ┆ LD       ┆ 64   ┆ 33     ┆ 97               │
    │ Kaleigh    ┆ Fratkin    ┆ RD       ┆ 65   ┆ 19     ┆ 84               │
    │ Blayre     ┆ Turnbull   ┆ C        ┆ 62   ┆ 14     ┆ 76               │
    │ Allie      ┆ Munroe     ┆ LD       ┆ 44   ┆ 25     ┆ 69               │
    │ Jessica    ┆ DiGirolamo ┆ LD       ┆ 36   ┆ 31     ┆ 67               │
    │ Emma       ┆ Maltais    ┆ LW       ┆ 53   ┆ 8      ┆ 61               │
    │ Lee        ┆ Stecklein  ┆ LD       ┆ 36   ┆ 25     ┆ 61               │
    │ Emma       ┆ Greco      ┆ LD       ┆ 32   ┆ 29     ┆ 61               │
    │ Kelly      ┆ Pannek     ┆ C        ┆ 28   ┆ 30     ┆ 58               │
    └────────────┴────────────┴──────────┴──────┴────────┴──────────────────┘



### Recipe 9 — The penalty box 🚨

[`load_pwhl_penalty_summary`](../pwhl/reference/loaders.md#load_pwhl_penalty_summary) is a tidy per-infraction log. Two quick cuts: the most common infractions league-wide, and the players spending the most time in the box.


```python
penalties = pwhl.load_pwhl_penalty_summary(seasons=[2024])

# Most common infractions
top_infractions = (penalties
    .group_by('description')
    .agg(pl.len().alias('count'))
    .sort('count', descending=True)
    .head(8))
top_infractions
```




    shape: (8, 2)
    ┌────────────────┬───────┐
    │ description    ┆ count │
    │ ---            ┆ ---   │
    │ str            ┆ u32   │
    ╞════════════════╪═══════╡
    │ Tripping       ┆ 106   │
    │ Hooking        ┆ 91    │
    │ Roughing       ┆ 64    │
    │ Interference   ┆ 53    │
    │ Slashing       ┆ 35    │
    │ Boarding       ┆ 30    │
    │ Holding        ┆ 26    │
    │ Cross Checking ┆ 26    │
    └────────────────┴───────┘




```python
# PIM leaders (players who actually took the penalty)
(penalties
    .filter(pl.col('taken_by_last').is_not_null())
    .group_by(['taken_by_first', 'taken_by_last'])
    .agg(
        pl.col('minutes').sum().alias('pim'),
        pl.len().alias('penalties'),
    )
    .sort('pim', descending=True)
    .head(10))
```




    shape: (10, 4)
    ┌────────────────┬───────────────┬─────┬───────────┐
    │ taken_by_first ┆ taken_by_last ┆ pim ┆ penalties │
    │ ---            ┆ ---           ┆ --- ┆ ---       │
    │ str            ┆ str           ┆ i32 ┆ u32       │
    ╞════════════════╪═══════════════╪═════╪═══════════╡
    │ Tereza         ┆ Vanišová      ┆ 37  ┆ 13        │
    │ Kaleigh        ┆ Fratkin       ┆ 36  ┆ 18        │
    │ Abby           ┆ Roque         ┆ 31  ┆ 10        │
    │ Jesse          ┆ Compher       ┆ 25  ┆ 7         │
    │ Megan          ┆ Keller        ┆ 22  ┆ 11        │
    │ Gabbie         ┆ Hughes        ┆ 20  ┆ 10        │
    │ Allie          ┆ Munroe        ┆ 20  ┆ 10        │
    │ Lee            ┆ Stecklein     ┆ 18  ┆ 9         │
    │ Jade           ┆ Downie-Landry ┆ 18  ┆ 9         │
    │ Renata         ┆ Fast          ┆ 18  ┆ 9         │
    └────────────────┴───────────────┴─────┴───────────┘



### Recipe 10 — When do goals get scored? ⏱️

Slice the goal log out of the play-by-play and bucket it by period — and pull the league's top finishers straight from the `event == 'goal'` rows while you're there.


```python
goal_events = pbp.filter(pl.col('event') == 'goal')

# Goals by period
goals_by_period = (goal_events
    .group_by('period_of_game')
    .agg(pl.len().alias('goals'))
    .sort('period_of_game'))
goals_by_period
```




    shape: (6, 2)
    ┌────────────────┬───────┐
    │ period_of_game ┆ goals │
    │ ---            ┆ ---   │
    │ str            ┆ u32   │
    ╞════════════════╪═══════╡
    │ 1              ┆ 109   │
    │ 2              ┆ 120   │
    │ 3              ┆ 138   │
    │ 4              ┆ 15    │
    │ 5              ┆ 2     │
    │ 6              ┆ 1     │
    └────────────────┴───────┘




```python
# Top goal-scorers from the play-by-play feed
(goal_events
    .filter(pl.col('player_name_last').is_not_null())
    .group_by(['player_name_first', 'player_name_last'])
    .agg(pl.len().alias('goals'))
    .sort('goals', descending=True)
    .head(10))
```




    shape: (10, 3)
    ┌───────────────────┬──────────────────┬───────┐
    │ player_name_first ┆ player_name_last ┆ goals │
    │ ---               ┆ ---              ┆ ---   │
    │ str               ┆ str              ┆ u32   │
    ╞═══════════════════╪══════════════════╪═══════╡
    │ Natalie           ┆ Spooner          ┆ 21    │
    │ Grace             ┆ Zumwinkle        ┆ 12    │
    │ Marie-Philip      ┆ Poulin           ┆ 11    │
    │ Sarah             ┆ Nurse            ┆ 11    │
    │ Laura             ┆ Stacey           ┆ 10    │
    │ Daryl             ┆ Watts            ┆ 10    │
    │ Brianne           ┆ Jenner           ┆ 9     │
    │ Michela           ┆ Cava             ┆ 9     │
    │ Gabbie            ┆ Hughes           ┆ 9     │
    │ Taylor            ┆ Heise            ┆ 9     │
    └───────────────────┴──────────────────┴───────┘



### Recipe 11 — Three-stars honour roll ⭐ and a head-to-head series

Two compact joins-on-themselves. First, who collected the most **first-star** nods ([`load_pwhl_three_stars`](../pwhl/reference/loaders.md#load_pwhl_three_stars)). Then a **head-to-head** series view from the schedule — swap in any two clubs.


```python
three_stars = pwhl.load_pwhl_three_stars(seasons=[2024])

# First-star honour roll
(three_stars
    .filter(pl.col('star') == 1)
    .group_by(['first_name', 'last_name'])
    .agg(pl.len().alias('first_stars'))
    .sort('first_stars', descending=True)
    .head(10))
```




    shape: (10, 3)
    ┌──────────────┬───────────┬─────────────┐
    │ first_name   ┆ last_name ┆ first_stars │
    │ ---          ┆ ---       ┆ ---         │
    │ str          ┆ str       ┆ u32         │
    ╞══════════════╪═══════════╪═════════════╡
    │ Natalie      ┆ Spooner   ┆ 7           │
    │ Kristen      ┆ Campbell  ┆ 4           │
    │ Nicole       ┆ Hensley   ┆ 4           │
    │ Marie-Philip ┆ Poulin    ┆ 3           │
    │ Sarah        ┆ Nurse     ┆ 3           │
    │ Gabbie       ┆ Hughes    ┆ 3           │
    │ Susanna      ┆ Tapani    ┆ 3           │
    │ Alex         ┆ Carpenter ┆ 3           │
    │ Hilary       ┆ Knight    ┆ 3           │
    │ Hannah       ┆ Brandt    ┆ 2           │
    └──────────────┴───────────┴─────────────┘




```python
# Head-to-head: Boston vs. Montreal, every meeting in 2024
A, B = 'Boston', 'Montreal'
(schedule
    .filter(
        ((pl.col('home_team') == A) & (pl.col('away_team') == B)) |
        ((pl.col('home_team') == B) & (pl.col('away_team') == A))
    )
    .select(['game_date', 'home_team', 'home_score',
             'away_score', 'away_team', 'winner', 'game_status']))
```




    shape: (7, 7)
    ┌─────────────┬───────────┬────────────┬────────────┬───────────┬──────────┬─────────────┐
    │ game_date   ┆ home_team ┆ home_score ┆ away_score ┆ away_team ┆ winner   ┆ game_status │
    │ ---         ┆ ---       ┆ ---        ┆ ---        ┆ ---       ┆ ---      ┆ ---         │
    │ str         ┆ str       ┆ str        ┆ str        ┆ str       ┆ str      ┆ str         │
    ╞═════════════╪═══════════╪════════════╪════════════╪═══════════╪══════════╪═════════════╡
    │ Tue, May 14 ┆ Boston    ┆ 3          ┆ 2          ┆ Montreal  ┆ Boston   ┆ Final OT    │
    │ Thu, May 9  ┆ Montreal  ┆ 1          ┆ 2          ┆ Boston    ┆ Boston   ┆ Final OT    │
    │ Sun, Feb 4  ┆ Boston    ┆ 1          ┆ 2          ┆ Montreal  ┆ Montreal ┆ Final OT    │
    │ Sat, May 4  ┆ Boston    ┆ 4          ┆ 3          ┆ Montreal  ┆ Boston   ┆ Final       │
    │ Sat, May 11 ┆ Montreal  ┆ 1          ┆ 2          ┆ Boston    ┆ Boston   ┆ Final OT3   │
    │ Sat, Mar 2  ┆ Montreal  ┆ 3          ┆ 1          ┆ Boston    ┆ Montreal ┆ Final       │
    │ Sat, Jan 13 ┆ Montreal  ┆ 2          ┆ 3          ┆ Boston    ┆ Boston   ┆ Final OT    │
    └─────────────┴───────────┴────────────┴────────────┴───────────┴──────────┴─────────────┘



### Recipe 12 — Find a player, then pull her career lines 🛰️🔎

A classic two-step lookup off the live feed: [`pwhl_player_search`](../pwhl/reference/additional.md#pwhl_player_search) resolves a name to a `player_id`, then [`pwhl_player_stats`](../pwhl/reference/additional.md#pwhl_player_stats) returns her season-by-season stat lines. Both are `safe()`-wrapped for offseason resilience.


```python
hit = safe('player search: Spooner', lambda: pwhl.pwhl_player_search('Spooner'))
if hit is not None and getattr(hit, 'height', 0):
    pid = int(hit['player_id'][0])
    career = safe(f'player stats {pid}', lambda: pwhl.pwhl_player_stats(player_id=pid))
    if career is not None and career.height:
        keep = [c for c in ['season_name', 'team_code', 'games_played',
                            'goals', 'assists', 'points', 'points_per_game']
                if c in career.columns]
        out = career.select(keep)
    else:
        out = 'player stats feed unavailable right now'
else:
    out = 'player search feed unavailable right now'
out
```

    ✅ player search: Spooner


    ✅ player stats 100





    shape: (10, 7)
    ┌────────────────────────┬───────────┬──────────────┬───────┬─────────┬────────┬─────────────────┐
    │ season_name            ┆ team_code ┆ games_played ┆ goals ┆ assists ┆ points ┆ points_per_game │
    │ ---                    ┆ ---       ┆ ---          ┆ ---   ┆ ---     ┆ ---    ┆ ---             │
    │ str                    ┆ str       ┆ str          ┆ str   ┆ str     ┆ str    ┆ str             │
    ╞════════════════════════╪═══════════╪══════════════╪═══════╪═════════╪════════╪═════════════════╡
    │ 2025-26 Regular Season ┆ TOR       ┆ 30           ┆ 3     ┆ 5       ┆ 8      ┆ 0.27            │
    │ 2024-25 Regular Season ┆ TOR       ┆ 14           ┆ 3     ┆ 2       ┆ 5      ┆ 0.36            │
    │ 2024 Regular Season    ┆ TOR       ┆ 24           ┆ 20    ┆ 7       ┆ 27     ┆ 1.13            │
    │ Total                  ┆ null      ┆ 68           ┆ 26    ┆ 14      ┆ 40     ┆ 0.59            │
    │ 2025-26 Preseason      ┆ TOR       ┆ 1            ┆ 0     ┆ 1       ┆ 1      ┆ 1.00            │
    │ 2024 Preseason         ┆ TOR       ┆ 1            ┆ 0     ┆ 0       ┆ 0      ┆ 0.00            │
    │ Total                  ┆ null      ┆ 2            ┆ 0     ┆ 1       ┆ 1      ┆ 0.50            │
    │ 2025 Playoffs          ┆ TOR       ┆ 4            ┆ 0     ┆ 1       ┆ 1      ┆ 0.25            │
    │ 2024 Playoffs          ┆ TOR       ┆ 3            ┆ 1     ┆ 1       ┆ 2      ┆ 0.67            │
    │ Total                  ┆ null      ┆ 7            ┆ 1     ┆ 2       ┆ 3      ┆ 0.43            │
    └────────────────────────┴───────────┴──────────────┴───────┴─────────┴────────┴─────────────────┘



### Recipe 13 — A team, its roster, and a game's PBP + Corsi 🛰️📈

The full live tour. List teams with [`pwhl_teams`](../pwhl/reference/additional.md#pwhl_teams), grab a `team_id`, pull the roster with [`pwhl_team_roster`](../pwhl/reference/additional.md#pwhl_team_roster), take a `game_id` from the loader schedule, then fetch enriched events with [`pwhl_pbp`](../pwhl/reference/additional.md#pwhl_pbp) and shot-attempt share with [`pwhl_game_corsi`](../pwhl/reference/additional.md#pwhl_game_corsi) — all from the same feed. Everything is `safe()`-wrapped, so offline this prints a friendly note instead of raising.


```python
teams = safe('PWHL teams', lambda: pwhl.pwhl_teams(season=2024))
if teams is not None and teams.height:
    tid = int(teams['team_id'][0])
    roster = safe(f'PWHL roster {tid}', lambda: pwhl.pwhl_team_roster(team_id=tid, season=2024))
    out = (roster.select([c for c in ['first_name', 'last_name', 'position', 'jersey_number']
                          if c in roster.columns]).head()
           if roster is not None else teams.head())
else:
    out = 'teams feed unavailable right now'
out
```

    ✅ PWHL teams


    ✅ PWHL roster 1





    shape: (5, 3)
    ┌────────────┬───────────┬──────────┐
    │ first_name ┆ last_name ┆ position │
    │ ---        ┆ ---       ┆ ---      │
    │ str        ┆ str       ┆ str      │
    ╞════════════╪═══════════╪══════════╡
    │ Emily      ┆ Brown     ┆ D        │
    │ Megan      ┆ Keller    ┆ D        │
    │ Sidney     ┆ Morin     ┆ D        │
    │ Lexie      ┆ Adzija    ┆ F        │
    │ Sophie     ┆ Shirley   ┆ F        │
    └────────────┴───────────┴──────────┘




```python
# A game_id from the loader schedule (offline-safe), then enrich it live.
gid = int(schedule['game_id'][0])
pbp_live = safe(f'PWHL pbp {gid}', lambda: pwhl.pwhl_pbp(game_id=gid))
corsi = safe(f'PWHL corsi {gid}', lambda: pwhl.pwhl_game_corsi(game_id=gid))
print('live pbp rows:', None if pbp_live is None else pbp_live.height,
      '| corsi rows:', None if corsi is None else corsi.height)
```

    ✅ PWHL pbp 84


    ✅ PWHL corsi 84
    live pbp rows: 188 | corsi rows: 39


## 🛰️ Live standings & leaders

Straight off the HockeyTech feed: [`pwhl_standings`](../pwhl/reference/additional.md#pwhl_standings) for the live table and [`pwhl_leaders`](../pwhl/reference/additional.md#pwhl_leaders) for the statistical leaderboard. Both take a `season` end-year. We keep them `safe()`-wrapped because live endpoints are seasonal.


```python
standings = safe('PWHL standings', lambda: pwhl.pwhl_standings(season=2024))
if standings is not None and standings.height:
    keep = [c for c in ['team', 'team_code', 'games_played', 'wins', 'losses', 'points']
            if c in standings.columns]
    out = standings.select(keep).head(10)
else:
    out = 'standings feed unavailable right now'
out
```

    ✅ PWHL standings





    shape: (6, 6)
    ┌────────────────────┬───────────┬──────────────┬──────┬────────┬────────┐
    │ team               ┆ team_code ┆ games_played ┆ wins ┆ losses ┆ points │
    │ ---                ┆ ---       ┆ ---          ┆ ---  ┆ ---    ┆ ---    │
    │ str                ┆ str       ┆ str          ┆ i64  ┆ str    ┆ i64    │
    ╞════════════════════╪═══════════╪══════════════╪══════╪════════╪════════╡
    │ x - PWHL Toronto   ┆ x - TOR   ┆ 24           ┆ 17   ┆ 7      ┆ 47     │
    │ x - PWHL Montreal  ┆ x - MTL   ┆ 24           ┆ 13   ┆ 6      ┆ 41     │
    │ x - PWHL Boston    ┆ x - BOS   ┆ 24           ┆ 12   ┆ 9      ┆ 35     │
    │ x - PWHL Minnesota ┆ x - MIN   ┆ 24           ┆ 12   ┆ 9      ┆ 35     │
    │ e - PWHL Ottawa    ┆ e - OTT   ┆ 24           ┆ 9    ┆ 9      ┆ 32     │
    │ e - PWHL New York  ┆ e - NY    ┆ 24           ┆ 9    ┆ 12     ┆ 26     │
    └────────────────────┴───────────┴──────────────┴──────┴────────┴────────┘




```python
leaders = safe('PWHL leaders', lambda: pwhl.pwhl_leaders(season=2024))
if leaders is not None and getattr(leaders, 'height', 0):
    keep = [c for c in ['rank', 'name', 'team_code', 'stat_formatted', 'type_formatted']
            if c in leaders.columns]
    out = leaders.select(keep).head(10)
else:
    out = 'leaders feed unavailable right now'
out
```

    ✅ PWHL leaders





    shape: (10, 5)
    ┌──────┬─────────────────────┬───────────┬────────────────┬────────────────┐
    │ rank ┆ name                ┆ team_code ┆ stat_formatted ┆ type_formatted │
    │ ---  ┆ ---                 ┆ ---       ┆ ---            ┆ ---            │
    │ i64  ┆ str                 ┆ str       ┆ str            ┆ str            │
    ╞══════╪═════════════════════╪═══════════╪════════════════╪════════════════╡
    │ 1    ┆ Natalie Spooner     ┆ TOR       ┆ 27             ┆ Points         │
    │ 2    ┆ Sarah Nurse         ┆ TOR       ┆ 23             ┆ Points         │
    │ 3    ┆ Marie-Philip Poulin ┆ MTL       ┆ 23             ┆ Points         │
    │ 4    ┆ Alex Carpenter      ┆ NY        ┆ 23             ┆ Points         │
    │ 5    ┆ Ella Shelton        ┆ NY        ┆ 21             ┆ Points         │
    │ 1    ┆ Natalie Spooner     ┆ TOR       ┆ 20             ┆ Goals          │
    │ 2    ┆ Sarah Nurse         ┆ TOR       ┆ 11             ┆ Goals          │
    │ 3    ┆ Grace Zumwinkle     ┆ MIN       ┆ 11             ┆ Goals          │
    │ 4    ┆ Marie-Philip Poulin ┆ MTL       ┆ 10             ┆ Goals          │
    │ 5    ┆ Laura Stacey        ┆ MTL       ┆ 10             ┆ Goals          │
    └──────┴─────────────────────┴───────────┴────────────────┴────────────────┘



## 🥅 On-ice analytics

Beyond the box score, three analytics helpers derive advanced metrics from the same shift + play-by-play feed:

| Function | Metric |
|---|---|
| [`pwhl_game_corsi`](../pwhl/reference/additional.md#pwhl_game_corsi) | Corsi / Fenwick shot-attempt share, with per-60 rates |
| [`pwhl_player_toi`](../pwhl/reference/additional.md#pwhl_player_toi) | summed time-on-ice + shift counts per player |
| [`pwhl_game_shifts`](../pwhl/reference/additional.md#pwhl_game_shifts) | raw shift stints (who's on the ice, when) |

⚠️ Corsi note: the HockeyTech feed has no *missed-shot* event, so Corsi and Fenwick here are proxies counting shots + blocked shots + goals only (`corsi_includes_missed = False`).


```python
toi = safe(f'PWHL TOI {gid}', lambda: pwhl.pwhl_player_toi(game_id=gid))
if toi is not None and toi.height:
    out = (toi.select([c for c in ['first_name', 'last_name', 'toi_seconds', 'num_shifts']
                       if c in toi.columns])
              .sort('toi_seconds', descending=True).head())
else:
    out = 'time-on-ice feed unavailable right now'
out
```

    ✅ PWHL TOI 84





    shape: (5, 4)
    ┌────────────┬───────────┬─────────────┬────────────┐
    │ first_name ┆ last_name ┆ toi_seconds ┆ num_shifts │
    │ ---        ┆ ---       ┆ ---         ┆ ---        │
    │ str        ┆ str       ┆ i64         ┆ u32        │
    ╞════════════╪═══════════╪═════════════╪════════════╡
    │ Kristen    ┆ Campbell  ┆ 3600        ┆ 3          │
    │ Nicole     ┆ Hensley   ┆ 3600        ┆ 3          │
    │ Jocelyne   ┆ Larocque  ┆ 1677        ┆ 29         │
    │ Renata     ┆ Fast      ┆ 1674        ┆ 28         │
    │ Sophie     ┆ Jaques    ┆ 1402        ┆ 26         │
    └────────────┴───────────┴─────────────┴────────────┘




```python
if corsi is not None and corsi.height:
    out = (corsi
        .with_columns((pl.col('corsi_for') - pl.col('corsi_against')).alias('corsi_net'))
        .select([c for c in ['player_id', 'corsi_for', 'corsi_against', 'corsi_net', 'corsi_for_per60']
                 if c in corsi.columns])
        .sort('corsi_for_per60', descending=True)
        .head())
else:
    out = 'corsi feed unavailable right now'
out
```




    shape: (5, 4)
    ┌───────────┬───────────┬───────────────┬─────────────────┐
    │ player_id ┆ corsi_for ┆ corsi_against ┆ corsi_for_per60 │
    │ ---       ┆ ---       ┆ ---           ┆ ---             │
    │ str       ┆ i64       ┆ i64           ┆ f64             │
    ╞═══════════╪═══════════╪═══════════════╪═════════════════╡
    │ 115       ┆ 16        ┆ 2             ┆ 74.805195       │
    │ 76        ┆ 17        ┆ 4             ┆ 66.521739       │
    │ 89        ┆ 15        ┆ 6             ┆ 64.362336       │
    │ 100       ┆ 17        ┆ 8             ┆ 59.824047       │
    │ 20        ┆ 16        ┆ 14            ┆ 51.382694       │
    └───────────┴───────────┴───────────────┴─────────────────┘



## ✨ Bonus: tidy goal log + pandas interop

[`load_pwhl_scoring_summary`](../pwhl/reference/loaders.md#load_pwhl_scoring_summary) is a clean per-goal log — scorer plus up to two assists, with situation flags like power play, short handed, and game-winning. And because every loader takes `return_as_pandas=True`, dropping into the pandas world is one keyword away.


```python
scoring = pwhl.load_pwhl_scoring_summary(seasons=[2024])
scoring.select([
    'game_id', 'period', 'time', 'team_abbr',
    'scorer_first', 'scorer_last', 'is_power_play', 'is_game_winning',
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




```python
# Same skater box, but as a pandas DataFrame — group with the pandas API.
skater_pd = pwhl.load_pwhl_skater_box(seasons=[2024], return_as_pandas=True)
print('type:', type(skater_pd).__name__, '| shape:', skater_pd.shape)
(skater_pd
    .groupby(['first_name', 'last_name'], as_index=False)['points'].sum()
    .sort_values('points', ascending=False)
    .head(10))
```

    type: DataFrame | shape: (3205, 22)





           first_name  last_name  points
    101       Natalie    Spooner      29
    90   Marie-Philip     Poulin      25
    114         Sarah      Nurse      24
    4            Alex  Carpenter      23
    35           Ella    Shelton      21
    40           Emma    Maltais      21
    126        Taylor      Heise      21
    18        Brianne     Jenner      20
    42           Erin    Ambrose      20
    47          Grace  Zumwinkle      20



## ⏱️ Shifts, strength state, and shot-level xG

New in 0.0.72: two published PWHL dataset releases. `load_pwhl_shifts` is the
shift-chart table backing the real on-ice `strength_state` (EV/PP/SH), and
`load_pwhl_xg_pbp` is the play-by-play enriched with shot-level,
coordinate-based expected goals:


```python
from sportsdataverse.pwhl import load_pwhl_shifts, load_pwhl_xg_pbp

shifts = load_pwhl_shifts(seasons=[2025])
xg     = load_pwhl_xg_pbp(seasons=[2025])
print("shifts:", shifts.shape, "| xg pbp:", xg.shape)
xg.select(["game_id", "event_type", "shot_distance", "shot_angle", "xg"]).drop_nulls("xg").head()
```

    shifts: (81546, 14) | xg pbp: (5671, 21)





    shape: (5, 5)
    ┌─────────┬────────────┬───────────────┬────────────┬──────────┐
    │ game_id ┆ event_type ┆ shot_distance ┆ shot_angle ┆ xg       │
    │ ---     ┆ ---        ┆ ---           ┆ ---        ┆ ---      │
    │ i32     ┆ str        ┆ f64           ┆ f64        ┆ f64      │
    ╞═════════╪════════════╪═══════════════╪════════════╪══════════╡
    │ 105     ┆ Default    ┆ 38.6912       ┆ 33.3136    ┆ 0.050235 │
    │ 105     ┆ Default    ┆ 46.5487       ┆ 21.4205    ┆ 0.053222 │
    │ 105     ┆ Default    ┆ 24.8723       ┆ 17.9129    ┆ 0.082214 │
    │ 105     ┆ Default    ┆ 36.8333       ┆ 22.6199    ┆ 0.06718  │
    │ 105     ┆ Default    ┆ 25.5563       ┆ 61.145     ┆ 0.061872 │
    └─────────┴────────────┴───────────────┴────────────┴──────────┘



## 🎉 Where to next

- 📦 **Loaders** are your offline-friendly workhorses — stack seasons with `seasons=[2024, 2025]` and pass `return_as_pandas=True` for pandas.
- 🛰️ **Live wrappers** (`pwhl_*`) pull fresh data and add analytics (Corsi, TOI, shifts) — no key required.
- Full reference: the **PWHL → [Loaders](../pwhl/reference/loaders.md)** and **[Additional functions](../pwhl/reference/additional.md)** pages in the sidebar.
- Junior & minor hockey? The same HockeyTech surface powers the AHL / OHL / WHL / QMJHL — see `11_junior_hockey_intro.ipynb`.
- The men's game and the modern NHL APIs live in `07_nhl_intro.ipynb`.
- R user? The same data lives in [fastRhockey](https://fastRhockey.sportsdataverse.org) (NHL + PWHL).

Now go tell the story of the PWHL — the data's all here. 🏒💜
