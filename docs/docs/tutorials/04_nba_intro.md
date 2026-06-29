---
title: NBA tutorial
sidebar_label: NBA
sidebar_position: 2
---

# 🏀 NBA hoops with `sportsdataverse-py`

Welcome to the hardwood! 🎉 In just a few lines of Python you're about to
pull a whole season of NBA data — **teams, standings, rosters, play-by-play,
box scores, schedules and statistical leaders** — straight from ESPN and the
SportsDataverse data releases. Everything comes back as a tidy **polars**
DataFrame that's ready to slice, model, and chart. 🚀

We lead with the richest surface in the package: the **`espn_nba_*`** family,
backed by ESPN's site / web / core APIs. If you know the R package
[hoopR](https://hoopR.sportsdataverse.org), these names will feel like home.
Python neighbor for the raw NBA Stats endpoints:
[nba_api](https://github.com/swar/nba_api). Let's lace 'em up! 👟

## 🧰 The toolbox

Here's the kit we'll reach for. The **`espn_nba_*`** wrappers (⭐ our
premium source) hit ESPN live and parse the JSON into polars for you; the
**`load_nba_*`** loaders pull pre-built season parquets from the
[sportsdataverse-data](https://github.com/sportsdataverse/sportsdataverse-data)
releases — fast and reliable. Click any name for the full reference.

| Function | What it gives you | Source |
|---|---|---|
| [`espn_nba_teams`](../nba/reference/additional.md#espn_nba_teams) | All 30 NBA teams (grab `team_id`s here) | ⭐ ESPN |
| [`espn_nba_scoreboard`](../nba/reference/site.md#espn_nba_scoreboard) | A day's slate — scores, status, matchups | ⭐ ESPN |
| [`espn_nba_schedule`](../nba/reference/additional.md#espn_nba_schedule) | Schedule for a date / date-range | ⭐ ESPN |
| [`espn_nba_standings`](../nba/reference/site.md#espn_nba_standings) | Conference standings (W-L, win%, streak) | ⭐ ESPN |
| [`espn_nba_team_roster`](../nba/reference/site.md#espn_nba_team_roster) | A team's active roster | ⭐ ESPN |
| [`espn_nba_team_schedule`](../nba/reference/site.md#espn_nba_team_schedule) | One team's full-season schedule | ⭐ ESPN |
| [`espn_nba_player_gamelog`](../nba/reference/web.md#espn_nba_player_gamelog) | A player's game-by-game log | ⭐ ESPN |
| [`espn_nba_leaders`](../nba/reference/web.md#espn_nba_leaders) | League statistical leaders | ⭐ ESPN |
| [`espn_nba_pbp`](../nba/reference/additional.md) | Full game payload (play-by-play, win prob, box) | ⭐ ESPN |
| [`espn_nba_game_rosters`](../nba/reference/additional.md) | Both teams' rosters for one game | ⭐ ESPN |
| [`load_nba_schedule`](../nba/reference/loaders.md#load_nba_pbp) | Multi-season schedule parquet | 📦 release |
| [`load_nba_player_boxscore`](../nba/reference/loaders.md#load_nba_player_boxscore) | Player box scores, every game | 📦 release |
| [`load_nba_standings`](../nba/reference/loaders.md#load_nba_standings) | Historical standings | 📦 release |
| [`espn_nba_injuries`](../nba/reference/site.md#espn_nba_injuries) | League-wide injury report, one row per team | ⭐ ESPN |
| [`load_nba_team_boxscore`](../nba/reference/loaders.md#load_nba_team_boxscore) | Team box scores, every game (off/def, shooting) | 📦 release |
| [`load_nba_shots`](../nba/reference/loaders.md#load_nba_shots) | Every made shot with court coordinates | 📦 release |
| [`most_recent_nba_season`](../nba/reference/additional.md#most_recent_nba_season) | The current season year helper | 🧮 util |


## 🔌 Setup

```sh
pip install sportsdataverse
```

No API key needed — ESPN's public endpoints and the data releases are open. 😊


```python
import polars as pl
import sportsdataverse as sdv
from sportsdataverse.nba import most_recent_nba_season

pl.Config.set_tbl_rows(8)
SEASON = most_recent_nba_season()
print('current NBA season:', SEASON)
```

    current NBA season: 2026


ESPN endpoints are live and seasonal, so we'll route every network call
through a tiny `safe()` helper. When the feed is up you get the frame; when
it's mid-offseason or briefly rate-limited you get a friendly one-liner
instead of a scary traceback. 🛟


```python
def safe(label, thunk):
    try:
        out = thunk()
        n = out.height if isinstance(out, pl.DataFrame) else (len(out) if hasattr(out, '__len__') else '?')
        print(f'✅ {label} — {n} rows')
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f'⏭️  {label}: unavailable right now ({type(e).__name__})')
        return None
```

## 🏟️ Teams

Start with [`espn_nba_teams`](../nba/reference/additional.md#espn_nba_teams) —
one wide row per franchise. The `team_id` column is the key you'll pass into
roster, schedule and standings calls everywhere else.


```python
teams = safe('teams', sdv.nba.espn_nba_teams)
(teams.select(['team_id', 'team_location', 'team_name', 'team_abbreviation', 'team_color']).head(10)
 if teams is not None else 'teams feed unavailable')
```

    ✅ teams — 30 rows





    shape: (10, 5)
    ┌─────────┬───────────────┬───────────┬───────────────────┬────────────┐
    │ team_id ┆ team_location ┆ team_name ┆ team_abbreviation ┆ team_color │
    │ ---     ┆ ---           ┆ ---       ┆ ---               ┆ ---        │
    │ str     ┆ str           ┆ str       ┆ str               ┆ str        │
    ╞═════════╪═══════════════╪═══════════╪═══════════════════╪════════════╡
    │ 1       ┆ Atlanta       ┆ Hawks     ┆ ATL               ┆ c8102e     │
    │ 2       ┆ Boston        ┆ Celtics   ┆ BOS               ┆ 008348     │
    │ 17      ┆ Brooklyn      ┆ Nets      ┆ BKN               ┆ 000000     │
    │ 30      ┆ Charlotte     ┆ Hornets   ┆ CHA               ┆ 008ca8     │
    │ …       ┆ …             ┆ …         ┆ …                 ┆ …          │
    │ 6       ┆ Dallas        ┆ Mavericks ┆ DAL               ┆ 0064b1     │
    │ 7       ┆ Denver        ┆ Nuggets   ┆ DEN               ┆ 0e2240     │
    │ 8       ┆ Detroit       ┆ Pistons   ┆ DET               ┆ 1d428a     │
    │ 9       ┆ Golden State  ┆ Warriors  ┆ GS                ┆ fdb927     │
    └─────────┴───────────────┴───────────┴───────────────────┴────────────┘



## 📅 Today on the slate (scoreboard)

[`espn_nba_scoreboard`](../nba/reference/site.md#espn_nba_scoreboard) returns a
tidy frame of every game for a date — final scores, live status, and
matchups. Pass `dates='YYYYMMDD'` for one day. Here's a slice of the 2024
NBA Finals opener.


```python
sb = safe('scoreboard', lambda: sdv.nba.espn_nba_scoreboard(dates='20240606'))
keep = ['game_id', 'short_name', 'home_abbreviation', 'away_abbreviation',
        'home_score', 'away_score', 'status_type_detail']
(sb.select([c for c in keep if c in sb.columns]).head()
 if sb is not None and sb.height else 'no games on that date')
```

    ✅ scoreboard — 1 rows





    shape: (1, 7)
    ┌───────────┬────────────┬───────────────┬───────────────┬────────────┬────────────┬───────────────┐
    │ game_id   ┆ short_name ┆ home_abbrevia ┆ away_abbrevia ┆ home_score ┆ away_score ┆ status_type_d │
    │ ---       ┆ ---        ┆ tion          ┆ tion          ┆ ---        ┆ ---        ┆ etail         │
    │ str       ┆ str        ┆ ---           ┆ ---           ┆ str        ┆ str        ┆ ---           │
    │           ┆            ┆ str           ┆ str           ┆            ┆            ┆ str           │
    ╞═══════════╪════════════╪═══════════════╪═══════════════╪════════════╪════════════╪═══════════════╡
    │ 401656359 ┆ DAL @ BOS  ┆ BOS           ┆ DAL           ┆ 107        ┆ 89         ┆ Final         │
    └───────────┴────────────┴───────────────┴───────────────┴────────────┴────────────┴───────────────┘



## 🏆 Standings

[`espn_nba_standings`](../nba/reference/site.md#espn_nba_standings) gives one
row per team with wins, losses, win%, point differential and current streak.
Pass `season=` (the end year of the season).


```python
standings = safe('standings', lambda: sdv.nba.espn_nba_standings(season=SEASON))
cols = ['team_display_name', 'wins', 'losses', 'win_percent', 'games_behind',
        'point_differential', 'streak']
(standings.select([c for c in cols if c in standings.columns])
          .sort('win_percent', descending=True).head(10)
 if standings is not None and standings.height else 'standings unavailable')
```

    ✅ standings — 30 rows





    shape: (10, 7)
    ┌───────────────────────┬──────┬────────┬─────────────┬──────────────┬────────────────────┬────────┐
    │ team_display_name     ┆ wins ┆ losses ┆ win_percent ┆ games_behind ┆ point_differential ┆ streak │
    │ ---                   ┆ ---  ┆ ---    ┆ ---         ┆ ---          ┆ ---                ┆ ---    │
    │ str                   ┆ f64  ┆ f64    ┆ f64         ┆ f64          ┆ f64                ┆ f64    │
    ╞═══════════════════════╪══════╪════════╪═════════════╪══════════════╪════════════════════╪════════╡
    │ Oklahoma City Thunder ┆ 64.0 ┆ 18.0   ┆ 0.7804878   ┆ 0.0          ┆ 914.0              ┆ -2.0   │
    │ San Antonio Spurs     ┆ 62.0 ┆ 20.0   ┆ 0.756098    ┆ 2.0          ┆ 681.0              ┆ -1.0   │
    │ Detroit Pistons       ┆ 60.0 ┆ 22.0   ┆ 0.731707    ┆ 0.0          ┆ 669.0              ┆ 3.0    │
    │ Boston Celtics        ┆ 56.0 ┆ 26.0   ┆ 0.682927    ┆ 4.0          ┆ 631.0              ┆ 2.0    │
    │ …                     ┆ …    ┆ …      ┆ …           ┆ …            ┆ …                  ┆ …      │
    │ Los Angeles Lakers    ┆ 53.0 ┆ 29.0   ┆ 0.646341    ┆ 11.0         ┆ 145.0              ┆ 3.0    │
    │ Cleveland Cavaliers   ┆ 52.0 ┆ 30.0   ┆ 0.634146    ┆ 8.0          ┆ 336.0              ┆ 1.0    │
    │ Houston Rockets       ┆ 52.0 ┆ 30.0   ┆ 0.634146    ┆ 12.0         ┆ 428.0              ┆ 1.0    │
    │ Minnesota             ┆ 49.0 ┆ 33.0   ┆ 0.597561    ┆ 15.0         ┆ 275.0              ┆ 2.0    │
    │ Timberwolves          ┆      ┆        ┆             ┆              ┆                    ┆        │
    └───────────────────────┴──────┴────────┴─────────────┴──────────────┴────────────────────┴────────┘



## 🍳 Cookbook: common NBA tasks

Now the fun part — a handful of recipes you'll reach for again and again.
Each one leans on the premium `espn_nba_*` wrappers.

### Recipe 1 — A team and its roster 👥

Grab a `team_id` from [`espn_nba_teams`](../nba/reference/additional.md#espn_nba_teams),
then pull the active roster with
[`espn_nba_team_roster`](../nba/reference/site.md#espn_nba_team_roster).


```python
tid = None
if teams is not None and teams.height:
    # Boston Celtics if present, else the first team
    row = teams.filter(pl.col('team_abbreviation') == 'BOS')
    tid = int((row if row.height else teams)['team_id'][0])

roster = safe(f'roster {tid}', lambda: sdv.nba.espn_nba_team_roster(team_id=tid)) if tid else None
cols = ['full_name', 'jersey', 'position_abbreviation', 'height', 'weight', 'age']
(roster.select([c for c in cols if c in roster.columns]).head(10)
 if roster is not None and roster.height else 'roster unavailable')
```

    ✅ roster 2 — 18 rows





    shape: (10, 6)
    ┌──────────────────┬────────┬───────────────────────┬────────┬────────┬─────┐
    │ full_name        ┆ jersey ┆ position_abbreviation ┆ height ┆ weight ┆ age │
    │ ---              ┆ ---    ┆ ---                   ┆ ---    ┆ ---    ┆ --- │
    │ str              ┆ str    ┆ str                   ┆ f64    ┆ f64    ┆ i64 │
    ╞══════════════════╪════════╪═══════════════════════╪════════╪════════╪═════╡
    │ Dalano Banton    ┆ 45     ┆ F                     ┆ 80.0   ┆ 203.0  ┆ 26  │
    │ Jaylen Brown     ┆ 7      ┆ G                     ┆ 78.0   ┆ 223.0  ┆ 29  │
    │ Chris Cenac Jr.  ┆ null   ┆ F                     ┆ 83.0   ┆ 240.0  ┆ 19  │
    │ Luka Garza       ┆ 52     ┆ C                     ┆ 82.0   ┆ 243.0  ┆ 27  │
    │ …                ┆ …      ┆ …                     ┆ …      ┆ …      ┆ …   │
    │ Sam Hauser       ┆ 30     ┆ F                     ┆ 79.0   ┆ 217.0  ┆ 28  │
    │ Dillon Mitchell  ┆ null   ┆ F                     ┆ 80.0   ┆ 210.0  ┆ 22  │
    │ Payton Pritchard ┆ 11     ┆ G                     ┆ 73.0   ┆ 195.0  ┆ 28  │
    │ Neemias Queta    ┆ 88     ┆ C                     ┆ 84.0   ┆ 248.0  ┆ 26  │
    └──────────────────┴────────┴───────────────────────┴────────┴────────┴─────┘



### Recipe 2 — One team's season schedule 📆

[`espn_nba_team_schedule`](../nba/reference/site.md#espn_nba_team_schedule)
returns every game on a team's calendar for a season — perfect for building
a results table or a strength-of-schedule view.


```python
tsched = safe(f'team schedule {tid}',
              lambda: sdv.nba.espn_nba_team_schedule(team_id=tid, season=SEASON)) if tid else None
cols = ['id', 'date', 'name', 'short_name', 'season_year']
(tsched.select([c for c in cols if c in tsched.columns]).head()
 if tsched is not None and tsched.height else 'team schedule unavailable')
```

    ✅ team schedule 2 — 7 rows





    shape: (5, 5)
    ┌───────────┬───────────────────┬─────────────────────────────────┬────────────┬─────────────┐
    │ id        ┆ date              ┆ name                            ┆ short_name ┆ season_year │
    │ ---       ┆ ---               ┆ ---                             ┆ ---        ┆ ---         │
    │ str       ┆ str               ┆ str                             ┆ str        ┆ i64         │
    ╞═══════════╪═══════════════════╪═════════════════════════════════╪════════════╪═════════════╡
    │ 401869191 ┆ 2026-04-19T17:00Z ┆ Philadelphia 76ers at Boston C… ┆ PHI @ BOS  ┆ 2026        │
    │ 401869396 ┆ 2026-04-21T23:00Z ┆ Philadelphia 76ers at Boston C… ┆ PHI @ BOS  ┆ 2026        │
    │ 401869404 ┆ 2026-04-24T23:00Z ┆ Boston Celtics at Philadelphia… ┆ BOS @ PHI  ┆ 2026        │
    │ 401869406 ┆ 2026-04-26T23:00Z ┆ Boston Celtics at Philadelphia… ┆ BOS @ PHI  ┆ 2026        │
    │ 401869408 ┆ 2026-04-28T23:00Z ┆ Philadelphia 76ers at Boston C… ┆ PHI @ BOS  ┆ 2026        │
    └───────────┴───────────────────┴─────────────────────────────────┴────────────┴─────────────┘



### Recipe 3 — A player's game log ⛹️

[`espn_nba_player_gamelog`](../nba/reference/web.md#espn_nba_player_gamelog)
returns a game-by-game stat line for one athlete. The `stat_*` columns are
positional (the ordered ESPN box categories); pair them with the opponent
and result columns to see how the night went. (`1966` = LeBron James.)


```python
gamelog = safe('LeBron gamelog',
               lambda: sdv.nba.espn_nba_player_gamelog(athlete_id=1966, season=SEASON))
cols = ['event_date', 'opponent_abbreviation', 'home_away', 'game_result', 'score',
        'stat_0', 'stat_1', 'stat_2']
(gamelog.select([c for c in cols if c in gamelog.columns]).head()
 if gamelog is not None and gamelog.height else 'gamelog unavailable')
```

    ✅ LeBron gamelog — 73 rows





    shape: (5, 8)
    ┌────────────┬───────────────────────┬───────────┬─────────────┬───────┬────────┬────────┬────────┐
    │ event_date ┆ opponent_abbreviation ┆ home_away ┆ game_result ┆ score ┆ stat_0 ┆ stat_1 ┆ stat_2 │
    │ ---        ┆ ---                   ┆ ---       ┆ ---         ┆ ---   ┆ ---    ┆ ---    ┆ ---    │
    │ str        ┆ str                   ┆ str       ┆ str         ┆ str   ┆ str    ┆ str    ┆ str    │
    ╞════════════╪═══════════════════════╪═══════════╪═════════════╪═══════╪════════╪════════╪════════╡
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 40     ┆ 8-18   ┆ 44.4   │
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 37     ┆ 7-19   ┆ 36.8   │
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 38     ┆ 9-18   ┆ 50.0   │
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 36     ┆ 12-17  ┆ 70.6   │
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 37     ┆ 10-25  ┆ 40.0   │
    └────────────┴───────────────────────┴───────────┴─────────────┴───────┴────────┴────────┴────────┘



### Recipe 4 — Top scorers from the box-score release 🥇

For a whole-season leaderboard the
[`load_nba_player_boxscore`](../nba/reference/loaders.md#load_nba_player_boxscore)
release is your friend — it's a fast parquet download, no live API needed.
Here we average points per game and rank the top 10 scorers.


```python
box = safe('player boxscore release', lambda: sdv.nba.load_nba_player_boxscore(seasons=[SEASON]))
if box is not None and box.height:
    leaders = (
        box.filter(pl.col('minutes') > 0)
        .group_by(['athlete_display_name', 'team_abbreviation'])
        .agg(pl.len().alias('gp'),
             pl.col('points').mean().round(1).alias('ppg'),
             pl.col('rebounds').mean().round(1).alias('rpg'),
             pl.col('assists').mean().round(1).alias('apg'))
        .filter(pl.col('gp') >= 20)
        .sort('ppg', descending=True)
        .head(10)
    )
    out = leaders
else:
    out = 'box-score release unavailable'
out
```

    ✅ player boxscore release — 34883 rows





    shape: (10, 6)
    ┌─────────────────────────┬───────────────────┬─────┬──────┬──────┬──────┐
    │ athlete_display_name    ┆ team_abbreviation ┆ gp  ┆ ppg  ┆ rpg  ┆ apg  │
    │ ---                     ┆ ---               ┆ --- ┆ ---  ┆ ---  ┆ ---  │
    │ str                     ┆ str               ┆ u32 ┆ f64  ┆ f64  ┆ f64  │
    ╞═════════════════════════╪═══════════════════╪═════╪══════╪══════╪══════╡
    │ Luka Doncic             ┆ LAL               ┆ 64  ┆ 33.5 ┆ 7.7  ┆ 8.3  │
    │ Shai Gilgeous-Alexander ┆ OKC               ┆ 83  ┆ 30.5 ┆ 4.0  ┆ 6.8  │
    │ Jaylen Brown            ┆ BOS               ┆ 78  ┆ 28.4 ┆ 6.8  ┆ 5.0  │
    │ Anthony Edwards         ┆ MIN               ┆ 71  ┆ 27.8 ┆ 5.1  ┆ 3.6  │
    │ …                       ┆ …                 ┆ …   ┆ …    ┆ …    ┆ …    │
    │ Giannis Antetokounmpo   ┆ MIL               ┆ 36  ┆ 27.6 ┆ 9.8  ┆ 5.4  │
    │ Nikola Jokic            ┆ DEN               ┆ 71  ┆ 27.5 ┆ 12.9 ┆ 10.6 │
    │ Donovan Mitchell        ┆ CLE               ┆ 88  ┆ 27.5 ┆ 4.6  ┆ 5.1  │
    │ Lauri Markkanen         ┆ UTAH              ┆ 42  ┆ 26.7 ┆ 6.9  ┆ 2.1  │
    └─────────────────────────┴───────────────────┴─────┴──────┴──────┴──────┘



### Recipe 5 — Offense vs defense, every team 🛡️

The [`load_nba_team_boxscore`](../nba/reference/loaders.md#load_nba_team_boxscore)
release has one row per team-game with both `team_score` and
`opponent_team_score` — so points-for, points-against and net rating are a
single `group_by` away.


```python
tbox = safe('team boxscore release', lambda: sdv.nba.load_nba_team_boxscore(seasons=[SEASON]))
if tbox is not None and tbox.height:
    netrtg = (
        tbox.group_by('team_abbreviation')
        .agg(pl.len().alias('gp'),
             pl.col('team_score').mean().round(1).alias('off_ppg'),
             pl.col('opponent_team_score').mean().round(1).alias('def_ppg'))
        .with_columns((pl.col('off_ppg') - pl.col('def_ppg')).round(1).alias('net'))
        .sort('net', descending=True)
        .head(10)
    )
    out = netrtg
else:
    out = 'team box-score release unavailable'
out
```

    ✅ team boxscore release — 2652 rows





    shape: (10, 5)
    ┌───────────────────┬─────┬─────────┬─────────┬──────┐
    │ team_abbreviation ┆ gp  ┆ off_ppg ┆ def_ppg ┆ net  │
    │ ---               ┆ --- ┆ ---     ┆ ---     ┆ ---  │
    │ str               ┆ u32 ┆ f64     ┆ f64     ┆ f64  │
    ╞═══════════════════╪═════╪═════════╪═════════╪══════╡
    │ OKC               ┆ 97  ┆ 118.5   ┆ 108.0   ┆ 10.5 │
    │ STARS             ┆ 3   ┆ 41.3    ┆ 32.7    ┆ 8.6  │
    │ SA                ┆ 106 ┆ 118.2   ┆ 110.2   ┆ 8.0  │
    │ NY                ┆ 102 ┆ 116.4   ┆ 108.4   ┆ 8.0  │
    │ …                 ┆ …   ┆ …       ┆ …       ┆ …    │
    │ HOU               ┆ 88  ┆ 114.1   ┆ 109.4   ┆ 4.7  │
    │ DEN               ┆ 88  ┆ 121.1   ┆ 116.6   ┆ 4.5  │
    │ CHA               ┆ 84  ┆ 115.8   ┆ 111.5   ┆ 4.3  │
    │ CLE               ┆ 100 ┆ 117.4   ┆ 114.6   ┆ 2.8  │
    └───────────────────┴─────┴─────────┴─────────┴──────┘



### Recipe 6 — Who lived behind the arc? 🎯

Sum makes and attempts across the season to get each team's true
three-point percentage (game-level percentages can't just be averaged).
Reuses the `tbox` frame from Recipe 5 — no second download.


```python
if tbox is not None and tbox.height:
    three_pt = (
        tbox.group_by('team_abbreviation')
        .agg(pl.col('three_point_field_goals_made').sum().alias('made'),
             pl.col('three_point_field_goals_attempted').sum().alias('att'))
        .with_columns((100 * pl.col('made') / pl.col('att')).round(1).alias('three_pt_pct'))
        .filter(pl.col('att') > 0)
        .sort('three_pt_pct', descending=True)
        .head(10)
    )
    out = three_pt
else:
    out = 'team box-score release unavailable'
out
```




    shape: (10, 4)
    ┌───────────────────┬──────┬──────┬──────────────┐
    │ team_abbreviation ┆ made ┆ att  ┆ three_pt_pct │
    │ ---               ┆ ---  ┆ ---  ┆ ---          │
    │ str               ┆ i32  ┆ i32  ┆ f64          │
    ╞═══════════════════╪══════╪══════╪══════════════╡
    │ WORLD             ┆ 11   ┆ 26   ┆ 42.3         │
    │ STRIPES           ┆ 21   ┆ 52   ┆ 40.4         │
    │ DEN               ┆ 1221 ┆ 3127 ┆ 39.0         │
    │ MIL               ┆ 1240 ┆ 3205 ┆ 38.7         │
    │ …                 ┆ …    ┆ …    ┆ …            │
    │ LAC               ┆ 1033 ┆ 2807 ┆ 36.8         │
    │ ATL               ┆ 1269 ┆ 3455 ┆ 36.7         │
    │ MIN               ┆ 1254 ┆ 3423 ┆ 36.6         │
    │ OKC               ┆ 1336 ┆ 3662 ┆ 36.5         │
    └───────────────────┴──────┴──────┴──────────────┘



### Recipe 7 — Double-double machines 💪

A *double-double* is double digits in two of points / rebounds / assists.
Count the categories per player-game, keep the ones that cleared two, then
tally them up — straight from
[`load_nba_player_boxscore`](../nba/reference/loaders.md#load_nba_player_boxscore).


```python
pbox = safe('player boxscore release', lambda: sdv.nba.load_nba_player_boxscore(seasons=[SEASON]))
if pbox is not None and pbox.height:
    dd = (
        pbox.filter(pl.col('minutes') > 0)
        .with_columns(
            ((pl.col('points') >= 10).cast(pl.Int8)
             + (pl.col('rebounds') >= 10).cast(pl.Int8)
             + (pl.col('assists') >= 10).cast(pl.Int8)).alias('cats10'))
        .filter(pl.col('cats10') >= 2)
        .group_by(['athlete_display_name', 'team_abbreviation'])
        .agg(pl.len().alias('double_doubles'))
        .sort('double_doubles', descending=True)
        .head(10)
    )
    out = dd
else:
    out = 'player box-score release unavailable'
out
```

    ✅ player boxscore release — 34883 rows





    shape: (10, 3)
    ┌──────────────────────┬───────────────────┬────────────────┐
    │ athlete_display_name ┆ team_abbreviation ┆ double_doubles │
    │ ---                  ┆ ---               ┆ ---            │
    │ str                  ┆ str               ┆ u32            │
    ╞══════════════════════╪═══════════════════╪════════════════╡
    │ Karl-Anthony Towns   ┆ NY                ┆ 69             │
    │ Nikola Jokic         ┆ DEN               ┆ 61             │
    │ Victor Wembanyama    ┆ SA                ┆ 54             │
    │ Jalen Johnson        ┆ ATL               ┆ 51             │
    │ …                    ┆ …                 ┆ …              │
    │ Alperen Sengun       ┆ HOU               ┆ 37             │
    │ Donovan Clingan      ┆ POR               ┆ 37             │
    │ Rudy Gobert          ┆ MIN               ┆ 37             │
    │ Bam Adebayo          ┆ MIA               ┆ 35             │
    └──────────────────────┴───────────────────┴────────────────┘



### Recipe 8 — A tidy standings table 🏆

The [`load_nba_standings`](../nba/reference/loaders.md#load_nba_standings)
release ships in **long** format (one row per team × stat). Pivot the stats
you care about into columns to get a classic standings grid, sorted by
win percentage.


```python
stload = safe('standings release', lambda: sdv.nba.load_nba_standings(seasons=[SEASON]))
wanted = ['wins', 'losses', 'winPercent', 'playoffSeed', 'pointDifferential']
if stload is not None and stload.height and {'stat_name', 'value'}.issubset(stload.columns):
    table = (
        stload.filter(pl.col('stat_name').is_in(wanted))
        .select(['team_abbreviation', 'group_name', 'stat_name', 'value'])
        .pivot(values='value', index=['team_abbreviation', 'group_name'], on='stat_name')
        .sort('winPercent', descending=True)
        .head(12)
    )
    out = table
else:
    out = 'standings release unavailable'
out
```

    ✅ standings release — 690 rows





    shape: (12, 7)
    ┌─────────────────────┬────────────┬────────┬─────────────┬────────────────────┬────────────┬──────┐
    │ team_abbreviation   ┆ group_name ┆ losses ┆ playoffSeed ┆ pointDifferential  ┆ winPercent ┆ wins │
    │ ---                 ┆ ---        ┆ ---    ┆ ---         ┆ ---                ┆ ---        ┆ ---  │
    │ str                 ┆ str        ┆ f64    ┆ f64         ┆ f64                ┆ f64        ┆ f64  │
    ╞═════════════════════╪════════════╪════════╪═════════════╪════════════════════╪════════════╪══════╡
    │ OKC                 ┆ Western    ┆ 18.0   ┆ 1.0         ┆ 914.0              ┆ 0.7804878  ┆ 64.0 │
    │                     ┆ Conference ┆        ┆             ┆                    ┆            ┆      │
    │ SA                  ┆ Western    ┆ 20.0   ┆ 2.0         ┆ 681.0              ┆ 0.756098   ┆ 62.0 │
    │                     ┆ Conference ┆        ┆             ┆                    ┆            ┆      │
    │ DET                 ┆ Eastern    ┆ 22.0   ┆ 1.0         ┆ 669.0              ┆ 0.731707   ┆ 60.0 │
    │                     ┆ Conference ┆        ┆             ┆                    ┆            ┆      │
    │ BOS                 ┆ Eastern    ┆ 26.0   ┆ 2.0         ┆ 631.0              ┆ 0.682927   ┆ 56.0 │
    │                     ┆ Conference ┆        ┆             ┆                    ┆            ┆      │
    │ …                   ┆ …          ┆ …      ┆ …           ┆ …                  ┆ …          ┆ …    │
    │ HOU                 ┆ Western    ┆ 30.0   ┆ 5.0         ┆ 428.0              ┆ 0.634146   ┆ 52.0 │
    │                     ┆ Conference ┆        ┆             ┆                    ┆            ┆      │
    │ MIN                 ┆ Western    ┆ 33.0   ┆ 6.0         ┆ 275.0              ┆ 0.597561   ┆ 49.0 │
    │                     ┆ Conference ┆        ┆             ┆                    ┆            ┆      │
    │ ATL                 ┆ Eastern    ┆ 36.0   ┆ 6.0         ┆ 198.0              ┆ 0.5609756  ┆ 46.0 │
    │                     ┆ Conference ┆        ┆             ┆                    ┆            ┆      │
    │ TOR                 ┆ Eastern    ┆ 36.0   ┆ 5.0         ┆ 232.0              ┆ 0.5609756  ┆ 46.0 │
    │                     ┆ Conference ┆        ┆             ┆                    ┆            ┆      │
    └─────────────────────┴────────────┴────────┴─────────────┴────────────────────┴────────────┴──────┘



### Recipe 9 — Built on threes (shot release + a join) 🧱

[`load_nba_shots`](../nba/reference/loaders.md#load_nba_shots) is one row per
made shot with a `score_value`. Tally points from twos vs threes per team,
then **join** team abbreviations from the box-score release to find who
leaned hardest on the long ball.


```python
shots = safe('shots release', lambda: sdv.nba.load_nba_shots(seasons=[SEASON]))
if shots is not None and shots.height and tbox is not None and tbox.height:
    fg = shots.filter(pl.col('score_value').is_in([2, 3]))
    reliance = (
        fg.group_by('team_id')
        .agg(pl.col('score_value').filter(pl.col('score_value') == 3).len().alias('threes_made'),
             pl.col('score_value').sum().alias('points_from_fg'))
        .with_columns((3 * pl.col('threes_made')).alias('points_from_threes'))
        .with_columns((100 * pl.col('points_from_threes') / pl.col('points_from_fg'))
                      .round(1).alias('pct_pts_from_3'))
        .filter(pl.col('threes_made') >= 500)  # drop All-Star / special rosters
    )
    abbr = tbox.select(['team_id', 'team_abbreviation']).unique()
    out = (reliance.join(abbr, on='team_id', how='left')
           .select(['team_abbreviation', 'threes_made', 'pct_pts_from_3'])
           .sort('pct_pts_from_3', descending=True).head(10))
else:
    out = 'shots / team box-score release unavailable'
out
```

    ✅ shots release — 298411 rows





    shape: (10, 3)
    ┌───────────────────┬─────────────┬────────────────┐
    │ team_abbreviation ┆ threes_made ┆ pct_pts_from_3 │
    │ ---               ┆ ---         ┆ ---            │
    │ str               ┆ u32         ┆ f64            │
    ╞═══════════════════╪═════════════╪════════════════╡
    │ CHA               ┆ 1373        ┆ 50.0           │
    │ GS                ┆ 1316        ┆ 48.2           │
    │ MIL               ┆ 1240        ┆ 46.9           │
    │ BOS               ┆ 1377        ┆ 46.8           │
    │ …                 ┆ …           ┆ …              │
    │ BKN               ┆ 1073        ┆ 44.6           │
    │ MEM               ┆ 1143        ┆ 43.3           │
    │ ATL               ┆ 1269        ┆ 43.0           │
    │ CHI               ┆ 1144        ┆ 42.9           │
    └───────────────────┴─────────────┴────────────────┘



### Recipe 10 — Head-to-head, game by game 🤝

Filter the team box-score release to one matchup and you get the full
season series — every meeting, the score, and who won. Swap the two
abbreviations for any rivalry you like.


```python
TEAM_A, TEAM_B = 'BOS', 'NY'
if tbox is not None and tbox.height and 'opponent_team_abbreviation' in tbox.columns:
    series = (
        tbox.filter((pl.col('team_abbreviation') == TEAM_A)
                    & (pl.col('opponent_team_abbreviation') == TEAM_B))
        .select([c for c in ['game_date', 'team_home_away', 'team_score',
                             'opponent_team_score', 'team_winner']
                 if c in tbox.columns])
        .sort('game_date')
    )
    out = series if series.height else f'no {TEAM_A} vs {TEAM_B} games in {SEASON}'
else:
    out = 'team box-score release unavailable'
out
```




    shape: (4, 5)
    ┌────────────┬────────────────┬────────────┬─────────────────────┬─────────────┐
    │ game_date  ┆ team_home_away ┆ team_score ┆ opponent_team_score ┆ team_winner │
    │ ---        ┆ ---            ┆ ---        ┆ ---                 ┆ ---         │
    │ date       ┆ str            ┆ i32        ┆ i32                 ┆ bool        │
    ╞════════════╪════════════════╪════════════╪═════════════════════╪═════════════╡
    │ 2025-10-24 ┆ away           ┆ 95         ┆ 105                 ┆ false       │
    │ 2025-12-02 ┆ home           ┆ 123        ┆ 117                 ┆ true        │
    │ 2026-02-08 ┆ home           ┆ 89         ┆ 111                 ┆ false       │
    │ 2026-04-09 ┆ away           ┆ 106        ┆ 112                 ┆ false       │
    └────────────┴────────────────┴────────────┴─────────────────────┴─────────────┘



### Recipe 11 — Who's banged up? 🩹 (pandas interop)

[`espn_nba_injuries`](../nba/reference/site.md#espn_nba_injuries) hits ESPN
live for the league-wide injury report. Ask for a **pandas** frame with
`return_as_pandas=True` (a handy interop point), count the listed players
per team, then hand the result back to polars for the final sort.


```python
import ast

inj = safe('injuries', lambda: sdv.nba.espn_nba_injuries(return_as_pandas=True))
if inj is not None and getattr(inj, 'shape', (0,))[0] and 'injuries' in inj.columns:
    def _n_listed(s):
        try:
            v = ast.literal_eval(s) if isinstance(s, str) else s
            return len(v) if isinstance(v, list) else 0
        except Exception:
            return 0
    inj = inj.copy()
    inj['players_listed'] = inj['injuries'].apply(_n_listed)
    out = (pl.from_pandas(inj[['display_name', 'players_listed']])
           .filter(pl.col('players_listed') > 0)
           .sort('players_listed', descending=True)
           .head(12))
else:
    out = 'injury report unavailable (off-season or feed down)'
out
```

    ✅ injuries — 27 rows





    shape: (12, 2)
    ┌───────────────────────┬────────────────┐
    │ display_name          ┆ players_listed │
    │ ---                   ┆ ---            │
    │ str                   ┆ i64            │
    ╞═══════════════════════╪════════════════╡
    │ Memphis Grizzlies     ┆ 14             │
    │ Chicago Bulls         ┆ 11             │
    │ Indiana Pacers        ┆ 9              │
    │ Sacramento Kings      ┆ 9              │
    │ …                     ┆ …              │
    │ Milwaukee Bucks       ┆ 7              │
    │ New Orleans Pelicans  ┆ 6              │
    │ Golden State Warriors ┆ 4              │
    │ Miami Heat            ┆ 4              │
    └───────────────────────┴────────────────┘



## 🎬 Play-by-play & game rosters

Now for the granular stuff. [`espn_nba_pbp`](../nba/reference/additional.md)
returns the **whole game payload** as a dict — play-by-play, win probability,
box score, and header — keyed by `game_id` (an ESPN event id). Pair it with
[`espn_nba_game_rosters`](../nba/reference/additional.md) for who actually
suited up.

We'll use Game 1 of the 2024 Finals (`game_id=401585660`).


```python
GAME_ID = 401585660
pbp = safe('pbp payload', lambda: sdv.nba.espn_nba_pbp(game_id=GAME_ID))
(list(pbp.keys())[:8] if isinstance(pbp, dict) else 'pbp unavailable')
```

    ✅ pbp payload — 22 rows





    ['gameId',
     'plays',
     'winprobability',
     'boxscore',
     'header',
     'format',
     'broadcasts',
     'videos']




```python
plays = (pl.DataFrame(pbp['plays'], infer_schema_length=None)
         if isinstance(pbp, dict) and pbp.get('plays') else None)
cols = ['period.number', 'clock.displayValue', 'text', 'homeScore', 'awayScore', 'scoringPlay']
(plays.select([c for c in cols if c in plays.columns]).head()
 if plays is not None and plays.height else 'no plays parsed')
```




    shape: (5, 6)
    ┌───────────────┬────────────────────┬───────────────────────┬───────────┬───────────┬─────────────┐
    │ period.number ┆ clock.displayValue ┆ text                  ┆ homeScore ┆ awayScore ┆ scoringPlay │
    │ ---           ┆ ---                ┆ ---                   ┆ ---       ┆ ---       ┆ ---         │
    │ i64           ┆ str                ┆ str                   ┆ i64       ┆ i64       ┆ bool        │
    ╞═══════════════╪════════════════════╪═══════════════════════╪═══════════╪═══════════╪═════════════╡
    │ 1             ┆ 12:00              ┆ Myles Turner vs.      ┆ 0         ┆ 0         ┆ false       │
    │               ┆                    ┆ Anthony Davis…        ┆           ┆           ┆             │
    │ 1             ┆ 11:42              ┆ Aaron Nesmith makes   ┆ 0         ┆ 3         ┆ true        │
    │               ┆                    ┆ 26-foot th…           ┆           ┆           ┆             │
    │ 1             ┆ 11:17              ┆ Austin Reaves misses  ┆ 0         ┆ 3         ┆ false       │
    │               ┆                    ┆ driving l…            ┆           ┆           ┆             │
    │ 1             ┆ 11:14              ┆ Austin Reaves         ┆ 0         ┆ 3         ┆ false       │
    │               ┆                    ┆ offensive reboun…     ┆           ┆           ┆             │
    │ 1             ┆ 11:12              ┆ Austin Reaves misses  ┆ 0         ┆ 3         ┆ false       │
    │               ┆                    ┆ 14-foot t…            ┆           ┆           ┆             │
    └───────────────┴────────────────────┴───────────────────────┴───────────┴───────────┴─────────────┘



### Slice it: every 3-pointer in the game 🎯

The `plays` frame is just polars — so a scoring slice is one filter away.
Here we pull made three-pointers in chronological order.


```python
if plays is not None and plays.height:
    threes = (
        plays.filter(pl.col('scoringPlay') == True)
        .filter(pl.col('text').str.contains('(?i)three point|3pt|three-point'))
        .select([c for c in ['period.number', 'clock.displayValue', 'text',
                              'homeScore', 'awayScore'] if c in plays.columns])
    )
    out = threes.head(10) if threes.height else 'no three-pointers matched the text filter'
else:
    out = 'no plays to slice'
out
```




    shape: (10, 5)
    ┌───────────────┬────────────────────┬─────────────────────────────────┬───────────┬───────────┐
    │ period.number ┆ clock.displayValue ┆ text                            ┆ homeScore ┆ awayScore │
    │ ---           ┆ ---                ┆ ---                             ┆ ---       ┆ ---       │
    │ i64           ┆ str                ┆ str                             ┆ i64       ┆ i64       │
    ╞═══════════════╪════════════════════╪═════════════════════════════════╪═══════════╪═══════════╡
    │ 1             ┆ 11:42              ┆ Aaron Nesmith makes 26-foot th… ┆ 0         ┆ 3         │
    │ 1             ┆ 10:14              ┆ Andrew Nembhard makes 23-foot … ┆ 2         ┆ 6         │
    │ 1             ┆ 9:58               ┆ LeBron James makes 26-foot thr… ┆ 5         ┆ 6         │
    │ 1             ┆ 5:44               ┆ Myles Turner makes 25-foot thr… ┆ 15        ┆ 19        │
    │ …             ┆ …                  ┆ …                               ┆ …         ┆ …         │
    │ 2             ┆ 10:05              ┆ Max Christie makes 25-foot thr… ┆ 41        ┆ 40        │
    │ 2             ┆ 9:39               ┆ Obi Toppin makes 27-foot three… ┆ 41        ┆ 43        │
    │ 2             ┆ 8:44               ┆ Aaron Nesmith makes 26-foot th… ┆ 41        ┆ 49        │
    │ 2             ┆ 7:02               ┆ Rui Hachimura makes 22-foot th… ┆ 51        ┆ 51        │
    └───────────────┴────────────────────┴─────────────────────────────────┴───────────┴───────────┘



### Who played? Game rosters 📋

[`espn_nba_game_rosters`](../nba/reference/additional.md) returns both teams'
rosters for a single game, one row per athlete — including the `starter`
flag and jersey number.


```python
grosters = safe('game rosters', lambda: sdv.nba.espn_nba_game_rosters(game_id=GAME_ID))
cols = ['athlete_display_name', 'team_abbreviation', 'starter', 'jersey', 'position_name']
(grosters.select([c for c in cols if c in grosters.columns]).head(10)
 if grosters is not None and grosters.height else 'game rosters unavailable')
```

    ✅ game rosters — 26 rows





    shape: (10, 4)
    ┌──────────────────────┬───────────────────┬─────────┬────────┐
    │ athlete_display_name ┆ team_abbreviation ┆ starter ┆ jersey │
    │ ---                  ┆ ---               ┆ ---     ┆ ---    │
    │ str                  ┆ str               ┆ bool    ┆ str    │
    ╞══════════════════════╪═══════════════════╪═════════╪════════╡
    │ LeBron James         ┆ LAL               ┆ true    ┆ 23     │
    │ Anthony Davis        ┆ LAL               ┆ true    ┆ 23     │
    │ Rui Hachimura        ┆ LAL               ┆ true    ┆ 28     │
    │ Spencer Dinwiddie    ┆ LAL               ┆ true    ┆ 26     │
    │ …                    ┆ …                 ┆ …       ┆ …      │
    │ Cam Reddish          ┆ LAL               ┆ false   ┆ 5      │
    │ Jaxson Hayes         ┆ LAL               ┆ false   ┆ 11     │
    │ Max Christie         ┆ LAL               ┆ false   ┆ 00     │
    │ Harry Giles III      ┆ LAL               ┆ false   ┆ 20     │
    └──────────────────────┴───────────────────┴─────────┴────────┘



## 📦 Bulk season data with the loaders

When you want *everything* for a season at once — not one game at a time —
the `load_nba_*` loaders pull pre-built parquet releases. They're fast,
reliable, and don't depend on a live API being up.

| Loader | Grain |
|---|---|
| [`load_nba_schedule`](../nba/reference/loaders.md#load_nba_schedule) | one row per game |
| [`load_nba_player_boxscore`](../nba/reference/loaders.md#load_nba_player_boxscore) | one row per player-game |
| [`load_nba_standings`](../nba/reference/loaders.md#load_nba_standings) | one row per team-season |


```python
sched = safe('schedule release', lambda: sdv.nba.load_nba_schedule(seasons=[SEASON]))
cols = ['id', 'date', 'home_display_name', 'away_display_name', 'home_score', 'away_score']
(sched.select([c for c in cols if c in sched.columns]).head()
 if sched is not None and sched.height else 'schedule release unavailable')
```

    ✅ schedule release — 1330 rows





    shape: (5, 6)
    ┌───────────┬───────────────────┬───────────────────┬───────────────────┬────────────┬────────────┐
    │ id        ┆ date              ┆ home_display_name ┆ away_display_name ┆ home_score ┆ away_score │
    │ ---       ┆ ---               ┆ ---               ┆ ---               ┆ ---        ┆ ---        │
    │ i32       ┆ str               ┆ str               ┆ str               ┆ i32        ┆ i32        │
    ╞═══════════╪═══════════════════╪═══════════════════╪═══════════════════╪════════════╪════════════╡
    │ 401859967 ┆ 2026-06-14T00:30Z ┆ San Antonio Spurs ┆ New York Knicks   ┆ 90         ┆ 94         │
    │ 401859966 ┆ 2026-06-11T00:30Z ┆ New York Knicks   ┆ San Antonio Spurs ┆ 107        ┆ 106        │
    │ 401859965 ┆ 2026-06-09T00:30Z ┆ New York Knicks   ┆ San Antonio Spurs ┆ 111        ┆ 115        │
    │ 401859964 ┆ 2026-06-06T00:30Z ┆ San Antonio Spurs ┆ New York Knicks   ┆ 104        ┆ 105        │
    │ 401859963 ┆ 2026-06-04T00:30Z ┆ San Antonio Spurs ┆ New York Knicks   ┆ 95         ┆ 105        │
    └───────────┴───────────────────┴───────────────────┴───────────────────┴────────────┴────────────┘



### Pipeline: the highest-scoring games of the season 🔥

With the schedule release in hand, a combined-points leaderboard is a quick
polars pipeline — cast the scores, sum them, sort descending.


```python
if sched is not None and sched.height and {'home_score', 'away_score'}.issubset(sched.columns):
    hot = (
        sched.with_columns(
            (pl.col('home_score').cast(pl.Int64, strict=False)
             + pl.col('away_score').cast(pl.Int64, strict=False)).alias('total_points')
        )
        .filter(pl.col('total_points').is_not_null())
        .sort('total_points', descending=True)
        .select([c for c in ['date', 'home_display_name', 'away_display_name',
                              'home_score', 'away_score', 'total_points'] if c in sched.columns])
        .head(10)
    )
    out = hot
else:
    out = 'schedule release unavailable'
out
```




    shape: (10, 5)
    ┌───────────────────┬──────────────────────┬────────────────────────┬────────────┬────────────┐
    │ date              ┆ home_display_name    ┆ away_display_name      ┆ home_score ┆ away_score │
    │ ---               ┆ ---                  ┆ ---                    ┆ ---        ┆ ---        │
    │ str               ┆ str                  ┆ str                    ┆ i32        ┆ i32        │
    ╞═══════════════════╪══════════════════════╪════════════════════════╪════════════╪════════════╡
    │ 2025-12-21T20:30Z ┆ Atlanta Hawks        ┆ Chicago Bulls          ┆ 150        ┆ 152        │
    │ 2025-11-17T01:00Z ┆ Utah Jazz            ┆ Chicago Bulls          ┆ 150        ┆ 147        │
    │ 2026-03-25T23:00Z ┆ Philadelphia 76ers   ┆ Chicago Bulls          ┆ 157        ┆ 137        │
    │ 2026-04-08T00:00Z ┆ New Orleans Pelicans ┆ Utah Jazz              ┆ 156        ┆ 137        │
    │ …                 ┆ …                    ┆ …                      ┆ …          ┆ …          │
    │ 2026-04-01T23:00Z ┆ Washington Wizards   ┆ Philadelphia 76ers     ┆ 131        ┆ 153        │
    │ 2026-03-12T02:30Z ┆ LA Clippers          ┆ Minnesota Timberwolves ┆ 153        ┆ 128        │
    │ 2025-11-15T01:00Z ┆ Milwaukee Bucks      ┆ Charlotte Hornets      ┆ 147        ┆ 134        │
    │ 2026-01-10T18:00Z ┆ Cleveland Cavaliers  ┆ Minnesota Timberwolves ┆ 146        ┆ 134        │
    └───────────────────┴──────────────────────┴────────────────────────┴────────────┴────────────┘



## 🎉 Where to next

You just toured the **premium `espn_nba_*` surface** plus the season
loaders — teams, scoreboard, standings, rosters, schedules, player game
logs, play-by-play, and bulk box scores. A few parting tips:

- Pass `return_as_pandas=True` to any wrapper for a pandas frame instead of polars.
- ESPN `espn_nba_*` wrappers also accept `return_parsed=False` for the raw JSON dict.
- Full reference lives in the **NBA** section of the sidebar:
  [ESPN site API](../nba/reference/site.md) ·
  [ESPN web API](../nba/reference/web.md) ·
  [ESPN core API](../nba/reference/core.md) ·
  [additional functions](../nba/reference/additional.md) ·
  [loaders](../nba/reference/loaders.md)
- R user? The same surface lives in [hoopR](https://hoopR.sportsdataverse.org).
- Need raw NBA Stats endpoints? See [nba_api](https://github.com/swar/nba_api).

Now go break down some film — and may your jumper always find the bottom of
the net! 🏀🔥
