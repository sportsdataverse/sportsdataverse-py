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


```python
plays = (pl.DataFrame(pbp['plays'], infer_schema_length=None)
         if isinstance(pbp, dict) and pbp.get('plays') else None)
cols = ['period.number', 'clock.displayValue', 'text', 'homeScore', 'awayScore', 'scoringPlay']
(plays.select([c for c in cols if c in plays.columns]).head()
 if plays is not None and plays.height else 'no plays parsed')
```

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
