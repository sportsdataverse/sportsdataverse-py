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


```python
schedule.select([
    'game_id', 'game_date', 'home_team', 'away_team',
    'home_score', 'away_score', 'winner', 'game_type',
]).head()
```

## 👥 Rosters (loader)

[`load_pwhl_rosters`](../pwhl/reference/loaders.md#load_pwhl_rosters) gives one row per player per team, split into skaters and goalies via the `player_type` column.


```python
rosters = pwhl.load_pwhl_rosters(seasons=[2024])
rosters.select([
    'team', 'team_abbr', 'player_type', 'first_name', 'last_name',
    'jersey_number', 'position',
]).head()
```

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


```python
goalie_box = pwhl.load_pwhl_goalie_box(seasons=[2024])
goalie_box.select([
    'game_id', 'first_name', 'last_name',
    'saves', 'shots_against', 'goals_against', 'time_on_ice',
]).head()
```

## 🎬 Play-by-play (loader)

[`load_pwhl_pbp`](../pwhl/reference/loaders.md#load_pwhl_pbp) returns a wide event log. The `event` column tags each row as `faceoff`, `shot`, `goal`, or `penalty` — and there are several coordinate systems (`x_coord`/`y_coord` plus rink-normalized `*_fixed` / `*_right` variants) for drawing rink plots.


```python
pbp = pwhl.load_pwhl_pbp(seasons=[2024])
pbp.shape
```


```python
(pbp
    .group_by('event')
    .agg(pl.len().alias('events'))
    .sort('events', descending=True))
```

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


```python
# Top goal-scorers from the play-by-play feed
(goal_events
    .filter(pl.col('player_name_last').is_not_null())
    .group_by(['player_name_first', 'player_name_last'])
    .agg(pl.len().alias('goals'))
    .sort('goals', descending=True)
    .head(10))
```

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


```python
# A game_id from the loader schedule (offline-safe), then enrich it live.
gid = int(schedule['game_id'][0])
pbp_live = safe(f'PWHL pbp {gid}', lambda: pwhl.pwhl_pbp(game_id=gid))
corsi = safe(f'PWHL corsi {gid}', lambda: pwhl.pwhl_game_corsi(game_id=gid))
print('live pbp rows:', None if pbp_live is None else pbp_live.height,
      '| corsi rows:', None if corsi is None else corsi.height)
```

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

## ✨ Bonus: tidy goal log + pandas interop

[`load_pwhl_scoring_summary`](../pwhl/reference/loaders.md#load_pwhl_scoring_summary) is a clean per-goal log — scorer plus up to two assists, with situation flags like power play, short handed, and game-winning. And because every loader takes `return_as_pandas=True`, dropping into the pandas world is one keyword away.


```python
scoring = pwhl.load_pwhl_scoring_summary(seasons=[2024])
scoring.select([
    'game_id', 'period', 'time', 'team_abbr',
    'scorer_first', 'scorer_last', 'is_power_play', 'is_game_winning',
]).head()
```


```python
# Same skater box, but as a pandas DataFrame — group with the pandas API.
skater_pd = pwhl.load_pwhl_skater_box(seasons=[2024], return_as_pandas=True)
print('type:', type(skater_pd).__name__, '| shape:', skater_pd.shape)
(skater_pd
    .groupby(['first_name', 'last_name'], as_index=False)['points'].sum()
    .sort_values('points', ascending=False)
    .head(10))
```

## 🎉 Where to next

- 📦 **Loaders** are your offline-friendly workhorses — stack seasons with `seasons=[2024, 2025]` and pass `return_as_pandas=True` for pandas.
- 🛰️ **Live wrappers** (`pwhl_*`) pull fresh data and add analytics (Corsi, TOI, shifts) — no key required.
- Full reference: the **PWHL → [Loaders](../pwhl/reference/loaders.md)** and **[Additional functions](../pwhl/reference/additional.md)** pages in the sidebar.
- Junior & minor hockey? The same HockeyTech surface powers the AHL / OHL / WHL / QMJHL — see `11_junior_hockey_intro.ipynb`.
- The men's game and the modern NHL APIs live in `07_nhl_intro.ipynb`.
- R user? The same data lives in [fastRhockey](https://fastRhockey.sportsdataverse.org) (NHL + PWHL).

Now go tell the story of the PWHL — the data's all here. 🏒💜
