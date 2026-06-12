---
title: NHL tutorial
sidebar_label: NHL
sidebar_position: 9
---

# 🏒 NHL hockey with `sportsdataverse-py`

Welcome to the show! 🎉 `sportsdataverse.nhl` gives you the **NHL's own modern
feed** — the same `api-web.nhle.com` data that powers NHL.com — plus the shiny
**NHL EDGE** puck-and-player tracking layer, the `api.nhle.com` stats-REST and
records flat APIs, an ESPN fallback, and fast parquet loaders. All of it hands
you tidy **polars** DataFrames, ready to model. 🚀

We'll **lead with the premium native wrappers** (the `nhl_*` and `nhl_edge_*`
functions) — they're the league's first-party data, no key required — and keep
ESPN (`espn_nhl_*`) as a friendly secondary path.

R user? The companion package is
[fastRhockey](https://fastRhockey.sportsdataverse.org) (NHL + PWHL). Let's drop
the puck! 🏒

## 🧰 The toolbox

Every native call returns a tidy **polars** `DataFrame` by default — pass
`return_as_pandas=True` for pandas, or `return_parsed=False` for the raw JSON.
Here's the kit we'll use (click any name for the full reference). The ⭐ rows
are the **premium native NHL feed** — start there.

| Function | What it gives you | Source |
|---|---|---|
| [`nhl_web_schedule`](../nhl/reference/nhl_api_web.md#nhl_web_schedule) | A day's games + scores, native `id`s | ⭐ NHL api-web |
| [`nhl_web_pbp`](../nhl/reference/nhl_api_web.md#nhl_web_pbp) | Event-level play-by-play (one row per event) | ⭐ NHL api-web |
| [`nhl_boxscore`](../nhl/reference/nhl_api_web.md#nhl_boxscore) | One row per player (skaters + goalies) | ⭐ NHL api-web |
| [`nhl_standings`](../nhl/reference/nhl_api_web.md#nhl_standings) | Team standings with conference/division | ⭐ NHL api-web |
| [`nhl_roster`](../nhl/reference/nhl_api_web.md#nhl_roster) | A club's roster for a season | ⭐ NHL api-web |
| [`nhl_club_schedule_season`](../nhl/reference/nhl_api_web.md#nhl_club_schedule_season) | A team's full-season schedule | ⭐ NHL api-web |
| [`nhl_player_game_log`](../nhl/reference/nhl_api_web.md#nhl_player_game_log) | A player's game-by-game line | ⭐ NHL api-web |
| [`nhl_player_landing`](../nhl/reference/nhl_api_web.md#nhl_player_landing) | A player's bio + career snapshot | ⭐ NHL api-web |
| [`nhl_skater_leaders`](../nhl/reference/nhl_api_web.md#nhl_skater_leaders) | Season skater leaderboard | ⭐ NHL api-web |
| [`nhl_goalie_leaders`](../nhl/reference/nhl_api_web.md#nhl_goalie_leaders) | Season goalie leaderboard | ⭐ NHL api-web |
| [`nhl_edge_skater_skating_speed_detail`](../nhl/reference/nhl_edge.md#nhl_edge_skater_skating_speed_detail) | A skater's tracked speed vs league avg + percentile | ⭐ NHL EDGE |
| [`nhl_edge_skater_landing`](../nhl/reference/nhl_edge.md#nhl_edge_skater_landing) | EDGE skater leaderboards (hardest shot, top speed…) | ⭐ NHL EDGE |
| [`nhl_edge_team_landing`](../nhl/reference/nhl_edge.md#nhl_edge_team_landing) | EDGE team-level tracking leaders | ⭐ NHL EDGE |
| [`nhl_stats_rest_leaders_skaters`](../nhl/reference/nhl_stats_rest.md#nhl_stats_rest_leaders_skaters) | Stats-REST top-10 skaters by attribute | ⭐ NHL stats-REST |
| [`nhl_stats_rest_leaders_goalies`](../nhl/reference/nhl_stats_rest.md#nhl_stats_rest_leaders_goalies) | Stats-REST top-10 goalies by attribute | ⭐ NHL stats-REST |
| [`nhl_records_franchises`](../nhl/reference/nhl_records.md#nhl_records_franchises) | Every franchise in NHL history (Records API) | ⭐ NHL records |
| [`load_nhl_schedule`](../nhl/reference/loaders.md#load_nhl_schedule) | Pre-built schedule parquet (offline-friendly) | 📦 loader |
| [`load_nhl_team_box`](../nhl/reference/additional.md#load_nhl_team_box) | Pre-built team box parquet | 📦 loader |
| [`load_nhl_player_box`](../nhl/reference/additional.md#load_nhl_player_box) | Pre-built player box parquet | 📦 loader |
| [`espn_nhl_teams`](../nhl/reference/additional.md#espn_nhl_teams) | ESPN team directory | ESPN |
| [`espn_nhl_schedule`](../nhl/reference/additional.md#espn_nhl_schedule) | ESPN schedule for a date | ESPN |
| [`espn_nhl_pbp`](../nhl/reference/additional.md#espn_nhl_pbp) | ESPN play-by-play (a dict) | ESPN |
| [`espn_nhl_standings`](../nhl/reference/site.md#espn_nhl_standings) | ESPN standings | ESPN |


## 🔌 Setup

```sh
pip install sportsdataverse
```

No API key needed — the NHL's public feeds ship ready to go. 😊


```python
import polars as pl
import sportsdataverse as sdv
import sportsdataverse.nhl as nhl
```

The native feeds are live and seasonal (and occasionally throttle), so a tiny
`safe()` helper runs each network call defensively — you get the frame when the
feed is up, and a friendly one-liner when it isn't (never a scary traceback). 🛟

We'll reference the **2024 Stanley Cup Final Game 7** throughout: Florida
Panthers 2, Edmonton Oilers 1 (June 24, 2024). Note the native game id
`2023030417` (season + game-type + sequence) is **different** from ESPN's
`401675111` for the very same game.


```python
def safe(label, thunk):
    try:
        out = thunk()
        print(f'✅ {label}')
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f'⏭️  {label}: unavailable right now ({type(e).__name__})')
        return None


# Game 7, 2024 Stanley Cup Final — two ids for the same game
NATIVE_GAME = 2023030417   # api-web.nhle.com
ESPN_GAME = 401675111      # ESPN
SEASON = 20232024          # NHL season strings are start+end years
```

## ⭐ The premium native feed (`nhl_*`)

These wrappers hit the league's own `api-web.nhle.com`. They're first-party,
richly detailed, and return polars directly. Let's tour the headline calls.

### 📅 Schedule

[`nhl_web_schedule(date='YYYY-MM-DD')`](../nhl/reference/nhl_api_web.md#nhl_web_schedule)
returns a day's games with `home_team_*` / `away_team_*` columns and the native `id`.


```python
sched = safe('native schedule', lambda: nhl.nhl_web_schedule(date='2024-06-24'))
cols = ['id', 'game_state', 'home_team_abbrev', 'home_team_score',
        'away_team_abbrev', 'away_team_score']
(sched.select([c for c in cols if c in sched.columns]).head()
 if sched is not None else 'schedule unavailable')
```

### 🥅 Play-by-play

[`nhl_web_pbp(game_id=...)`](../nhl/reference/nhl_api_web.md#nhl_web_pbp) returns
one row per event in clean `snake_case` — `type_desc_key`, `time_in_period`,
`period_descriptor_number`, plus shot coordinates `details_x_coord` /
`details_y_coord`. That coordinate pair is your gateway to shot maps. 🗺️


```python
pbp = safe('native pbp', lambda: nhl.nhl_web_pbp(game_id=NATIVE_GAME))
if pbp is not None:
    print('pbp shape:', pbp.shape)
    show = ['period_descriptor_number', 'time_in_period', 'type_desc_key',
            'details_event_owner_team_id', 'details_x_coord', 'details_y_coord']
    out = pbp.select([c for c in show if c in pbp.columns]).head()
else:
    out = 'pbp unavailable'
out
```


```python
# Event-type mix for the game — native uses `type_desc_key`
(pbp.group_by('type_desc_key').agg(pl.len().alias('events'))
    .sort('events', descending=True).head(10)
 if pbp is not None else 'pbp unavailable')
```

### 📊 Boxscore

[`nhl_boxscore(game_id=...)`](../nhl/reference/nhl_api_web.md#nhl_boxscore) gives
one row per player (skaters + goalies) with `home_away`, `position`, and the
per-player stat line. Let's pull the night's top scorers.


```python
box = safe('native boxscore', lambda: nhl.nhl_boxscore(game_id=NATIVE_GAME))
if box is not None:
    out = (box.filter(pl.col('position') != 'G')
              .select(['name_default', 'home_away', 'position',
                       'goals', 'assists', 'points', 'sog', 'toi'])
              .sort('points', descending=True).head())
else:
    out = 'boxscore unavailable'
out
```

### 🏆 Standings

[`nhl_standings(date='YYYY-MM-DD')`](../nhl/reference/nhl_api_web.md#nhl_standings)
returns one row per team with conference/division context and points — pass any
date to get the table *as of* that day.


```python
standings = safe('native standings', lambda: nhl.nhl_standings(date='2024-04-15'))
if standings is not None:
    out = (standings.select(['team_name_default', 'conference_name', 'division_name',
                             'games_played', 'wins', 'losses', 'points'])
                    .sort('points', descending=True).head())
else:
    out = 'standings unavailable'
out
```

## 🛰️ NHL EDGE — player & puck tracking

EDGE is the league's tracking layer: skating speed, shot speed, zone time,
skating distance — all measured by sensors. The `*_detail` calls return a
player's tracked values **alongside the league average and percentile**, and
the `*_landing` calls return wide leaderboard frames.

| Function | Tracking metric |
|---|---|
| [`nhl_edge_skater_skating_speed_detail`](../nhl/reference/nhl_edge.md#nhl_edge_skater_skating_speed_detail) | top speed, speed bursts, vs league avg |
| [`nhl_edge_skater_landing`](../nhl/reference/nhl_edge.md#nhl_edge_skater_landing) | skater leaders (hardest shot, top speed…) |
| [`nhl_edge_team_landing`](../nhl/reference/nhl_edge.md#nhl_edge_team_landing) | team-level tracking leaders |

Here's Connor McDavid's (`8478402`) skating-speed detail for 2023-24 — how does
the fastest man in the league stack up? ⚡


```python
edge = safe('EDGE skating speed',
            lambda: nhl.nhl_edge_skater_skating_speed_detail(player_id=8478402, season=SEASON))
if edge is not None:
    keep = [c for c in (
        'skating_speed_details_max_skating_speed_imperial',
        'skating_speed_details_max_skating_speed_league_avg_imperial',
        'skating_speed_details_max_skating_speed_percentile',
        'skating_speed_details_bursts_over22_value',
        'skating_speed_details_bursts_over22_percentile',
    ) if c in edge.columns]
    out = edge.select(keep) if keep else edge.head()
else:
    out = 'EDGE detail unavailable'
out
```

## 📈 Stats-REST & Records flat APIs

Two more first-party surfaces round out the kit:

- **Stats-REST** (`api.nhle.com/stats/rest`) — clean leaderboard frames.
  [`nhl_stats_rest_leaders_skaters(attribute=...)`](../nhl/reference/nhl_stats_rest.md#nhl_stats_rest_leaders_skaters)
  returns a tidy top-10 for any attribute (`goals`, `points`, `assists`, …);
  [`nhl_stats_rest_leaders_goalies`](../nhl/reference/nhl_stats_rest.md#nhl_stats_rest_leaders_goalies)
  is the goalie twin.
- **Records** (`records.nhl.com`) — historical reference data, e.g.
  [`nhl_records_franchises`](../nhl/reference/nhl_records.md#nhl_records_franchises).


```python
leaders = safe('stats-rest goal leaders',
               lambda: nhl.nhl_stats_rest_leaders_skaters(attribute='goals'))
if leaders is not None:
    keep = ['player_full_name', 'player_position_code', 'team_tri_code', 'goals']
    out = leaders.select([c for c in keep if c in leaders.columns]).head(10)
else:
    out = 'leaders unavailable'
out
```

## 🍳 Cookbook: common NHL tasks

Now the fun part — a few recipes you'll reach for constantly, all built on the
premium native feed.

### Recipe 1 — A game's boxscore + play-by-play 🎯

Grab a `game_id` from [`nhl_web_schedule`](../nhl/reference/nhl_api_web.md#nhl_web_schedule),
then pull the [`nhl_boxscore`](../nhl/reference/nhl_api_web.md#nhl_boxscore) and
[`nhl_web_pbp`](../nhl/reference/nhl_api_web.md#nhl_web_pbp) together — the box
for the line score, the pbp for the event stream.


```python
if sched is not None and sched.height:
    gid = int(sched['id'][0])
    r_box = safe(f'boxscore {gid}', lambda: nhl.nhl_boxscore(game_id=gid))
    r_pbp = safe(f'pbp {gid}', lambda: nhl.nhl_web_pbp(game_id=gid))
    print('players in box:', None if r_box is None else r_box.height,
          '| pbp events:', None if r_pbp is None else r_pbp.height)
else:
    print('no schedule rows to pick a game_id from')
```

### Recipe 2 — A team, its schedule & its roster 👥

Use the team tri-code (e.g. `FLA`) with
[`nhl_club_schedule_season`](../nhl/reference/nhl_api_web.md#nhl_club_schedule_season)
for the full slate and [`nhl_roster`](../nhl/reference/nhl_api_web.md#nhl_roster)
for the player list.


```python
TEAM = 'FLA'
club_sched = safe(f'{TEAM} schedule',
                  lambda: nhl.nhl_club_schedule_season(team=TEAM, season=SEASON))
roster = safe(f'{TEAM} roster', lambda: nhl.nhl_roster(team=TEAM, season=SEASON))
print('games:', None if club_sched is None else club_sched.height,
      '| roster size:', None if roster is None else roster.height)
if roster is not None and roster.height:
    cols = ['id', 'first_name_default', 'last_name_default',
            'sweater_number', 'position_code', 'shoots_catches']
    out = roster.select([c for c in cols if c in roster.columns]).head()
else:
    out = 'roster unavailable'
out
```

### Recipe 3 — A player's game log + the league leaderboard ⚡

Pair a single player's [`nhl_player_game_log`](../nhl/reference/nhl_api_web.md#nhl_player_game_log)
(game-by-game) with the season-wide
[`nhl_skater_leaders`](../nhl/reference/nhl_api_web.md#nhl_skater_leaders) board
to see where they rank. McDavid is `8478402`.


```python
gamelog = safe('McDavid game log',
               lambda: nhl.nhl_player_game_log(player_id=8478402, season=SEASON))
if gamelog is not None and gamelog.height:
    cols = ['game_date', 'opponent_abbrev', 'goals', 'assists', 'points', 'shots', 'toi']
    out = gamelog.select([c for c in cols if c in gamelog.columns]).head()
else:
    out = 'game log unavailable'
out
```


```python
board = safe('skater leaders', lambda: nhl.nhl_skater_leaders(season=SEASON))
if board is not None and board.height:
    cols = ['category', 'first_name_default', 'last_name_default', 'team_abbrev', 'value']
    out = board.select([c for c in cols if c in board.columns]).head(10)
else:
    out = 'leaders unavailable'
out
```

### Recipe 4 — An EDGE tracking leaderboard 🛰️

[`nhl_edge_skater_landing`](../nhl/reference/nhl_edge.md#nhl_edge_skater_landing)
returns a wide single-row frame of EDGE *leaders* — hardest shot, fastest
skater, and more. Here we surface who owned the hardest shot in 2023-24.


```python
el = safe('EDGE skater leaders', lambda: nhl.nhl_edge_skater_landing(season=SEASON))
if el is not None:
    keep = [c for c in el.columns if c.startswith('leaders_hardest_shot_player_')
            and ('first_name' in c or 'last_name' in c or 'team_abbrev' in c
                 or c.endswith('position'))]
    out = el.select(keep) if keep else el.head()
else:
    out = 'EDGE leaders unavailable'
out
```

## 🛟 ESPN NHL (`espn_nhl_*`) — the secondary path

Prefer the native feed above, but ESPN is a handy fallback and matches the
conventions used across every other league in the package. Team names are
`home_display_name` / `away_display_name`, scores come back as **strings** (cast
before arithmetic), and [`espn_nhl_pbp`](../nhl/reference/additional.md#espn_nhl_pbp)
returns a **dict** whose `plays` use raw ESPN dot-notation. ESPN game ids look
like `401675111`.

| Function | What it gives you |
|---|---|
| [`espn_nhl_teams`](../nhl/reference/additional.md#espn_nhl_teams) | ESPN team directory |
| [`espn_nhl_schedule`](../nhl/reference/additional.md#espn_nhl_schedule) | schedule for a date |
| [`espn_nhl_pbp`](../nhl/reference/additional.md#espn_nhl_pbp) | play-by-play (a dict) |
| [`espn_nhl_standings`](../nhl/reference/site.md#espn_nhl_standings) | standings |



```python
teams = safe('ESPN teams', lambda: nhl.espn_nhl_teams())
if teams is not None:
    cols = ['team_id', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name']
    out = teams.select([c for c in cols if c in teams.columns]).head()
else:
    out = 'ESPN teams unavailable'
out
```


```python
espn_pbp = safe(f'ESPN pbp {ESPN_GAME}', lambda: nhl.espn_nhl_pbp(game_id=ESPN_GAME))
if espn_pbp is not None and espn_pbp.get('plays'):
    plays = pl.DataFrame(espn_pbp['plays'], infer_schema_length=None)
    show = [c for c in ['period.number', 'clock.displayValue', 'text', 'type.text', 'scoringPlay']
            if c in plays.columns]
    print('ESPN plays:', plays.height)
    out = plays.select(show).head()
else:
    out = 'ESPN pbp unavailable'
out
```

## 📦 Parquet loaders (`load_nhl_*`)

When you want **multi-season** data fast and offline-friendly, the `load_nhl_*`
loaders read pre-built parquet data releases (fastRhockey-era schema) and return
polars frames. Pass `seasons=[...]`; add `return_as_pandas=True` for pandas.

| Function | Release |
|---|---|
| [`load_nhl_schedule`](../nhl/reference/loaders.md#load_nhl_schedule) | schedules |
| [`load_nhl_team_box`](../nhl/reference/additional.md#load_nhl_team_box) | team box |
| [`load_nhl_player_box`](../nhl/reference/additional.md#load_nhl_player_box) | player box |
| [`load_nhl_pbp`](../nhl/reference/loaders.md#load_nhl_pbp) | play-by-play |



```python
rel = safe('load schedule 2024', lambda: nhl.load_nhl_schedule(seasons=[2024]))
if rel is not None:
    print('release schedule shape:', rel.shape)
    cols = ['game_id', 'game_date', 'home_team_name', 'away_team_name', 'home_score', 'away_score']
    out = rel.select([c for c in cols if c in rel.columns]).head()
else:
    out = 'release loader unavailable'
out
```

## 🎉 Where to next

You just toured the **premium native NHL feed** end to end — schedule,
play-by-play, boxscores, standings, rosters, leaderboards, **EDGE tracking**,
the **stats-REST** and **Records** flat APIs — plus the ESPN fallback and the
parquet loaders. A few parting tips:

- Pass `return_as_pandas=True` on any native call for a pandas frame, or
  `return_parsed=False` for the raw JSON.
- Native game ids (`2023030417`) ≠ ESPN game ids (`401675111`) — same game,
  different namespaces. 🧭
- Full reference, by source:
  [NHL Web API](../nhl/reference/nhl_api_web.md) ·
  [NHL EDGE](../nhl/reference/nhl_edge.md) ·
  [Stats-REST](../nhl/reference/nhl_stats_rest.md) ·
  [Records](../nhl/reference/nhl_records.md) ·
  [loaders](../nhl/reference/loaders.md) ·
  [additional / ESPN](../nhl/reference/additional.md)
- Women's pro hockey? See the **PWHL** tutorial (`10_pwhl_intro.ipynb`).
- R user? The same surface lives in
  [fastRhockey](https://fastRhockey.sportsdataverse.org).

Now go build something great — and may your save percentage be ever high! 🥅
