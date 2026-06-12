---
title: CFB tutorial
sidebar_label: CFB
sidebar_position: 7
---

# 🏈 College football with `sportsdataverse-py`

Saturdays in autumn, condensed into tidy DataFrames. 🍂 In a few lines of
Python you're about to pull **a decade of play-by-play**, full **rosters**,
**schedules**, **team info**, plus **live** ESPN scoreboards, standings,
polls and recruiting boards — all as clean **polars** frames ready to model.

CFB has no single native premium API, so our **premium path** is two-pronged:

1. 🗄️ **Release loaders** (`load_cfb_*`) — pre-built, EPA/WPA-enriched
   datasets served straight from the
   [cfbfastR-data](https://github.com/sportsdataverse/cfbfastR-data) GitHub
   release. Fast, reliable, no key needed.
2. 📡 **ESPN families** (`espn_cfb_*`) — live scoreboards, team pages,
   standings, rankings, recruiting and per-play participants.

R user? Every verb here has a twin in
[cfbfastR](https://cfbfastR.sportsdataverse.org). Let's kick off! 🏈

## 🧰 The toolbox

Everything returns a tidy **polars** `DataFrame` by default — pass
`return_as_pandas=True` for pandas, or (on the `espn_cfb_*` wrappers)
`return_parsed=False` for the raw JSON. ⭐ marks the **premium** path.

| Function | What it gives you | Source |
|---|---|---|
| [`load_cfb_pbp`](../cfb/reference/loaders.md#load_cfb_pbp) | Full **play-by-play** with EPA/WPA, since 2003 | ⭐ release |
| [`load_cfb_rosters`](../cfb/reference/loaders.md#load_cfb_rosters) | Season **rosters** (bio, position, hometown) | ⭐ release |
| [`load_cfb_schedule`](../cfb/reference/loaders.md#load_cfb_schedule) | Season **schedule** + results + Elo | ⭐ release |
| [`load_cfb_team_info`](../cfb/reference/loaders.md#load_cfb_team_info) | **Team** metadata: conference, colors, venue | ⭐ release |
| [`espn_cfb_scoreboard`](../cfb/reference/site.md#espn_cfb_scoreboard) | Live + recent **scoreboard** for a date/week | ⭐ ESPN |
| [`espn_cfb_schedule`](../cfb/reference/additional.md#espn_cfb_schedule) | ESPN **schedule** frame for a date/week | ⭐ ESPN |
| [`espn_cfb_teams`](../cfb/reference/additional.md#espn_cfb_teams) | Every FBS/FCS **team** (grab `team_id`s) | ⭐ ESPN |
| [`espn_cfb_team_roster`](../cfb/reference/site.md#espn_cfb_team_roster) | One team's **roster** | ⭐ ESPN |
| [`espn_cfb_team_schedule`](../cfb/reference/site.md#espn_cfb_team_schedule) | One team's **schedule** | ⭐ ESPN |
| [`espn_cfb_standings`](../cfb/reference/site.md#espn_cfb_standings) | Conference / division **standings** | ⭐ ESPN |
| [`espn_cfb_rankings`](../cfb/reference/site.md#espn_cfb_rankings) | AP / Coaches / CFP **polls** | ⭐ ESPN |
| [`espn_cfb_leaders`](../cfb/reference/web.md#espn_cfb_leaders) | League **stat leaders** by category | ⭐ ESPN |
| [`espn_cfb_recruits`](../cfb/reference/core.md#espn_cfb_recruits) | Season **recruiting** class | ⭐ ESPN |
| [`espn_cfb_play_participants`](../cfb/reference/additional.md#espn_cfb_play_participants) | Per-play **athletes** (passer/rusher/tackler…) | ⭐ ESPN |
| [`CFBPlayProcess`](../cfb/reference/additional.md#CFBPlayProcess) | Full ESPN **PBP pipeline** (EPA/WPA + box) | ⭐ ESPN |
| [`most_recent_cfb_season`](../cfb/reference/additional.md#most_recent_cfb_season) | The current season year helper | helper |


## 🔌 Setup

```sh
pip install sportsdataverse
```

No API key required. The `load_cfb_*` loaders read public parquet from the
cfbfastR-data release, and the `espn_cfb_*` wrappers hit ESPN's public
endpoints.


```python
import polars as pl
import sportsdataverse as sdv
from sportsdataverse.cfb import most_recent_cfb_season

SEASON = most_recent_cfb_season()
print('most recent CFB season:', SEASON)
```

ESPN's live endpoints are seasonal and occasionally rate-limited, so a tiny
`safe()` helper runs the riskier calls defensively — you get the frame when
the feed is up, and a friendly one-liner when it isn't (never a scary
traceback). The release loaders are reliable, so we call those directly. 🛟


```python
def safe(label, thunk):
    try:
        out = thunk()
        print(f'✅ {label}')
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f'⏭️  {label}: unavailable right now ({type(e).__name__})')
        return None
```

## 🗄️ Premium loaders: a whole season in one call

The `load_cfb_*` family is the fastest way to get **clean, complete**
season data. Each takes a `seasons=` int or list (≥ 2003) and returns one
tidy frame. Let's start with the schedule — one row per game, with final
scores, conference flags, and pre/post-game Elo ratings baked in.

| Function | Grain | Highlights |
|---|---|---|
| [`load_cfb_schedule`](../cfb/reference/loaders.md#load_cfb_schedule) | one row / game | scores, Elo, neutral-site & conference flags |



```python
schedule = sdv.cfb.load_cfb_schedule(seasons=[2023])
print('schedule shape:', schedule.shape)
schedule.select([
    'game_id', 'week', 'home_team', 'away_team',
    'home_points', 'away_points', 'home_conference', 'neutral_site',
]).head()
```

## 👥 Premium loaders: rosters

[`load_cfb_rosters`](../cfb/reference/loaders.md#load_cfb_rosters) gives you
every listed player for a season — name, position, jersey, physicals and
hometown. Perfect for joining onto play-by-play or building depth tables.


```python
rosters = sdv.cfb.load_cfb_rosters(seasons=[2023])
print('rosters shape:', rosters.shape)
rosters.select([
    'athlete_id', 'first_name', 'last_name', 'team',
    'position', 'jersey', 'home_state',
]).head()
```

## 🏟️ Premium loaders: team info

[`load_cfb_team_info`](../cfb/reference/loaders.md#load_cfb_team_info)
carries the reference metadata you'll want to label every chart: school
name, conference, classification (FBS/FCS), team colors, and venue.


```python
team_info = sdv.cfb.load_cfb_team_info(seasons=[2023])
print('team_info shape:', team_info.shape)
team_info.select([
    'team_id', 'school', 'conference', 'classification',
    'venue_name', 'city', 'state', 'dome',
]).head()
```

## 🎬 Premium loaders: play-by-play with EPA

The crown jewel. [`load_cfb_pbp`](../cfb/reference/loaders.md#load_cfb_pbp)
returns **every play** of a season with hundreds of engineered columns —
down & distance, win probability, and **Expected Points Added (EPA)**
already computed. (It's a big pull, so we grab a single season and peek.) 📊


```python
pbp = sdv.cfb.load_cfb_pbp(seasons=[2023])
print('pbp shape:', pbp.shape)
cols = ['game_id', 'pos_team', 'down', 'distance', 'play_type', 'epa', 'wpa']
have = [c for c in cols if c in pbp.columns]
pbp.select(have).head()
```

## 📡 Live from ESPN: the scoreboard

When you need *today's* slate (or a specific date), the ESPN wrappers shine.
[`espn_cfb_scoreboard`](../cfb/reference/site.md#espn_cfb_scoreboard) takes a
`dates=YYYYMMDD` (or season year) and returns the games on the board. We wrap
it in `safe()` since live endpoints can be quiet in the offseason.


```python
board = safe(
    'ESPN scoreboard',
    lambda: sdv.cfb.espn_cfb_scoreboard(dates=20231125),  # rivalry Saturday
)
if board is not None and getattr(board, 'height', 0):
    keep = [c for c in board.columns
            if c in ('game_id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'away_team_abbreviation')]
    out = board.select(keep).head() if keep else board.head()
else:
    out = 'no games on the board for that date'
out
```

## 🏫 Live from ESPN: teams (and their `team_id`s)

[`espn_cfb_teams`](../cfb/reference/additional.md#espn_cfb_teams) lists every
team in a division (`groups=80` FBS, `groups=81` FCS). The `team_id` column
is the key you feed into every team-scoped ESPN call below.


```python
teams = safe('ESPN teams', sdv.cfb.espn_cfb_teams)
if teams is not None and teams.height:
    cols = [c for c in ('team_id', 'team_location', 'team_name',
                        'team_abbreviation') if c in teams.columns]
    out = teams.select(cols).head(8)
else:
    out = 'teams unavailable right now'
out
```

## 🍳 Cookbook: common CFB tasks

Now the fun part — real questions, answered with a few expressions. The
loaders are reliable so these recipes lean on them, reaching for ESPN where
it adds something live.

### Recipe 1 — Highest-scoring games of the season 🔥

Straight from the loaded schedule: add the two scores and sort. No casting
needed — the release frame already stores points as integers.


```python
(schedule
    .with_columns(
        (pl.col('home_points') + pl.col('away_points')).alias('total_points')
    )
    .sort('total_points', descending=True)
    .select(['week', 'home_team', 'away_team',
             'home_points', 'away_points', 'total_points'])
    .head(10))
```

### Recipe 2 — Team offensive EPA/play leaderboard 📈

This is what premium EPA-tagged play-by-play unlocks. Filter to real
scrimmage plays, group by the offense, and average the EPA per play — a
clean efficiency ranking in five lines.


```python
epa_cols = {'pos_team', 'epa', 'play'}
if epa_cols.issubset(pbp.columns):
    leaderboard = (
        pbp
        .filter(pl.col('play') & pl.col('epa').is_not_null())
        .group_by('pos_team')
        .agg(
            pl.len().alias('plays'),
            pl.col('epa').mean().round(3).alias('epa_per_play'),
        )
        .filter(pl.col('plays') >= 500)
        .sort('epa_per_play', descending=True)
        .head(15)
    )
    out = leaderboard
else:
    out = 'expected EPA columns not present in this pbp build'
out
```

### Recipe 3 — A team's roster, sorted by position 🧩

Join the loaded roster against `team_info` to resolve a school name to its
players, then count the depth at each position group.


```python
team_name = 'Michigan'
squad = (
    rosters
    .filter(pl.col('team') == team_name)
    .select(['first_name', 'last_name', 'position', 'jersey',
             'height', 'weight', 'home_state'])
)
if squad.height:
    depth = (squad.group_by('position')
                  .agg(pl.len().alias('players'))
                  .sort('players', descending=True))
    print(f'{team_name}: {squad.height} players')
    out = depth.head(10)
else:
    out = f'no roster rows for {team_name} (try another school string)'
out
```

### Recipe 4 — Who was on the field? Per-play participants 🕵️

[`espn_cfb_play_participants`](../cfb/reference/additional.md#espn_cfb_play_participants)
resolves the athletes involved in each play (passer, rusher, receiver,
tackler…) straight from ESPN's authoritative `participants[]` array — far
more reliable than regex-parsing the play text. Set `resolve_missing=False`
to skip the per-athlete `$ref` fan-out and keep it snappy.


```python
gid = 401628334  # 2024 CFP National Championship
participants = safe(
    f'play participants {gid}',
    lambda: sdv.cfb.espn_cfb_play_participants(
        game_id=gid, resolve_missing=False,
    ),
)
if participants is not None and getattr(participants, 'height', 0):
    name_cols = [c for c in participants.columns if c.endswith('_player_name')]
    show = ['play_id'] + name_cols[:4] if 'play_id' in participants.columns else name_cols[:5]
    out = participants.select([c for c in show if c in participants.columns]).head()
else:
    out = 'participants feed quiet right now (offseason / rate limit)'
out
```

## 🗞️ Live tour: standings, polls, leaders & recruits

A quick lap through the rest of the live ESPN surface. Each is wrapped in
`safe()` so the page renders cleanly whatever the feed is doing today.

| Function | Use it for |
|---|---|
| [`espn_cfb_standings`](../cfb/reference/site.md#espn_cfb_standings) | conference / division standings |
| [`espn_cfb_rankings`](../cfb/reference/site.md#espn_cfb_rankings) | AP / Coaches / CFP polls |
| [`espn_cfb_leaders`](../cfb/reference/web.md#espn_cfb_leaders) | league stat leaders by category |
| [`espn_cfb_recruits`](../cfb/reference/core.md#espn_cfb_recruits) | a season's recruiting class |



```python
standings = safe('ESPN standings', sdv.cfb.espn_cfb_standings)
rankings = safe('ESPN rankings (polls)', sdv.cfb.espn_cfb_rankings)
(standings.head()
 if standings is not None and getattr(standings, 'height', 0)
 else (rankings.head()
       if rankings is not None and getattr(rankings, 'height', 0)
       else 'standings & rankings unavailable right now'))
```


```python
leaders = safe(
    'ESPN passing leaders',
    lambda: sdv.cfb.espn_cfb_leaders(category='passingYards', season=2023, limit=15),
)
recruits = safe(
    'ESPN recruiting class',
    lambda: sdv.cfb.espn_cfb_recruits(season=2024, limit=25),
)
(leaders.head()
 if leaders is not None and getattr(leaders, 'height', 0)
 else (recruits.head()
       if recruits is not None and getattr(recruits, 'height', 0)
       else 'leaders & recruits unavailable right now'))
```

## 🧪 Bonus: process one game from scratch with `CFBPlayProcess`

Want EPA/WPA on a single *live* game without loading a whole season?
[`CFBPlayProcess`](../cfb/reference/additional.md#CFBPlayProcess) drives the
full ESPN pipeline: `.espn_cfb_pbp()` fetches the raw summary, then
`.run_processing_pipeline()` returns a dict whose `plays` key is the
fully-featured play list (alongside an advanced box score and metadata).


```python
from sportsdataverse.cfb import CFBPlayProcess

def process_game(game_id):
    game = CFBPlayProcess(gameId=game_id)
    game.espn_cfb_pbp()
    processed = game.run_processing_pipeline()
    return pl.DataFrame(processed['plays'], infer_schema_length=None)

plays = safe('CFBPlayProcess 401628334', lambda: process_game(401628334))
if plays is not None and plays.height:
    cols = [c for c in ('period', 'pos_team', 'down', 'distance',
                        'play_type', 'EPA') if c in plays.columns]
    out = plays.select(cols).head()
else:
    out = 'live PBP pipeline quiet right now'
out
```

## 🎉 Where to next

- 🗄️ **Loaders** are your premium fast-path — full reference on the
  [Loaders](../cfb/reference/loaders.md) page (`load_cfb_pbp`,
  `load_cfb_rosters`, `load_cfb_schedule`, `load_cfb_team_info`).
- 📡 **ESPN families** live across the
  [Site](../cfb/reference/site.md), [Web](../cfb/reference/web.md),
  [Core](../cfb/reference/core.md) and
  [Additional](../cfb/reference/additional.md) reference pages.
- 🐼 Pass `return_as_pandas=True` for pandas, or `return_parsed=False` on the
  `espn_cfb_*` wrappers for the raw JSON.
- 🟥 R user? The same verbs live in
  [cfbfastR](https://cfbfastR.sportsdataverse.org).
- Part of the [SportsDataverse](https://py.sportsdataverse.org/docs/ecosystem)
  ecosystem.

Now go chart some chunk plays — and may your EPA always be positive! 📈🏈
