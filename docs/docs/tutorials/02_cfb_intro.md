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
| [`load_cfb_schedule`](../cfb/reference/loaders.md#load_cfb_schedule) | Season **schedule** + results | ⭐ release |
| [`load_cfb_team_info`](../cfb/reference/loaders.md#load_cfb_team_info) | **Team** metadata: conference, colors, venue | ⭐ release |
| [`load_cfb_ratings`](../cfb/reference/loaders.md#load_cfb_ratings) | Opponent-adjusted **EPA ratings** per team-season | ⭐ release |
| [`load_cfb_betting_lines`](../cfb/reference/additional.md#load_cfb_betting_lines) | Historical **betting market** lines (spread/total/ML) | ⭐ release |
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

    most recent CFB season: 2026


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
scores, conference flags, and playoff fields baked in.

| Function | Grain | Highlights |
|---|---|---|
| [`load_cfb_schedule`](../cfb/reference/loaders.md#load_cfb_schedule) | one row / game | scores, neutral-site & conference flags, playoff rounds |



```python
schedule = sdv.cfb.load_cfb_schedule(seasons=[2023])
print('schedule shape:', schedule.shape)
schedule.select([
    'game_id', 'week', 'home_team', 'away_team',
    'home_points', 'away_points', 'home_conference', 'neutral_site',
]).head()
```

    schedule shape: (3734, 43)





    shape: (5, 8)
    ┌───────────┬──────┬─────────────┬─────────────┬────────────┬────────────┬────────────┬────────────┐
    │ game_id   ┆ week ┆ home_team   ┆ away_team   ┆ home_point ┆ away_point ┆ home_confe ┆ neutral_si │
    │ ---       ┆ ---  ┆ ---         ┆ ---         ┆ s          ┆ s          ┆ rence      ┆ te         │
    │ i64       ┆ i64  ┆ str         ┆ str         ┆ ---        ┆ ---        ┆ ---        ┆ ---        │
    │           ┆      ┆             ┆             ┆ i64        ┆ i64        ┆ str        ┆ bool       │
    ╞═══════════╪══════╪═════════════╪═════════════╪════════════╪════════════╪════════════╪════════════╡
    │ 401520145 ┆ 1    ┆ Jacksonvill ┆ UTEP        ┆ 17         ┆ 14         ┆ Conference ┆ false      │
    │           ┆      ┆ e State     ┆             ┆            ┆            ┆ USA        ┆            │
    │ 401520146 ┆ 1    ┆ Louisiana   ┆ Florida Int ┆ 22         ┆ 17         ┆ Conference ┆ false      │
    │           ┆      ┆ Tech        ┆ ernational  ┆            ┆            ┆ USA        ┆            │
    │ 401520147 ┆ 1    ┆ Vanderbilt  ┆ Hawai'i     ┆ 35         ┆ 28         ┆ SEC        ┆ false      │
    │ 401520148 ┆ 1    ┆ Minnesota   ┆ Nebraska    ┆ 13         ┆ 10         ┆ Big Ten    ┆ false      │
    │ 401520149 ┆ 1    ┆ Alabama     ┆ Middle      ┆ 56         ┆ 7          ┆ SEC        ┆ false      │
    │           ┆      ┆             ┆ Tennessee   ┆            ┆            ┆            ┆            │
    └───────────┴──────┴─────────────┴─────────────┴────────────┴────────────┴────────────┴────────────┘



## 👥 Premium loaders: rosters

[`load_cfb_rosters`](../cfb/reference/loaders.md#load_cfb_rosters) gives you
every listed player for a season — name, position, jersey, physicals and
hometown. Perfect for joining onto play-by-play or building depth tables.


```python
rosters = sdv.cfb.load_cfb_rosters(seasons=[2023])
print('rosters shape:', rosters.shape)
rosters.select([
    'athlete_id', 'first_name', 'last_name', 'team_location',
    'position', 'jersey', 'cfbd_home_state',
]).head()
```

    rosters shape: (26375, 85)





    shape: (5, 7)
    ┌────────────┬────────────┬───────────┬───────────────┬───────────────┬────────┬─────────────────┐
    │ athlete_id ┆ first_name ┆ last_name ┆ team_location ┆ position      ┆ jersey ┆ cfbd_home_state │
    │ ---        ┆ ---        ┆ ---       ┆ ---           ┆ ---           ┆ ---    ┆ ---             │
    │ i64        ┆ str        ┆ str       ┆ str           ┆ str           ┆ str    ┆ str             │
    ╞════════════╪════════════╪═══════════╪═══════════════╪═══════════════╪════════╪═════════════════╡
    │ 4360930    ┆ Jyaire     ┆ Shorter   ┆ Auburn        ┆ Wide Receiver ┆ 4      ┆ TX              │
    │ 4362310    ┆ Shane      ┆ Hooks     ┆ Auburn        ┆ Wide Receiver ┆ 3      ┆ FL              │
    │ 4373489    ┆ Kassidy    ┆ Woods     ┆ Auburn        ┆ Wide Receiver ┆ 30     ┆ TX              │
    │ 4373959    ┆ Nick       ┆ Mardner   ┆ Auburn        ┆ Wide Receiver ┆ 8      ┆ ON              │
    │ 4431277    ┆ Koy        ┆ Moore     ┆ Auburn        ┆ Wide Receiver ┆ 5      ┆ LA              │
    └────────────┴────────────┴───────────┴───────────────┴───────────────┴────────┴─────────────────┘



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

    team_info shape: (672, 43)





    shape: (5, 8)
    ┌─────────┬───────────────┬───────────────┬──────────────┬──────────────┬──────────┬───────┬───────┐
    │ team_id ┆ school        ┆ conference    ┆ classificati ┆ venue_name   ┆ city     ┆ state ┆ dome  │
    │ ---     ┆ ---           ┆ ---           ┆ on           ┆ ---          ┆ ---      ┆ ---   ┆ ---   │
    │ i64     ┆ str           ┆ str           ┆ ---          ┆ str          ┆ str      ┆ str   ┆ bool  │
    │         ┆               ┆               ┆ str          ┆              ┆          ┆       ┆       │
    ╞═════════╪═══════════════╪═══════════════╪══════════════╪══════════════╪══════════╪═══════╪═══════╡
    │ 2000    ┆ Abilene       ┆ UAC           ┆ fcs          ┆ Wildcat      ┆ Abilene  ┆ TX    ┆ false │
    │         ┆ Christian     ┆               ┆              ┆ Stadium (TX) ┆          ┆       ┆       │
    │ 2001    ┆ Adams State   ┆ Rocky         ┆ ii           ┆ Rex Stadium  ┆ Alamosa  ┆ CO    ┆ false │
    │         ┆               ┆ Mountain      ┆              ┆              ┆          ┆       ┆       │
    │ 2003    ┆ Adrian        ┆ Michigan      ┆ iii          ┆ Docking      ┆ Adrian   ┆ MI    ┆ false │
    │         ┆               ┆               ┆              ┆ Stadium      ┆          ┆       ┆       │
    │ 2005    ┆ Air Force     ┆ Mountain West ┆ fbs          ┆ Falcon       ┆ Colorado ┆ CO    ┆ false │
    │         ┆               ┆               ┆              ┆ Stadium      ┆ Springs  ┆       ┆       │
    │ 2006    ┆ Akron         ┆ Mid-American  ┆ fbs          ┆ InfoCision   ┆ Akron    ┆ OH    ┆ false │
    │         ┆               ┆               ┆              ┆ Stadium      ┆          ┆       ┆       │
    └─────────┴───────────────┴───────────────┴──────────────┴──────────────┴──────────┴───────┴───────┘



## 🎬 Premium loaders: play-by-play with EPA

The crown jewel. [`load_cfb_pbp`](../cfb/reference/loaders.md#load_cfb_pbp)
returns **every play** of a season with hundreds of engineered columns —
down & distance, win probability, and **Expected Points Added (EPA)**
already computed. (It's a big pull, so we grab a single season and peek.) 📊


```python
# The release serves PBP for whichever seasons are currently published.
# Try a few recent-ish seasons and keep the first one that comes back full,
# so the EPA recipes below always have real plays to chew on.
pbp = pl.DataFrame()
for yr in (2023, 2022, 2021, 2020):
    cand = safe(f'load_cfb_pbp {yr}', lambda yr=yr: sdv.cfb.load_cfb_pbp(seasons=[yr]))
    if cand is not None and cand.width > 0 and cand.height > 0:
        pbp, PBP_SEASON = cand, yr
        break
else:
    PBP_SEASON = None

print('pbp season:', PBP_SEASON, '| pbp shape:', pbp.shape)
cols = ['game_id', 'start.pos_team.name', 'down', 'distance',
        'play_type', 'EPA', 'wpa']
have = [c for c in cols if c in pbp.columns]
pbp.select(have).head() if have else 'pbp not published for these seasons right now'
```

    ✅ load_cfb_pbp 2023
    pbp season: 2023 | pbp shape: (153945, 506)





    shape: (5, 6)
    ┌───────────┬─────────────────────┬──────┬──────────┬───────────┬───────────┐
    │ game_id   ┆ start.pos_team.name ┆ down ┆ distance ┆ EPA       ┆ wpa       │
    │ ---       ┆ ---                 ┆ ---  ┆ ---      ┆ ---       ┆ ---       │
    │ i64       ┆ str                 ┆ i64  ┆ i64      ┆ f64       ┆ f64       │
    ╞═══════════╪═════════════════════╪══════╪══════════╪═══════════╪═══════════╡
    │ 401523986 ┆ San José State      ┆ 1    ┆ 10       ┆ 0.0       ┆ 0.0       │
    │ 401523986 ┆ San José State      ┆ 1    ┆ 10       ┆ -0.687152 ┆ -0.002614 │
    │ 401523986 ┆ San José State      ┆ 2    ┆ 10       ┆ 2.361556  ┆ 0.015136  │
    │ 401523986 ┆ San José State      ┆ 1    ┆ 10       ┆ 0.106075  ┆ 0.00069   │
    │ 401523986 ┆ San José State      ┆ 2    ┆ 5        ┆ -1.156004 ┆ -0.002393 │
    └───────────┴─────────────────────┴──────┴──────────┴───────────┴───────────┘



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

    ✅ ESPN scoreboard





    shape: (5, 4)
    ┌───────────┬─────────────────────────────────┬────────────┬─────────────────────────┐
    │ game_id   ┆ name                            ┆ short_name ┆ status_type_description │
    │ ---       ┆ ---                             ┆ ---        ┆ ---                     │
    │ str       ┆ str                             ┆ str        ┆ str                     │
    ╞═══════════╪═════════════════════════════════╪════════════╪═════════════════════════╡
    │ 401520430 ┆ Georgia Bulldogs at Georgia Te… ┆ UGA @ GT   ┆ Final                   │
    │ 401520434 ┆ Ohio State Buckeyes at Michiga… ┆ OSU @ MICH ┆ Final                   │
    │ 401524068 ┆ Washington State Cougars at Wa… ┆ WSU @ WASH ┆ Final                   │
    │ 401520429 ┆ Florida State Seminoles at Flo… ┆ FSU @ FLA  ┆ Final                   │
    │ 401520427 ┆ Alabama Crimson Tide at Auburn… ┆ ALA @ AUB  ┆ Final                   │
    └───────────┴─────────────────────────────────┴────────────┴─────────────────────────┘



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

    ✅ ESPN teams





    shape: (8, 4)
    ┌─────────┬───────────────────┬──────────────┬───────────────────┐
    │ team_id ┆ team_location     ┆ team_name    ┆ team_abbreviation │
    │ ---     ┆ ---               ┆ ---          ┆ ---               │
    │ str     ┆ str               ┆ str          ┆ str               │
    ╞═════════╪═══════════════════╪══════════════╪═══════════════════╡
    │ 2000    ┆ Abilene Christian ┆ Wildcats     ┆ ACU               │
    │ 2001    ┆ Adams State       ┆ Grizzlies    ┆ ADSU              │
    │ 2003    ┆ Adrian            ┆ Bulldogs     ┆ ADR               │
    │ 2005    ┆ Air Force         ┆ Falcons      ┆ AF                │
    │ 2006    ┆ Akron             ┆ Zips         ┆ AKR               │
    │ 2010    ┆ Alabama A&M       ┆ Bulldogs     ┆ AAMU              │
    │ 333     ┆ Alabama           ┆ Crimson Tide ┆ ALA               │
    │ 2011    ┆ Alabama State     ┆ Hornets      ┆ ALST              │
    └─────────┴───────────────────┴──────────────┴───────────────────┘



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




    shape: (10, 6)
    ┌──────┬──────────────────┬────────────────────┬─────────────┬─────────────┬──────────────┐
    │ week ┆ home_team        ┆ away_team          ┆ home_points ┆ away_points ┆ total_points │
    │ ---  ┆ ---              ┆ ---                ┆ ---         ┆ ---         ┆ ---          │
    │ i64  ┆ str              ┆ str                ┆ i64         ┆ i64         ┆ i64          │
    ╞══════╪══════════════════╪════════════════════╪═════════════╪═════════════╪══════════════╡
    │ 9    ┆ Bowdoin          ┆ Trinity (CT)       ┆ null        ┆ null        ┆ null         │
    │ 9    ┆ Bates            ┆ Williams           ┆ null        ┆ null        ┆ null         │
    │ 9    ┆ Colby            ┆ Middlebury         ┆ null        ┆ null        ┆ null         │
    │ 11   ┆ Worcester State  ┆ Framingham State   ┆ null        ┆ null        ┆ null         │
    │ 10   ┆ Defiance College ┆ Rose Hulman        ┆ 54          ┆ 78          ┆ 132          │
    │ 10   ┆ Muskingum        ┆ Wilmington (OH)    ┆ 64          ┆ 63          ┆ 127          │
    │ 2    ┆ Coast Guard      ┆ Anna Maria College ┆ 93          ┆ 24          ┆ 117          │
    │ 13   ┆ Oklahoma         ┆ TCU                ┆ 69          ┆ 45          ┆ 114          │
    │ 3    ┆ Texas State      ┆ Jackson State      ┆ 77          ┆ 34          ┆ 111          │
    │ 10   ┆ Cornell (IA)     ┆ Illinois College   ┆ 34          ┆ 76          ┆ 110          │
    └──────┴──────────────────┴────────────────────┴─────────────┴─────────────┴──────────────┘



### Recipe 2 — Team offensive EPA/play leaderboard 📈

This is what premium EPA-tagged play-by-play unlocks. Filter to real
scrimmage plays, group by the offense, and average the EPA per play — a
clean efficiency ranking in five lines.


```python
team_col = 'start.pos_team.name'  # human-readable offense on each play
epa_cols = {team_col, 'EPA', 'play'}
if epa_cols.issubset(pbp.columns):
    leaderboard = (
        pbp
        .filter(pl.col('play') & pl.col('EPA').is_not_null())
        .group_by(team_col)
        .agg(
            pl.len().alias('plays'),
            pl.col('EPA').mean().round(3).alias('epa_per_play'),
        )
        .filter(pl.col('plays') >= 500)
        .sort('epa_per_play', descending=True)
        .rename({team_col: 'offense'})
        .head(15)
    )
    out = leaderboard
else:
    out = 'expected EPA columns not present in this pbp build'
out
```




    shape: (15, 3)
    ┌──────────────┬───────┬──────────────┐
    │ offense      ┆ plays ┆ epa_per_play │
    │ ---          ┆ ---   ┆ ---          │
    │ str          ┆ u32   ┆ f64          │
    ╞══════════════╪═══════╪══════════════╡
    │ LSU          ┆ 946   ┆ 0.377        │
    │ Oregon       ┆ 1052  ┆ 0.351        │
    │ Georgia      ┆ 1064  ┆ 0.272        │
    │ USC          ┆ 958   ┆ 0.216        │
    │ Liberty      ┆ 1080  ┆ 0.21         │
    │ …            ┆ …     ┆ …            │
    │ Missouri     ┆ 1004  ┆ 0.159        │
    │ Alabama      ┆ 1030  ┆ 0.158        │
    │ Oregon State ┆ 933   ┆ 0.158        │
    │ North Texas  ┆ 1026  ┆ 0.154        │
    │ Notre Dame   ┆ 917   ┆ 0.153        │
    └──────────────┴───────┴──────────────┘



### Recipe 3 — A team's roster, sorted by position 🧩

Join the loaded roster against `team_info` to resolve a school name to its
players, then count the depth at each position group.


```python
team_name = 'Michigan'
squad = (
    rosters
    .filter(pl.col('team_location') == team_name)
    .select(['first_name', 'last_name', 'position', 'jersey',
             'height', 'weight', 'cfbd_home_state'])
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

    Michigan: 143 players





    shape: (10, 2)
    ┌───────────────────┬─────────┐
    │ position          ┆ players │
    │ ---               ┆ ---     │
    │ str               ┆ u32     │
    ╞═══════════════════╪═════════╡
    │ Defensive Back    ┆ 26      │
    │ Offensive Lineman ┆ 21      │
    │ Linebacker        ┆ 18      │
    │ Wide Receiver     ┆ 18      │
    │ Defensive End     ┆ 12      │
    │ Defensive Lineman ┆ 12      │
    │ Running Back      ┆ 11      │
    │ Tight End         ┆ 10      │
    │ Place Kicker      ┆ 6       │
    │ Quarterback       ┆ 6       │
    └───────────────────┴─────────┘



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

    ✅ play participants 401628334





    shape: (5, 5)
    ┌───────────────────┬───────────────────┬───────────────────┬───────────────────┬──────────────────┐
    │ play_id           ┆ kicker_player_nam ┆ returner_player_n ┆ passer_player_nam ┆ receiver_player_ │
    │ ---               ┆ e                 ┆ ame               ┆ e                 ┆ name             │
    │ i64               ┆ ---               ┆ ---               ┆ ---               ┆ ---              │
    │                   ┆ str               ┆ str               ┆ str               ┆ str              │
    ╞═══════════════════╪═══════════════════╪═══════════════════╪═══════════════════╪══════════════════╡
    │ 40162833410184990 ┆ Michael Lantz     ┆ Zavion Thomas     ┆ null              ┆ null             │
    │ 2                 ┆                   ┆                   ┆                   ┆                  │
    │ 40162833410185440 ┆ null              ┆ null              ┆ Garrett Nussmeier ┆ Kyren Lacy       │
    │ 1                 ┆                   ┆                   ┆                   ┆                  │
    │ 40162833410185750 ┆ null              ┆ null              ┆ Garrett Nussmeier ┆ Kyren Lacy       │
    │ 1                 ┆                   ┆                   ┆                   ┆                  │
    │ 40162833410185960 ┆ null              ┆ null              ┆ null              ┆ null             │
    │ 1                 ┆                   ┆                   ┆                   ┆                  │
    │ 40162833410186700 ┆ null              ┆ null              ┆ Garrett Nussmeier ┆ CJ Daniels       │
    │ 1                 ┆                   ┆                   ┆                   ┆                  │
    └───────────────────┴───────────────────┴───────────────────┴───────────────────┴──────────────────┘



### Recipe 5 — Build a standings table from the schedule 🏆

No standings endpoint needed: stack each team's home and away results, count wins and losses, and you've got a win-percentage table for *any* season the loader serves.


```python
completed = schedule.filter(pl.col('completed') == True)
home = completed.select(
    pl.col('home_team').alias('team'),
    (pl.col('home_points') > pl.col('away_points')).alias('win'),
)
away = completed.select(
    pl.col('away_team').alias('team'),
    (pl.col('away_points') > pl.col('home_points')).alias('win'),
)
standings_tbl = (
    pl.concat([home, away])
    .group_by('team')
    .agg(
        pl.col('win').sum().alias('wins'),
        (~pl.col('win')).sum().alias('losses'),
    )
    .with_columns(
        (pl.col('wins') / (pl.col('wins') + pl.col('losses')))
        .round(3).alias('win_pct')
    )
    .sort(['wins', 'win_pct'], descending=True)
)
standings_tbl.head(10)
```




    shape: (10, 4)
    ┌──────────────────────────┬──────┬────────┬─────────┐
    │ team                     ┆ wins ┆ losses ┆ win_pct │
    │ ---                      ┆ ---  ┆ ---    ┆ ---     │
    │ str                      ┆ u32  ┆ u32    ┆ f64     │
    ╞══════════════════════════╪══════╪════════╪═════════╡
    │ Michigan                 ┆ 15   ┆ 0      ┆ 1.0     │
    │ Harding                  ┆ 15   ┆ 0      ┆ 1.0     │
    │ South Dakota State       ┆ 15   ┆ 0      ┆ 1.0     │
    │ SUNY Cortland            ┆ 14   ┆ 1      ┆ 0.933   │
    │ Colorado School of Mines ┆ 14   ┆ 1      ┆ 0.933   │
    │ North Central College    ┆ 14   ┆ 1      ┆ 0.933   │
    │ Washington               ┆ 14   ┆ 1      ┆ 0.933   │
    │ Liberty                  ┆ 13   ┆ 1      ┆ 0.929   │
    │ Wartburg                 ┆ 13   ┆ 1      ┆ 0.929   │
    │ Georgia                  ┆ 13   ┆ 1      ┆ 0.929   │
    └──────────────────────────┴──────┴────────┴─────────┘



### Recipe 6 — End-of-season power ratings ⚡

[`load_cfb_ratings`](../cfb/reference/loaders.md#load_cfb_ratings) ships opponent-adjusted EPA ratings for every team-season: `adj_net` is adjusted offensive EPA per play minus adjusted defensive EPA per play, with ranks alongside. Join the schedule's team names onto its `team_id` for a ready-to-rank power table — no model to fit.


```python
ratings = sdv.cfb.load_cfb_ratings(seasons=[2023])
names = pl.concat([
    schedule.select(pl.col('home_id').alias('team_id'), pl.col('home_team').alias('team')),
    schedule.select(pl.col('away_id').alias('team_id'), pl.col('away_team').alias('team')),
]).unique(subset=['team_id'])
assert ratings.schema['team_id'] == names.schema['team_id']  # join keys share one dtype
power = (
    ratings
    .join(names, on='team_id', how='left')
    .sort('net_rank')
    .select(['net_rank', 'team', 'adj_net', 'adj_off_epa', 'adj_def_epa', 'games'])
)
power.head(15)
```




    shape: (15, 6)
    ┌──────────┬───────────────┬──────────┬─────────────┬─────────────┬───────┐
    │ net_rank ┆ team          ┆ adj_net  ┆ adj_off_epa ┆ adj_def_epa ┆ games │
    │ ---      ┆ ---           ┆ ---      ┆ ---         ┆ ---         ┆ ---   │
    │ i64      ┆ str           ┆ f64      ┆ f64         ┆ f64         ┆ i64   │
    ╞══════════╪═══════════════╪══════════╪═════════════╪═════════════╪═══════╡
    │ 1        ┆ Oregon        ┆ 0.486063 ┆ 0.328165    ┆ -0.157897   ┆ 13    │
    │ 2        ┆ Ohio State    ┆ 0.46467  ┆ 0.182749    ┆ -0.281921   ┆ 12    │
    │ 3        ┆ Michigan      ┆ 0.431085 ┆ 0.205671    ┆ -0.225414   ┆ 15    │
    │ 4        ┆ Georgia       ┆ 0.420475 ┆ 0.26391     ┆ -0.156565   ┆ 13    │
    │ 5        ┆ Oklahoma      ┆ 0.401535 ┆ 0.22448     ┆ -0.177056   ┆ 13    │
    │ …        ┆ …             ┆ …        ┆ …           ┆ …           ┆ …     │
    │ 11       ┆ Missouri      ┆ 0.293728 ┆ 0.13169     ┆ -0.162038   ┆ 12    │
    │ 12       ┆ Florida State ┆ 0.274252 ┆ 0.094095    ┆ -0.180156   ┆ 13    │
    │ 13       ┆ Tennessee     ┆ 0.268064 ┆ 0.123445    ┆ -0.14462    ┆ 12    │
    │ 14       ┆ Kansas State  ┆ 0.264411 ┆ 0.155379    ┆ -0.109032   ┆ 12    │
    │ 15       ┆ Washington    ┆ 0.248367 ┆ 0.208181    ┆ -0.040186   ┆ 15    │
    └──────────┴───────────────┴──────────┴─────────────┴─────────────┴───────┘



### Recipe 7 — One team's full game log 📜

Filter the schedule to a single program, then flip the home/away columns so every row reads from *that team's* perspective — opponent, points for, points against, and the margin. Swap `team` to scout anyone.


```python
team = 'Michigan'
gamelog = (
    schedule
    .filter((pl.col('home_team') == team) | (pl.col('away_team') == team))
    .unique(subset=['game_id'])
    .with_columns(
        pl.when(pl.col('home_team') == team)
          .then(pl.col('away_team')).otherwise(pl.col('home_team'))
          .alias('opponent'),
        pl.when(pl.col('home_team') == team)
          .then(pl.col('home_points')).otherwise(pl.col('away_points'))
          .alias('pts_for'),
        pl.when(pl.col('home_team') == team)
          .then(pl.col('away_points')).otherwise(pl.col('home_points'))
          .alias('pts_against'),
    )
    .with_columns(
        (pl.col('pts_for') - pl.col('pts_against')).alias('margin')
    )
    .select(['week', 'opponent', 'pts_for', 'pts_against', 'margin',
             'neutral_site'])
    .sort('week')
)
gamelog.head(16) if gamelog.height else f'no games found for {team}'
```




    shape: (15, 6)
    ┌──────┬───────────────┬─────────┬─────────────┬────────┬──────────────┐
    │ week ┆ opponent      ┆ pts_for ┆ pts_against ┆ margin ┆ neutral_site │
    │ ---  ┆ ---           ┆ ---     ┆ ---         ┆ ---    ┆ ---          │
    │ i64  ┆ str           ┆ i64     ┆ i64         ┆ i64    ┆ bool         │
    ╞══════╪═══════════════╪═════════╪═════════════╪════════╪══════════════╡
    │ 1    ┆ Alabama       ┆ 27      ┆ 20          ┆ 7      ┆ true         │
    │ 1    ┆ Washington    ┆ 34      ┆ 13          ┆ 21     ┆ true         │
    │ 1    ┆ East Carolina ┆ 30      ┆ 3           ┆ 27     ┆ false        │
    │ 2    ┆ UNLV          ┆ 35      ┆ 7           ┆ 28     ┆ false        │
    │ 3    ┆ Bowling Green ┆ 31      ┆ 6           ┆ 25     ┆ false        │
    │ …    ┆ …             ┆ …       ┆ …           ┆ …      ┆ …            │
    │ 10   ┆ Purdue        ┆ 41      ┆ 13          ┆ 28     ┆ false        │
    │ 11   ┆ Penn State    ┆ 24      ┆ 15          ┆ 9      ┆ false        │
    │ 12   ┆ Maryland      ┆ 31      ┆ 24          ┆ 7      ┆ false        │
    │ 13   ┆ Ohio State    ┆ 30      ┆ 24          ┆ 6      ┆ false        │
    │ 14   ┆ Iowa          ┆ 26      ┆ 0           ┆ 26     ┆ true         │
    └──────┴───────────────┴─────────┴─────────────┴────────┴──────────────┘



### Recipe 8 — Rushing leaders, EPA included 🏃

Premium play-by-play means leaderboards aren't just totals — they carry **efficiency**. Filter to designed runs, sum the yards, and average the EPA per carry to separate the bell-cows from the truly explosive backs.


```python
rush_cols = {'rush', 'rusher_player_name', 'statYardage', 'EPA'}
if rush_cols.issubset(pbp.columns):
    rushers = (
        pbp
        .filter((pl.col('rush') == True)
                & pl.col('rusher_player_name').is_not_null())
        .group_by('rusher_player_name')
        .agg(
            pl.len().alias('carries'),
            pl.col('statYardage').sum().alias('rush_yds'),
            pl.col('EPA').mean().round(3).alias('epa_per_rush'),
        )
        .filter(pl.col('carries') >= 100)
        .sort('rush_yds', descending=True)
        .head(15)
    )
    out = rushers
else:
    out = 'rushing columns not present in this pbp build'
out
```




    shape: (15, 4)
    ┌────────────────────┬─────────┬──────────┬──────────────┐
    │ rusher_player_name ┆ carries ┆ rush_yds ┆ epa_per_rush │
    │ ---                ┆ ---     ┆ ---      ┆ ---          │
    │ str                ┆ u32     ┆ i64      ┆ f64          │
    ╞════════════════════╪═════════╪══════════╪══════════════╡
    │ Ollie Gordon II    ┆ 280     ┆ 1762     ┆ 0.077        │
    │ Kimani Vidal       ┆ 288     ┆ 1600     ┆ 0.048        │
    │ Cody Schrader      ┆ 274     ┆ 1585     ┆ 0.142        │
    │ Tahj Brooks        ┆ 289     ┆ 1570     ┆ 0.202        │
    │ Omarion Hampton    ┆ 243     ┆ 1503     ┆ 0.123        │
    │ …                  ┆ …       ┆ …        ┆ …            │
    │ Makhi Hughes       ┆ 255     ┆ 1365     ┆ 0.075        │
    │ Ismail Mahdi       ┆ 217     ┆ 1362     ┆ 0.146        │
    │ Jaydn Ott          ┆ 245     ┆ 1350     ┆ -0.008       │
    │ Marcus Carroll     ┆ 273     ┆ 1340     ┆ 0.038        │
    │ Ashton Jeanty      ┆ 212     ┆ 1340     ┆ 0.118        │
    └────────────────────┴─────────┴──────────┴──────────────┘



### Recipe 9 — The closest finishes of the year 🎢

Rank completed FBS games by final margin, breaking ties toward the highest-scoring shootouts, and you've got the season's white-knuckle finishes in one expression.


```python
thrillers = (
    schedule
    .filter((pl.col('completed') == True) & (pl.col('home_division') == 'fbs'))
    .with_columns(
        (pl.col('home_points') - pl.col('away_points')).abs().alias('margin'),
        (pl.col('home_points') + pl.col('away_points')).alias('total_points'),
    )
    .sort(['margin', 'total_points'], descending=[False, True])
    .select(['week', 'home_team', 'away_team',
             'home_points', 'away_points', 'margin'])
    .head(10)
)
thrillers
```




    shape: (10, 6)
    ┌──────┬────────────────┬───────────────┬─────────────┬─────────────┬────────┐
    │ week ┆ home_team      ┆ away_team     ┆ home_points ┆ away_points ┆ margin │
    │ ---  ┆ ---            ┆ ---           ┆ ---         ┆ ---         ┆ ---    │
    │ i64  ┆ str            ┆ str           ┆ i64         ┆ i64         ┆ i64    │
    ╞══════╪════════════════╪═══════════════╪═════════════╪═════════════╪════════╡
    │ 9    ┆ California     ┆ USC           ┆ 49          ┆ 50          ┆ 1      │
    │ 5    ┆ UL Monroe      ┆ App State     ┆ 40          ┆ 41          ┆ 1      │
    │ 5    ┆ UCF            ┆ Baylor        ┆ 35          ┆ 36          ┆ 1      │
    │ 2    ┆ Virginia       ┆ James Madison ┆ 35          ┆ 36          ┆ 1      │
    │ 5    ┆ UConn          ┆ Utah State    ┆ 33          ┆ 34          ┆ 1      │
    │ 12   ┆ Bowling Green  ┆ Toledo        ┆ 31          ┆ 32          ┆ 1      │
    │ 7    ┆ Colorado State ┆ Boise State   ┆ 31          ┆ 30          ┆ 1      │
    │ 10   ┆ Minnesota      ┆ Illinois      ┆ 26          ┆ 27          ┆ 1      │
    │ 10   ┆ Baylor         ┆ Houston       ┆ 24          ┆ 25          ┆ 1      │
    │ 13   ┆ Old Dominion   ┆ Georgia State ┆ 25          ┆ 24          ┆ 1      │
    └──────┴────────────────┴───────────────┴─────────────┴─────────────┴────────┘



### Recipe 10 — Where does the talent come from? 🗺️

Roll the season roster up by `home_state` to map the recruiting footprint of college football — a quick reminder of just how much of the sport flows out of a handful of states.


```python
talent_map = (
    rosters
    .filter(pl.col('cfbd_home_state').is_not_null())
    .group_by('cfbd_home_state')
    .agg(pl.len().alias('players'))
    .sort('players', descending=True)
    .head(15)
)
talent_map
```




    shape: (15, 2)
    ┌─────────────────┬─────────┐
    │ cfbd_home_state ┆ players │
    │ ---             ┆ ---     │
    │ str             ┆ u32     │
    ╞═════════════════╪═════════╡
    │ TX              ┆ 2510    │
    │ FL              ┆ 1819    │
    │ CA              ┆ 1723    │
    │ GA              ┆ 1560    │
    │ OH              ┆ 791     │
    │ …               ┆ …       │
    │ TN              ┆ 561     │
    │ IL              ┆ 542     │
    │ MD              ┆ 509     │
    │ NJ              ┆ 502     │
    │ SC              ┆ 467     │
    └─────────────────┴─────────┘



### Recipe 11 — Conference vs. non-conference, by margin 🔀

The schedule's `conference_game` flag lets you split the slate. Restrict to FBS, then compare the average final margin in league play versus the out-of-conference cupcakes — group games are (predictably) tighter.


```python
fbs = schedule.filter(
    (pl.col('home_division') == 'fbs') & (pl.col('completed') == True)
)
splits = (
    fbs
    .with_columns(
        (pl.col('home_points') - pl.col('away_points')).abs().alias('margin')
    )
    .group_by('conference_game')
    .agg(
        pl.len().alias('games'),
        pl.col('margin').mean().round(1).alias('avg_margin'),
        pl.col('home_points').add(pl.col('away_points'))
          .mean().round(1).alias('avg_total_points'),
    )
    .sort('conference_game')
)
splits
```




    shape: (2, 4)
    ┌─────────────────┬───────┬────────────┬──────────────────┐
    │ conference_game ┆ games ┆ avg_margin ┆ avg_total_points │
    │ ---             ┆ ---   ┆ ---        ┆ ---              │
    │ bool            ┆ u32   ┆ f64        ┆ f64              │
    ╞═════════════════╪═══════╪════════════╪══════════════════╡
    │ false           ┆ 362   ┆ 22.2       ┆ 54.0             │
    │ true            ┆ 548   ┆ 15.2       ┆ 53.5             │
    └─────────────────┴───────┴────────────┴──────────────────┘



### Recipe 12 — Biggest betting favorites in history 💸

[`load_cfb_betting_lines`](../cfb/reference/additional.md#load_cfb_betting_lines) is a premium release frame of historical sportsbook lines. Average the spread across books per game and sort to surface the most lopsided favorites — the mismatches Vegas saw coming a mile away.


```python
lines = safe('load_cfb_betting_lines', sdv.cfb.load_cfb_betting_lines)
if lines is not None and {'season', 'market_type', 'lines',
                          'game_desc', 'abbr'}.issubset(lines.columns):
    target = sorted(lines['season'].drop_nulls().unique().to_list())[-1]
    favorites = (
        lines
        .filter((pl.col('season') == target)
                & (pl.col('market_type') == 'spread')
                & pl.col('lines').is_not_null())
        .group_by(['game_desc', 'abbr'])
        .agg(pl.col('lines').mean().round(1).alias('avg_spread'))
        .filter(pl.col('avg_spread') < 0)  # negative spread = favorite
        .sort('avg_spread')
        .head(10)
    )
    print(f'biggest favorites, {int(target)} season:')
    out = favorites
else:
    out = 'betting-lines frame unavailable right now'
out
```

    ✅ load_cfb_betting_lines
    biggest favorites, 2025 season:





    shape: (10, 3)
    ┌────────────────────────────────┬────────────┬────────────┐
    │ game_desc                      ┆ abbr       ┆ avg_spread │
    │ ---                            ┆ ---        ┆ ---        │
    │ str                            ┆ str        ┆ f64        │
    ╞════════════════════════════════╪════════════╪════════════╡
    │ Bethune-Cookman@Miami          ┆ Miami      ┆ -54.5      │
    │ Arkansas-Pine Bluff@Texas Tech ┆ Texas Tech ┆ -54.5      │
    │ Samford@Texas A&M              ┆ Texas A&M  ┆ -54.2      │
    │ Grambling@Ohio State           ┆ Ohio State ┆ -53.5      │
    │ Samford@Baylor                 ┆ Baylor     ┆ -52.0      │
    │ The Citadel@Ole Miss           ┆ Ole Miss   ┆ -51.8      │
    │ Eastern Illinois@Alabama       ┆ Alabama    ┆ -51.3      │
    │ East Texas A&M@SMU             ┆ SMU        ┆ -50.8      │
    │ SE Louisiana@LSU               ┆ LSU        ┆ -48.8      │
    │ Western Illinois@Illinois      ┆ Illinois   ┆ -48.5      │
    └────────────────────────────────┴────────────┴────────────┘



### Recipe 13 — Hand it to pandas 🐼

Every loader takes `return_as_pandas=True`, and any polars frame converts with `.to_pandas()`. Once it's a pandas DataFrame the whole pandas/`numpy`/`scikit-learn` world opens up — here, a one-call `.describe()` of scoring across the season.


```python
score_pd = (
    schedule
    .select(['home_points', 'away_points'])
    .to_pandas()
)
score_pd['total_points'] = score_pd['home_points'] + score_pd['away_points']
print(type(score_pd).__module__)
score_pd.describe().round(1)
```

    pandas





           home_points  away_points  total_points
    count       3730.0       3730.0        3730.0
    mean          28.9         24.3          53.1
    std           16.0         15.0          17.8
    min            0.0          0.0           0.0
    25%           17.0         14.0          41.0
    50%           28.0         23.0          52.0
    75%           38.0         34.0          65.0
    max           96.0         91.0         132.0



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

    ✅ ESPN standings
    ✅ ESPN rankings (polls)





    shape: (5, 27)
    ┌────────────┬────────────┬─────────┬────────────┬───┬────────────┬───────────┬───────────┬────────┐
    │ group_name ┆ group_abbr ┆ team_id ┆ team_name  ┆ … ┆ vs         ┆ vs. conf. ┆ vs ap top ┆ vs usa │
    │ ---        ┆ eviation   ┆ ---     ┆ ---        ┆   ┆ division   ┆ ---       ┆ 25        ┆ ranked │
    │ str        ┆ ---        ┆ str     ┆ str        ┆   ┆ ---        ┆ str       ┆ ---       ┆ teams  │
    │            ┆ str        ┆         ┆            ┆   ┆ str        ┆           ┆ str       ┆ ---    │
    │            ┆            ┆         ┆            ┆   ┆            ┆           ┆           ┆ str    │
    ╞════════════╪════════════╪═════════╪════════════╪═══╪════════════╪═══════════╪═══════════╪════════╡
    │ American   ┆ American   ┆ 58      ┆ Bulls      ┆ … ┆ null       ┆ null      ┆ null      ┆ null   │
    │ Conference ┆            ┆         ┆            ┆   ┆            ┆           ┆           ┆        │
    │ American   ┆ American   ┆ 2226    ┆ Owls       ┆ … ┆ null       ┆ null      ┆ null      ┆ null   │
    │ Conference ┆            ┆         ┆            ┆   ┆            ┆           ┆           ┆        │
    │ American   ┆ American   ┆ 235     ┆ Tigers     ┆ … ┆ null       ┆ null      ┆ null      ┆ null   │
    │ Conference ┆            ┆         ┆            ┆   ┆            ┆           ┆           ┆        │
    │ American   ┆ American   ┆ 202     ┆ Golden     ┆ … ┆ null       ┆ null      ┆ null      ┆ null   │
    │ Conference ┆            ┆         ┆ Hurricane  ┆   ┆            ┆           ┆           ┆        │
    │ American   ┆ American   ┆ 2636    ┆ Roadrunner ┆ … ┆ null       ┆ null      ┆ null      ┆ null   │
    │ Conference ┆            ┆         ┆ s          ┆   ┆            ┆           ┆           ┆        │
    └────────────┴────────────┴─────────┴────────────┴───┴────────────┴───────────┴───────────┴────────┘




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

    ✅ ESPN passing leaders
    ✅ ESPN recruiting class





    shape: (1, 2)
    ┌──────┬─────────────────────────────────┐
    │ code ┆ message                         │
    │ ---  ┆ ---                             │
    │ i64  ┆ str                             │
    ╞══════╪═════════════════════════════════╡
    │ 400  ┆ http://sports.core.api.espn.pv… │
    └──────┴─────────────────────────────────┘



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

    ✅ CFBPlayProcess 401628334





    shape: (5, 5)
    ┌────────┬──────────┬──────┬──────────┬───────────┐
    │ period ┆ pos_team ┆ down ┆ distance ┆ EPA       │
    │ ---    ┆ ---      ┆ ---  ┆ ---      ┆ ---       │
    │ i64    ┆ i64      ┆ i64  ┆ i64      ┆ f64       │
    ╞════════╪══════════╪══════╪══════════╪═══════════╡
    │ 1      ┆ 99       ┆ 1    ┆ 10       ┆ -0.035415 │
    │ 1      ┆ 99       ┆ 1    ┆ 10       ┆ 1.233441  │
    │ 1      ┆ 99       ┆ 1    ┆ 10       ┆ 1.028077  │
    │ 1      ┆ 99       ┆ 1    ┆ 10       ┆ -0.480993 │
    │ 1      ┆ 99       ┆ 2    ┆ 8        ┆ 0.096702  │
    └────────┴──────────┴──────┴──────────┴───────────┘



## 🏛️ stats.ncaa.org football — `parse_cfb_ncaa_pbp` + box parsers

New in 0.0.72: parsers for the **stats.ncaa.org** football surface.
`parse_cfb_ncaa_pbp` takes the raw HTML of a `/contests/{id}/play_by_play`
page and emits one tidy row per play, cfbfastR-style — drive context,
down/distance/yard line, a classified `play_type`, extracted
players/yards/kick details, and boolean flags — while `cfb_ncaa_box` adds the
box-score / drives / officials tables:

```python
from sportsdataverse.cfb import parse_cfb_ncaa_pbp

pbp = parse_cfb_ncaa_pbp(html, contest_id=6081276)   # html = captured page
pbp.filter(pl.col("touchdown") == True).head()
```

stats.ncaa.org is rate-limit unfriendly, so this family is parser-first:
feed it pages captured by your own fetch pipeline (the shared proxy-bound
NCAA fetch layer used by the `ncaa_mbb_*` family) rather than scraping live
in a notebook.

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
