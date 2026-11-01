# NBA tutorial

> Welcome to the hardwood! In just a few lines of Python you're about to pull a whole season of NBA data — teams, standings, rosters, play-by-play, box scores, schedules and statistical leaders…

# 🏀 NBA hoops with `sportsdataverse-py`

> This page is the executed notebook [`04_nba_intro.ipynb`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/04_nba_intro.ipynb): [download it](https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/main/examples/notebooks/04_nba_intro.ipynb) to run it yourself.

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
| [`espn_nba_teams`](../nba/reference/additional/highlights.md#espn_nba_teams) | All 30 NBA teams (grab `team_id`s here) | ⭐ ESPN |
| [`espn_nba_scoreboard`](../nba/reference/site.md#espn_nba_scoreboard) | A day's slate — scores, status, matchups | ⭐ ESPN |
| [`espn_nba_schedule`](../nba/reference/additional/highlights.md#espn_nba_schedule) | Schedule for a date / date-range | ⭐ ESPN |
| [`espn_nba_standings`](../nba/reference/site.md#espn_nba_standings) | Conference standings (W-L, win%, streak) | ⭐ ESPN |
| [`espn_nba_team_roster`](../nba/reference/site.md#espn_nba_team_roster) | A team's active roster | ⭐ ESPN |
| [`espn_nba_team_schedule`](../nba/reference/site.md#espn_nba_team_schedule) | One team's full-season schedule | ⭐ ESPN |
| [`espn_nba_player_gamelog`](../nba/reference/web.md#espn_nba_player_gamelog) | A player's game-by-game log | ⭐ ESPN |
| [`espn_nba_leaders`](../nba/reference/web.md#espn_nba_leaders) | League statistical leaders | ⭐ ESPN |
| [`espn_nba_pbp`](../nba/reference/additional.md) | Full game payload (play-by-play, win prob, box) | ⭐ ESPN |
| [`espn_nba_game_rosters`](../nba/reference/additional.md) | Both teams' rosters for one game | ⭐ ESPN |
| [`load_nba_schedule`](../nba/reference/loaders/other.md#load_nba_pbp) | Multi-season schedule parquet | 📦 release |
| [`load_nba_player_boxscore`](../nba/reference/loaders/player.md#load_nba_player_boxscore) | Player box scores, every game | 📦 release |
| [`load_nba_standings`](../nba/reference/loaders/other.md#load_nba_standings) | Historical standings | 📦 release |
| [`espn_nba_injuries`](../nba/reference/site.md#espn_nba_injuries) | League-wide injury report, one row per team | ⭐ ESPN |
| [`load_nba_team_boxscore`](../nba/reference/loaders/team.md#load_nba_team_boxscore) | Team box scores, every game (off/def, shooting) | 📦 release |
| [`load_nba_shots`](../nba/reference/loaders/other.md#load_nba_shots) | Every made shot with court coordinates | 📦 release |
| [`most_recent_nba_season`](../nba/reference/additional/highlights.md#most_recent_nba_season) | The current season year helper | 🧮 util |

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

    current NBA season: 2027

ESPN endpoints are live and seasonal, so we'll route every network call
through a tiny `safe()` helper. When the feed is up you get the frame; when
it's mid-offseason or briefly rate-limited you get a friendly one-liner
instead of a scary traceback. 🛟

```python
from sportsdataverse.errors import AssetFetchError, NoDataError

def safe(label, thunk):
    try:
        out = thunk()
        n = out.height if isinstance(out, pl.DataFrame) else (len(out) if hasattr(out, '__len__') else '?')
        print(f'✅ {label} — {n} rows')
        return out
    except (NoDataError, AssetFetchError) as e:
        print(f"\u23ed\ufe0f  {label}: {type(e).__name__}: {e}")
        return None
```

## 🏟️ Teams

Start with [`espn_nba_teams`](../nba/reference/additional/highlights.md#espn_nba_teams) —
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
    │ i64     ┆ str           ┆ str       ┆ str               ┆ str        │
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
    │ i64       ┆ str        ┆ ---           ┆ ---           ┆ str        ┆ str        ┆ ---           │
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
    │ Detroit Pistons       ┆ 3.0  ┆ 0.0    ┆ 1.0         ┆ 0.0          ┆ 38.0               ┆ 3.0    │
    │ Portland Trail        ┆ 2.0  ┆ 0.0    ┆ 1.0         ┆ 0.5          ┆ 49.0               ┆ 2.0    │
    │ Blazers               ┆      ┆        ┆             ┆              ┆                    ┆        │
    │ Minnesota             ┆ 3.0  ┆ 0.0    ┆ 1.0         ┆ 0.0          ┆ 70.0               ┆ 3.0    │
    │ Timberwolves          ┆      ┆        ┆             ┆              ┆                    ┆        │
    │ Oklahoma City Thunder ┆ 3.0  ┆ 0.0    ┆ 1.0         ┆ 0.0          ┆ 31.0               ┆ 3.0    │
    │ …                     ┆ …    ┆ …      ┆ …           ┆ …            ┆ …                  ┆ …      │
    │ Houston Rockets       ┆ 3.0  ┆ 0.0    ┆ 1.0         ┆ 0.0          ┆ 29.0               ┆ 3.0    │
    │ Philadelphia 76ers    ┆ 3.0  ┆ 1.0    ┆ 0.75        ┆ 0.5          ┆ 12.0               ┆ -1.0   │
    │ Atlanta Hawks         ┆ 2.0  ┆ 1.0    ┆ 0.6666667   ┆ 1.0          ┆ 47.0               ┆ -1.0   │
    │ Toronto Raptors       ┆ 2.0  ┆ 1.0    ┆ 0.6666667   ┆ 1.0          ┆ 30.0               ┆ -1.0   │
    └───────────────────────┴──────┴────────┴─────────────┴──────────────┴────────────────────┴────────┘

## 🍳 Cookbook: common NBA tasks

Now the fun part — a handful of recipes you'll reach for again and again.
Each one leans on the premium `espn_nba_*` wrappers.

### Recipe 1 — A team and its roster 👥

Grab a `team_id` from [`espn_nba_teams`](../nba/reference/additional/highlights.md#espn_nba_teams),
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

    ✅ roster 2 — 17 rows

    shape: (10, 6)
    ┌──────────────────┬────────┬───────────────────────┬────────┬────────┬─────┐
    │ full_name        ┆ jersey ┆ position_abbreviation ┆ height ┆ weight ┆ age │
    │ ---              ┆ ---    ┆ ---                   ┆ ---    ┆ ---    ┆ --- │
    │ str              ┆ str    ┆ str                   ┆ f64    ┆ f64    ┆ i64 │
    ╞══════════════════╪════════╪═══════════════════════╪════════╪════════╪═════╡
    │ Devin Carter     ┆ 99     ┆ G                     ┆ 74.0   ┆ 188.0  ┆ 24  │
    │ Chris Cenac Jr.  ┆ 12     ┆ F                     ┆ 83.0   ┆ 240.0  ┆ 19  │
    │ Mike Conley      ┆ 45     ┆ G                     ┆ 73.0   ┆ 175.0  ┆ 39  │
    │ Luka Garza       ┆ 52     ┆ C                     ┆ 82.0   ┆ 243.0  ┆ 27  │
    │ …                ┆ …      ┆ …                     ┆ …      ┆ …      ┆ …   │
    │ Ron Harper Jr.   ┆ 8      ┆ G                     ┆ 77.0   ┆ 233.0  ┆ 26  │
    │ Sam Hauser       ┆ 30     ┆ F                     ┆ 80.0   ┆ 221.0  ┆ 28  │
    │ Dillon Mitchell  ┆ 20     ┆ F                     ┆ 80.0   ┆ 210.0  ┆ 23  │
    │ Payton Pritchard ┆ 11     ┆ G                     ┆ 74.0   ┆ 211.0  ┆ 28  │
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

    ✅ team schedule 2 — 80 rows

    shape: (5, 5)
    ┌───────────┬───────────────────┬─────────────────────────────────┬────────────┬─────────────┐
    │ id        ┆ date              ┆ name                            ┆ short_name ┆ season_year │
    │ ---       ┆ ---               ┆ ---                             ┆ ---        ┆ ---         │
    │ i64       ┆ str               ┆ str                             ┆ str        ┆ i64         │
    ╞═══════════╪═══════════════════╪═════════════════════════════════╪════════════╪═════════════╡
    │ 401909088 ┆ 2026-10-20T19:00Z ┆ Boston Celtics at Detroit Pist… ┆ BOS @ DET  ┆ 2027        │
    │ 401909095 ┆ 2026-10-23T23:00Z ┆ New York Knicks at Boston Celt… ┆ NY @ BOS   ┆ 2027        │
    │ 401909870 ┆ 2026-10-26T23:30Z ┆ Chicago Bulls at Boston Celtic… ┆ CHI @ BOS  ┆ 2027        │
    │ 401909877 ┆ 2026-10-27T23:30Z ┆ Brooklyn Nets at Boston Celtic… ┆ BKN @ BOS  ┆ 2027        │
    │ 401909278 ┆ 2026-10-30T23:30Z ┆ Chicago Bulls at Boston Celtic… ┆ CHI @ BOS  ┆ 2027        │
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

    ✅ LeBron gamelog — 5 rows

    shape: (5, 8)
    ┌────────────┬───────────────────────┬───────────┬─────────────┬───────┬────────┬────────┬────────┐
    │ event_date ┆ opponent_abbreviation ┆ home_away ┆ game_result ┆ score ┆ stat_0 ┆ stat_1 ┆ stat_2 │
    │ ---        ┆ ---                   ┆ ---       ┆ ---         ┆ ---   ┆ ---    ┆ ---    ┆ ---    │
    │ str        ┆ str                   ┆ str       ┆ str         ┆ str   ┆ str    ┆ str    ┆ str    │
    ╞════════════╪═══════════════════════╪═══════════╪═════════════╪═══════╪════════╪════════╪════════╡
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 33     ┆ 8-15   ┆ 53.3   │
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 33     ┆ 5-14   ┆ 35.7   │
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 34     ┆ 5-13   ┆ 38.5   │
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 25     ┆ 4-9    ┆ 44.4   │
    │ null       ┆ null                  ┆ null      ┆ null        ┆ null  ┆ 18     ┆ 1-1    ┆ 100.0  │
    └────────────┴───────────────────────┴───────────┴─────────────┴───────┴────────┴────────┴────────┘

### Recipe 4 — Top scorers from the box-score release 🥇

For a whole-season leaderboard the
[`load_nba_player_boxscore`](../nba/reference/loaders/player.md#load_nba_player_boxscore)
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

    ✅ player boxscore release — 1191 rows

    shape: (0, 6)
    ┌──────────────────────┬───────────────────┬─────┬─────┬─────┬─────┐
    │ athlete_display_name ┆ team_abbreviation ┆ gp  ┆ ppg ┆ rpg ┆ apg │
    │ ---                  ┆ ---               ┆ --- ┆ --- ┆ --- ┆ --- │
    │ str                  ┆ str               ┆ u32 ┆ f64 ┆ f64 ┆ f64 │
    ╞══════════════════════╪═══════════════════╪═════╪═════╪═════╪═════╡
    └──────────────────────┴───────────────────┴─────┴─────┴─────┴─────┘

### Recipe 5 — Offense vs defense, every team 🛡️

The [`load_nba_team_boxscore`](../nba/reference/loaders/team.md#load_nba_team_boxscore)
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

    ✅ team boxscore release — 86 rows

    shape: (10, 5)
    ┌───────────────────┬─────┬─────────┬─────────┬──────┐
    │ team_abbreviation ┆ gp  ┆ off_ppg ┆ def_ppg ┆ net  │
    │ ---               ┆ --- ┆ ---     ┆ ---     ┆ ---  │
    │ str               ┆ u32 ┆ f64     ┆ f64     ┆ f64  │
    ╞═══════════════════╪═════╪═════════╪═════════╪══════╡
    │ POR               ┆ 2   ┆ 126.0   ┆ 101.5   ┆ 24.5 │
    │ MIN               ┆ 3   ┆ 121.0   ┆ 97.7    ┆ 23.3 │
    │ ATL               ┆ 3   ┆ 124.7   ┆ 109.0   ┆ 15.7 │
    │ LAL               ┆ 3   ┆ 128.0   ┆ 115.0   ┆ 13.0 │
    │ …                 ┆ …   ┆ …       ┆ …       ┆ …    │
    │ OKC               ┆ 3   ┆ 106.7   ┆ 96.3    ┆ 10.4 │
    │ TOR               ┆ 3   ┆ 113.7   ┆ 103.7   ┆ 10.0 │
    │ HOU               ┆ 3   ┆ 121.0   ┆ 111.3   ┆ 9.7  │
    │ IND               ┆ 3   ┆ 121.3   ┆ 116.7   ┆ 4.6  │
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
    ┌───────────────────┬──────┬─────┬──────────────┐
    │ team_abbreviation ┆ made ┆ att ┆ three_pt_pct │
    │ ---               ┆ ---  ┆ --- ┆ ---          │
    │ str               ┆ i32  ┆ i32 ┆ f64          │
    ╞═══════════════════╪══════╪═════╪══════════════╡
    │ MIN               ┆ 50   ┆ 118 ┆ 42.4         │
    │ LAL               ┆ 45   ┆ 107 ┆ 42.1         │
    │ DET               ┆ 47   ┆ 112 ┆ 42.0         │
    │ HOU               ┆ 40   ┆ 96  ┆ 41.7         │
    │ …                 ┆ …    ┆ …   ┆ …            │
    │ MEM               ┆ 43   ┆ 113 ┆ 38.1         │
    │ BKN               ┆ 45   ┆ 119 ┆ 37.8         │
    │ MIL               ┆ 45   ┆ 121 ┆ 37.2         │
    │ DAL               ┆ 36   ┆ 100 ┆ 36.0         │
    └───────────────────┴──────┴─────┴──────────────┘

### Recipe 7 — Double-double machines 💪

A *double-double* is double digits in two of points / rebounds / assists.
Count the categories per player-game, keep the ones that cleared two, then
tally them up — straight from
[`load_nba_player_boxscore`](../nba/reference/loaders/player.md#load_nba_player_boxscore).

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

    ✅ player boxscore release — 1191 rows

    shape: (10, 3)
    ┌──────────────────────┬───────────────────┬────────────────┐
    │ athlete_display_name ┆ team_abbreviation ┆ double_doubles │
    │ ---                  ┆ ---               ┆ ---            │
    │ str                  ┆ str               ┆ u32            │
    ╞══════════════════════╪═══════════════════╪════════════════╡
    │ Karl-Anthony Towns   ┆ NY                ┆ 3              │
    │ Domantas Sabonis     ┆ SAC               ┆ 3              │
    │ Amen Thompson        ┆ HOU               ┆ 3              │
    │ Joel Embiid          ┆ PHI               ┆ 2              │
    │ …                    ┆ …                 ┆ …              │
    │ Keyonte George       ┆ UTAH              ┆ 2              │
    │ Victor Wembanyama    ┆ SA                ┆ 2              │
    │ Tyrese Haliburton    ┆ IND               ┆ 2              │
    │ Davion Mitchell      ┆ MIA               ┆ 2              │
    └──────────────────────┴───────────────────┴────────────────┘

### Recipe 8 — A tidy standings table 🏆

The [`load_nba_standings`](../nba/reference/loaders/other.md#load_nba_standings)
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

    ✅ standings release — 0 rows

    'standings release unavailable'

### Recipe 9 — Built on threes (shot release + a join) 🧱

[`load_nba_shots`](../nba/reference/loaders/other.md#load_nba_shots) is one row per
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

    ✅ shots release — 9978 rows

    shape: (0, 3)
    ┌───────────────────┬─────────────┬────────────────┐
    │ team_abbreviation ┆ threes_made ┆ pct_pts_from_3 │
    │ ---               ┆ ---         ┆ ---            │
    │ str               ┆ u32         ┆ f64            │
    ╞═══════════════════╪═════════════╪════════════════╡
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

    shape: (1, 5)
    ┌────────────┬────────────────┬────────────┬─────────────────────┬─────────────┐
    │ game_date  ┆ team_home_away ┆ team_score ┆ opponent_team_score ┆ team_winner │
    │ ---        ┆ ---            ┆ ---        ┆ ---                 ┆ ---         │
    │ date       ┆ str            ┆ i32        ┆ i32                 ┆ bool        │
    ╞════════════╪════════════════╪════════════╪═════════════════════╪═════════════╡
    │ 2026-10-23 ┆ home           ┆ 107        ┆ 103                 ┆ true        │
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
    │ Dallas Mavericks      ┆ 6              │
    │ Golden State Warriors ┆ 6              │
    │ Indiana Pacers        ┆ 6              │
    │ Philadelphia 76ers    ┆ 6              │
    │ …                     ┆ …              │
    │ Oklahoma City Thunder ┆ 4              │
    │ Sacramento Kings      ┆ 4              │
    │ Chicago Bulls         ┆ 3              │
    │ Detroit Pistons       ┆ 3              │
    └───────────────────────┴────────────────┘

## 🎬 Play-by-play & game rosters

Now for the granular stuff. [`espn_nba_pbp`](../nba/reference/additional.md)
with `return_parsed=False` returns the **whole game payload** as a dict — play-by-play, win probability,
box score, and header — keyed by `game_id` (an ESPN event id). Pair it with
[`espn_nba_game_rosters`](../nba/reference/additional.md) for who actually
suited up.

We'll use Game 1 of the 2024 Finals (`game_id=401585660`).

```python
GAME_ID = 401585660
pbp = safe('pbp payload', lambda: sdv.nba.espn_nba_pbp(game_id=GAME_ID, return_parsed=False))
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

    shape: (10, 5)
    ┌──────────────────────┬───────────────────┬─────────┬────────┬───────────────┐
    │ athlete_display_name ┆ team_abbreviation ┆ starter ┆ jersey ┆ position_name │
    │ ---                  ┆ ---               ┆ ---     ┆ ---    ┆ ---           │
    │ str                  ┆ str               ┆ bool    ┆ str    ┆ str           │
    ╞══════════════════════╪═══════════════════╪═════════╪════════╪═══════════════╡
    │ LeBron James         ┆ LAL               ┆ true    ┆ 23     ┆ Forward       │
    │ Anthony Davis        ┆ LAL               ┆ true    ┆ 23     ┆ Forward       │
    │ Rui Hachimura        ┆ LAL               ┆ true    ┆ 28     ┆ Forward       │
    │ Spencer Dinwiddie    ┆ LAL               ┆ true    ┆ 26     ┆ Point Guard   │
    │ …                    ┆ …                 ┆ …       ┆ …      ┆ …             │
    │ Cam Reddish          ┆ LAL               ┆ false   ┆ 5      ┆ Small Forward │
    │ Jaxson Hayes         ┆ LAL               ┆ false   ┆ 11     ┆ Center        │
    │ Max Christie         ┆ LAL               ┆ false   ┆ 0      ┆ Guard         │
    │ Harry Giles III      ┆ LAL               ┆ false   ┆ 20     ┆ Power Forward │
    └──────────────────────┴───────────────────┴─────────┴────────┴───────────────┘

## 📦 Bulk season data with the loaders

When you want *everything* for a season at once — not one game at a time —
the `load_nba_*` loaders pull pre-built parquet releases. They're fast,
reliable, and don't depend on a live API being up.

| Loader | Grain |
|---|---|
| [`load_nba_schedule`](../nba/reference/loaders/other.md#load_nba_schedule) | one row per game |
| [`load_nba_player_boxscore`](../nba/reference/loaders/player.md#load_nba_player_boxscore) | one row per player-game |
| [`load_nba_standings`](../nba/reference/loaders/other.md#load_nba_standings) | one row per team-season |

```python
sched = safe('schedule release', lambda: sdv.nba.load_nba_schedule(seasons=[SEASON]))
cols = ['id', 'date', 'home_display_name', 'away_display_name', 'home_score', 'away_score']
(sched.select([c for c in cols if c in sched.columns]).head()
 if sched is not None and sched.height else 'schedule release unavailable')
```

    ✅ schedule release — 1206 rows

    shape: (5, 6)
    ┌───────────┬───────────────────┬────────────────────┬───────────────────┬────────────┬────────────┐
    │ id        ┆ date              ┆ home_display_name  ┆ away_display_name ┆ home_score ┆ away_score │
    │ ---       ┆ ---               ┆ ---                ┆ ---               ┆ ---        ┆ ---        │
    │ i32       ┆ str               ┆ str                ┆ str               ┆ i32        ┆ i32        │
    ╞═══════════╪═══════════════════╪════════════════════╪═══════════════════╪════════════╪════════════╡
    │ 401910945 ┆ 2027-04-12T00:30Z ┆ Dallas Mavericks   ┆ Memphis Grizzlies ┆ 0          ┆ 0          │
    │ 401910946 ┆ 2027-04-12T00:30Z ┆ Minnesota          ┆ Houston Rockets   ┆ 0          ┆ 0          │
    │           ┆                   ┆ Timberwolves       ┆                   ┆            ┆            │
    │ 401910947 ┆ 2027-04-12T00:30Z ┆ San Antonio Spurs  ┆ Portland Trail    ┆ 0          ┆ 0          │
    │           ┆                   ┆                    ┆ Blazers           ┆            ┆            │
    │ 401910948 ┆ 2027-04-12T00:30Z ┆ Denver Nuggets     ┆ Oklahoma City     ┆ 0          ┆ 0          │
    │           ┆                   ┆                    ┆ Thunder           ┆            ┆            │
    │ 401910949 ┆ 2027-04-12T00:30Z ┆ Golden State       ┆ Utah Jazz         ┆ 0          ┆ 0          │
    │           ┆                   ┆ Warriors           ┆                   ┆            ┆            │
    └───────────┴───────────────────┴────────────────────┴───────────────────┴────────────┴────────────┘

### Pipeline: the highest-scoring games of the season 🔥

With the schedule release in hand, a combined-points leaderboard is a quick
polars pipeline — cast the scores, sum them, sort descending.

## stats.nba.com surface (`nba_stats_*`)

Beyond the ESPN wrappers, `sportsdataverse.nba` also ships **128 wrappers** for the official `stats.nba.com` API — the same source powering [nba_api](https://github.com/swar/nba_api). The module is `nba_stats` and every function is named `nba_stats_<slug>`. These are the **capture-confirmed live, non-deprecated** endpoints; deprecated and never-populated endpoints are excluded (see `ENDPOINT_HEALTH.md` in `sdv-internal-refs` for the full active/dying/barren/dead matrix).

**League routing** is a single `league_id` parameter: `"00"` → NBA, `"20"` → G-League, `"15"` → Summer League. One stem, one host, league chosen per call.

**curl_cffi required for live calls:** `stats.nba.com` uses TLS/JA3 fingerprinting that silently blocks plain `requests`. Install the optional transport: `pip install "sportsdataverse[all]"` (or `pip install curl_cffi`). The wrappers run fully offline when fed a fixture — live calls simply need the extra installed.

Wrappers return a tidy **polars DataFrame** by default (parsed via the generic `parse_nba_stats_result_sets` parser — which also flattens the shot-location endpoints' grouped 2-level headers into composite columns and unrolls the `scoreboardv3` game feed). Pass `return_parsed=False` for the raw `Dict` or `return_as_pandas=True` for pandas. Multi-result-set endpoints (e.g. `playercareerstats`) return `dict[str, DataFrame]`.

```python
from sportsdataverse.nba import nba_stats

# NBA player dashboard (polars DataFrame by default)
# Live calls require: pip install "sportsdataverse[all]"  (curl_cffi transport)
df = safe('nba_stats_leaguedashplayerstats (NBA)',
          lambda: nba_stats.nba_stats_leaguedashplayerstats(league_id="00"))

# Same endpoint, other leagues via league_id on the SAME stem:
gleague = safe('nba_stats_leaguedashplayerstats (G-League)',
               lambda: nba_stats.nba_stats_leaguedashplayerstats(league_id="20"))
summer  = safe('nba_stats_leaguedashplayerstats (Summer League)',
               lambda: nba_stats.nba_stats_leaguedashplayerstats(league_id="15"))

# Multi result-set endpoint -> dict[str, polars.DataFrame]
career = safe('nba_stats_playercareerstats (LeBron)',
              lambda: nba_stats.nba_stats_playercareerstats(player_id="2544"))

print('career result sets:', list(career.keys()) if isinstance(career, dict) else career)
df
```

    ✅ nba_stats_leaguedashplayerstats (NBA) — 582 rows

    ✅ nba_stats_leaguedashplayerstats (G-League) — 526 rows

    ✅ nba_stats_leaguedashplayerstats (Summer League) — 450 rows

    ✅ nba_stats_playercareerstats (LeBron) — 14 rows
    career result sets: ['SeasonTotalsRegularSeason', 'CareerTotalsRegularSeason', 'SeasonTotalsPostSeason', 'CareerTotalsPostSeason', 'SeasonTotalsAllStarSeason', 'CareerTotalsAllStarSeason', 'SeasonTotalsCollegeSeason', 'CareerTotalsCollegeSeason', 'SeasonTotalsShowcaseSeason', 'CareerTotalsShowcaseSeason', 'SeasonRankingsRegularSeason', 'SeasonRankingsPostSeason', 'SeasonHighs', 'CareerHighs']

    shape: (582, 69)
    ┌───────────┬────────────┬──────────┬───────────┬───┬──────────┬───────────┬───────────┬───────────┐
    │ player_id ┆ player_nam ┆ nickname ┆ team_id   ┆ … ┆ td3_rank ┆ wnba_fant ┆ fp_high_s ┆ team_coun │
    │ ---       ┆ e          ┆ ---      ┆ ---       ┆   ┆ ---      ┆ asy_pts_r ┆ core_rank ┆ t         │
    │ i64       ┆ ---        ┆ str      ┆ i64       ┆   ┆ i64      ┆ ank       ┆ ---       ┆ ---       │
    │           ┆ str        ┆          ┆           ┆   ┆          ┆ ---       ┆ i64       ┆ i64       │
    │           ┆            ┆          ┆           ┆   ┆          ┆ i64       ┆           ┆           │
    ╞═══════════╪════════════╪══════════╪═══════════╪═══╪══════════╪═══════════╪═══════════╪═══════════╡
    │ 1630639   ┆ A.J.       ┆ A.J.     ┆ 161061276 ┆ … ┆ 38       ┆ 449       ┆ 454       ┆ 1         │
    │           ┆ Lawson     ┆          ┆ 1         ┆   ┆          ┆           ┆           ┆           │
    │ 1631260   ┆ AJ Green   ┆ AJ       ┆ 161061274 ┆ … ┆ 38       ┆ 139       ┆ 167       ┆ 1         │
    │           ┆            ┆          ┆ 9         ┆   ┆          ┆           ┆           ┆           │
    │ 1642358   ┆ AJ Johnson ┆ AJ       ┆ 161061274 ┆ … ┆ 38       ┆ 413       ┆ 412       ┆ 2         │
    │           ┆            ┆          ┆ 2         ┆   ┆          ┆           ┆           ┆           │
    │ 203932    ┆ Aaron      ┆ Aaron    ┆ 161061274 ┆ … ┆ 38       ┆ 241       ┆ 250       ┆ 1         │
    │           ┆ Gordon     ┆          ┆ 3         ┆   ┆          ┆           ┆           ┆           │
    │ …         ┆ …          ┆ …        ┆ …         ┆ … ┆ …        ┆ …         ┆ …         ┆ …         │
    │ 1630192   ┆ Zeke Nnaji ┆ Zeke     ┆ 161061274 ┆ … ┆ 38       ┆ 371       ┆ 373       ┆ 1         │
    │           ┆            ┆          ┆ 3         ┆   ┆          ┆           ┆           ┆           │
    │ 1630533   ┆ Ziaire     ┆ Ziaire   ┆ 161061275 ┆ … ┆ 38       ┆ 233       ┆ 244       ┆ 1         │
    │           ┆ Williams   ┆          ┆ 1         ┆   ┆          ┆           ┆           ┆           │
    │ 1629627   ┆ Zion       ┆ Zion     ┆ 161061274 ┆ … ┆ 38       ┆ 70        ┆ 65        ┆ 1         │
    │           ┆ Williamson ┆          ┆ 0         ┆   ┆          ┆           ┆           ┆           │
    │ 1642389   ┆ Zyon       ┆ Zyon     ┆ 161061275 ┆ … ┆ 38       ┆ 537       ┆ 533       ┆ 1         │
    │           ┆ Pullin     ┆          ┆ 0         ┆   ┆          ┆           ┆           ┆           │
    └───────────┴────────────┴──────────┴───────────┴───┴──────────┴───────────┴───────────┴───────────┘

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
    ┌───────────────────┬─────────────────────┬──────────────────────┬────────────┬────────────┐
    │ date              ┆ home_display_name   ┆ away_display_name    ┆ home_score ┆ away_score │
    │ ---               ┆ ---                 ┆ ---                  ┆ ---        ┆ ---        │
    │ str               ┆ str                 ┆ str                  ┆ i32        ┆ i32        │
    ╞═══════════════════╪═════════════════════╪══════════════════════╪════════════╪════════════╡
    │ 2026-10-25T01:00Z ┆ Denver Nuggets      ┆ New Orleans Pelicans ┆ 141        ┆ 135        │
    │ 2026-10-23T20:00Z ┆ Milwaukee Bucks     ┆ Chicago Bulls        ┆ 125        ┆ 132        │
    │ 2026-10-21T23:00Z ┆ Orlando Magic       ┆ Atlanta Hawks        ┆ 125        ┆ 132        │
    │ 2026-10-25T21:00Z ┆ Utah Jazz           ┆ Los Angeles Lakers   ┆ 123        ┆ 132        │
    │ …                 ┆ …                   ┆ …                    ┆ …          ┆ …          │
    │ 2026-10-24T23:30Z ┆ Cleveland Cavaliers ┆ Brooklyn Nets        ┆ 125        ┆ 121        │
    │ 2026-10-25T22:00Z ┆ Brooklyn Nets       ┆ Indiana Pacers       ┆ 109        ┆ 136        │
    │ 2026-10-25T00:00Z ┆ Chicago Bulls       ┆ Washington Wizards   ┆ 120        ┆ 124        │
    │ 2026-10-21T23:00Z ┆ Washington Wizards  ┆ Milwaukee Bucks      ┆ 128        ┆ 116        │
    └───────────────────┴─────────────────────┴──────────────────────┴────────────┴────────────┘

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

## 🦓 Officiating

Last Two Minute reports and referee assignments, from `official.nba.com` and the
NBA's own feeds.

```python
from sportsdataverse.nba import nba_officiating

print("nba_officiating:", [n for n in dir(nba_officiating) if not n.startswith("_")][:8])
safe("referee assignments", lambda: nba_officiating.nba_referee_assignments("2024-04-01"))
safe("L2M games", lambda: nba_officiating.nba_l2m_games(2024))

```

    nba_officiating: ['Any', 'AssetFetchError', 'L2M_CALLS_SCHEMA', 'L2M_GAME_SCHEMA', 'L2M_STATS_SCHEMA', 'Literal', 'NBA_REFEREE_ASSIGN_SCHEMA', 'NBA_REFEREE_REPLAY_SCHEMA']

    ✅ referee assignments — 2 rows

    ✅ L2M games — 413 rows

    shape: (413, 4)
    ┌────────────┬────────┬─────────────┬─────────────────────────────────┐
    │ game_id    ┆ season ┆ season_type ┆ label                           │
    │ ---        ┆ ---    ┆ ---         ┆ ---                             │
    │ str        ┆ i32    ┆ str         ┆ str                             │
    ╞════════════╪════════╪═════════════╪═════════════════════════════════╡
    │ 0042300403 ┆ 2024   ┆ playoffs    ┆ Celtics 106, Mavericks 99       │
    │ 0042300314 ┆ 2024   ┆ playoffs    ┆ Timberwolves 105, Mavericks 10… │
    │ 0042300304 ┆ 2024   ┆ playoffs    ┆ Celtics 105, Pacers 102         │
    │ 0042300303 ┆ 2024   ┆ playoffs    ┆ Celtics 114, Pacers 111         │
    │ …          ┆ …      ┆ …           ┆ …                               │
    │ 0022300068 ┆ 2024   ┆ regular     ┆ Heat 103, Pistons 102           │
    │ 0022300069 ┆ 2024   ┆ regular     ┆ Raptors 97, Timberwolves 94     │
    │ 0022300073 ┆ 2024   ┆ regular     ┆ Mavericks 126, Spurs 119        │
    │ 0022300062 ┆ 2024   ┆ regular     ┆ Suns 108, Warriors 104          │
    └────────────┴────────┴─────────────┴─────────────────────────────────┘
