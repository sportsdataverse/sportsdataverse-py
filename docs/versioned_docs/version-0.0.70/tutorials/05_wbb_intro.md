---
title: WBB tutorial
sidebar_label: WBB
sidebar_position: 5
---

# 🏀 Women's college basketball with `sportsdataverse-py`

Welcome to the **women's college hoops** corner of the SportsDataverse! 🎉 In a handful of lines you're about to pull rosters, schedules, play-by-play, live scoreboards, AP rankings, **ESPN's Basketball Power Index (BPI)**, in-game **win-probability** curves, and season-long parquet releases — all returned as tidy [polars](https://pola.rs) DataFrames that are ready to model. 🚀

`sportsdataverse.wbb` leads with ESPN's deep **`espn_wbb_*`** women's-college-basketball surface — over a hundred endpoints — plus blazing-fast `load_wbb_*` data loaders. If you know the R package [wehoop](https://wehoop.sportsdataverse.org), these names will feel like home. Let's go scout some hoopers! 🏀

## 🧰 The toolbox

Every accessor returns a tidy **polars** `DataFrame` by default — pass `return_as_pandas=True` for pandas. The ⭐ rows are the **premium ESPN analytics** surfaces we lead with. Click any name for the full reference:

| Function | What it gives you | Source |
|---|---|---|
| [`espn_wbb_teams`](../wbb/reference/additional.md#espn_wbb_teams) | Every D-I program, one wide row each | ESPN |
| [`espn_wbb_team_roster`](../wbb/reference/site.md#espn_wbb_team_roster) | A team's roster, one row per player | ESPN |
| [`espn_wbb_schedule`](../wbb/reference/additional.md#espn_wbb_schedule) | Games for a date / date-range | ESPN |
| [`espn_wbb_team_schedule`](../wbb/reference/site.md#espn_wbb_team_schedule) | One program's full season slate | ESPN |
| [`espn_wbb_scoreboard`](../wbb/reference/site.md#espn_wbb_scoreboard) | ⭐ Live + final scoreboard, one row per game | ESPN |
| [`espn_wbb_pbp`](../wbb/reference/additional.md#espn_wbb_pbp) | Full play-by-play + boxscore for a game | ESPN |
| [`espn_wbb_player_gamelog`](../wbb/reference/web.md#espn_wbb_player_gamelog) | A player's game-by-game log | ESPN |
| [`espn_wbb_player_splits`](../wbb/reference/web.md#espn_wbb_player_splits) | A player's situational stat splits | ESPN |
| [`espn_wbb_team_stats`](../wbb/reference/additional.md#espn_wbb_team_stats) | A team's season stat splits | ESPN |
| [`espn_wbb_standings`](../wbb/reference/site.md#espn_wbb_standings) | Conference standings + records | ESPN |
| [`espn_wbb_conferences`](../wbb/reference/site.md#espn_wbb_conferences) | Conference groups + group ids | ESPN |
| [`espn_wbb_rankings`](../wbb/reference/site.md#espn_wbb_rankings) | ⭐ AP / Coaches poll rankings | ESPN |
| [`espn_wbb_leaders`](../wbb/reference/web.md#espn_wbb_leaders) | ⭐ League statistical leaders | ESPN |
| [`espn_wbb_injuries`](../wbb/reference/site.md#espn_wbb_injuries) | ⭐ Active injury report | ESPN |
| [`espn_wbb_season_powerindex`](../wbb/reference/core.md#espn_wbb_season_powerindex) | ⭐ **BPI** ratings, one row per team | ESPN |
| [`espn_wbb_season_powerindex_leaders`](../wbb/reference/core.md#espn_wbb_season_powerindex_leaders) | ⭐ BPI / SOS / SOR category leaders | ESPN |
| [`espn_wbb_game_predictor`](../wbb/reference/core.md#espn_wbb_game_predictor) | ⭐ BPI matchup projection for a game | ESPN |
| [`espn_wbb_game_probabilities`](../wbb/reference/core.md#espn_wbb_game_probabilities) | ⭐ Play-by-play win-probability curve | ESPN |
| [`espn_wbb_calendar`](../wbb/reference/site.md#espn_wbb_calendar) | Valid game dates for a season | ESPN |
| [`load_wbb_schedule`](../wbb/reference/loaders.md#load_wbb_schedule) | Season-long schedule (parquet release) | release |
| [`load_wbb_pbp`](../wbb/reference/loaders.md#load_wbb_pbp) | Season-long play-by-play (parquet, back to 2002) | release |
| [`load_wbb_team_boxscore`](../wbb/reference/loaders.md#load_wbb_team_boxscore) | Season-long team boxscores (parquet) | release |
| [`load_wbb_player_boxscore`](../wbb/reference/loaders.md#load_wbb_player_boxscore) | Season-long player boxscores (parquet) | release |

## 🔌 Setup

```sh
pip install sportsdataverse
```

**No API key needed** — the ESPN endpoints and the parquet releases are all public. 😊


```python
import polars as pl
import sportsdataverse as sdv
import sportsdataverse.wbb as wbb

SEASON = 2025  # the 2024-25 season — UConn's title run
print('most recent wbb season:', wbb.most_recent_wbb_season())
```

    most recent wbb season: 2026


ESPN's live endpoints are **seasonal** — polls, injuries, and live scoreboards go quiet in the offseason, and any network call can hiccup. So we use a tiny `safe()` helper: you get the frame when the feed is up, and a friendly one-liner when it isn't (never a scary traceback). 🛟 The `load_wbb_*` parquet loaders are rock-solid year-round, so we lean on those for anything historical.


```python
def safe(label, thunk):
    """Run a live call defensively; return None (with a note) if it can't."""
    try:
        out = thunk()
        ok = out is not None and (not hasattr(out, 'height') or out.height > 0)
        print(f"{'✅' if ok else '🟡'} {label}" + ('' if ok else ' (no rows right now)'))
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None


def has_rows(df):
    return df is not None and hasattr(df, 'height') and df.height > 0
```

## 🏟️ Teams

[`espn_wbb_teams`](../wbb/reference/additional.md#espn_wbb_teams) returns one wide row per Division-I program. The `team_id` here is the key you'll feed to roster, stats, and leader endpoints. (NCAA team frames carry no conference column — that comes from `espn_wbb_standings()` / `espn_wbb_conferences()` below.)


```python
teams = safe('teams', wbb.espn_wbb_teams)
(teams.select(['team_id', 'team_location', 'team_name', 'team_abbreviation', 'team_display_name']).head(10)
 if has_rows(teams) else 'teams unavailable')
```

    ✅ teams





    shape: (10, 5)
    ┌─────────┬─────────────────────┬──────────────┬───────────────────┬────────────────────────────┐
    │ team_id ┆ team_location       ┆ team_name    ┆ team_abbreviation ┆ team_display_name          │
    │ ---     ┆ ---                 ┆ ---          ┆ ---               ┆ ---                        │
    │ str     ┆ str                 ┆ str          ┆ str               ┆ str                        │
    ╞═════════╪═════════════════════╪══════════════╪═══════════════════╪════════════════════════════╡
    │ 2000    ┆ Abilene Christian   ┆ Wildcats     ┆ ACU               ┆ Abilene Christian Wildcats │
    │ 2005    ┆ Air Force           ┆ Falcons      ┆ AF                ┆ Air Force Falcons          │
    │ 2006    ┆ Akron               ┆ Zips         ┆ AKR               ┆ Akron Zips                 │
    │ 2010    ┆ Alabama A&M         ┆ Bulldogs     ┆ AAMU              ┆ Alabama A&M Bulldogs       │
    │ 333     ┆ Alabama             ┆ Crimson Tide ┆ ALA               ┆ Alabama Crimson Tide       │
    │ 2011    ┆ Alabama State       ┆ Lady Hornets ┆ ALST              ┆ Alabama State Lady Hornets │
    │ 2016    ┆ Alcorn State        ┆ Lady Braves  ┆ ALCN              ┆ Alcorn State Lady Braves   │
    │ 44      ┆ American University ┆ Eagles       ┆ AMER              ┆ American University Eagles │
    │ 2026    ┆ App State           ┆ Mountaineers ┆ APP               ┆ App State Mountaineers     │
    │ 9       ┆ Arizona State       ┆ Sun Devils   ┆ ASU               ┆ Arizona State Sun Devils   │
    └─────────┴─────────────────────┴──────────────┴───────────────────┴────────────────────────────┘



## 👥 Team roster

[`espn_wbb_team_roster`](../wbb/reference/site.md#espn_wbb_team_roster) takes a `team_id` and `season` and returns one row per player. Here's the 2024-25 **UConn Huskies** (`team_id=2509`) — the eventual national champions, led by Paige Bueckers.


```python
uconn = safe('UConn roster', lambda: wbb.espn_wbb_team_roster(team_id=2509, season=SEASON))
(uconn.select(['athlete_id', 'full_name', 'jersey', 'position_abbreviation', 'display_height', 'display_weight']).head(12)
 if has_rows(uconn) else 'roster unavailable')
```

    ✅ UConn roster





    shape: (12, 6)
    ┌────────────┬────────────────────┬────────┬─────────────────────┬────────────────┬────────────────┐
    │ athlete_id ┆ full_name          ┆ jersey ┆ position_abbreviati ┆ display_height ┆ display_weight │
    │ ---        ┆ ---                ┆ ---    ┆ on                  ┆ ---            ┆ ---            │
    │ str        ┆ str                ┆ str    ┆ ---                 ┆ str            ┆ str            │
    │            ┆                    ┆        ┆ str                 ┆                ┆                │
    ╞════════════╪════════════════════╪════════╪═════════════════════╪════════════════╪════════════════╡
    │ 5311737    ┆ Carley Barrett     ┆ 24     ┆ G                   ┆ 5' 7"          ┆ null           │
    │ 5106182    ┆ Tara Daye          ┆ 44     ┆ G                   ┆ 5' 10"         ┆ null           │
    │ 5107710    ┆ Taylor Feldman     ┆ 5      ┆ G                   ┆ 5' 8"          ┆ null           │
    │ 5311739    ┆ Avery Gordon       ┆ 55     ┆ F                   ┆ 6' 7"          ┆ null           │
    │ 5108895    ┆ Taylor Henderson   ┆ 2      ┆ G                   ┆ 5' 11"         ┆ null           │
    │ …          ┆ …                  ┆ …      ┆ …                   ┆ …              ┆ …              │
    │ 4433438    ┆ Madison Layden-Zay ┆ 33     ┆ G                   ┆ 6' 1"          ┆ null           │
    │ 5240041    ┆ Lana McCarthy      ┆ 35     ┆ F                   ┆ 6' 4"          ┆ null           │
    │ 5240040    ┆ Kendall Puryear    ┆ 22     ┆ F                   ┆ 6' 3"          ┆ null           │
    │ 5239064    ┆ Kiki Smith         ┆ 23     ┆ G                   ┆ 5' 7"          ┆ null           │
    │ 5243531    ┆ Nya Smith          ┆ 3      ┆ G                   ┆ 5' 9"          ┆ null           │
    └────────────┴────────────────────┴────────┴─────────────────────┴────────────────┴────────────────┘



## 📅 Schedule & scoreboard

Two complementary views of a slate:

| Function | Best for |
|---|---|
| [`espn_wbb_schedule`](../wbb/reference/additional.md#espn_wbb_schedule) | a clean game list for a date or `'YYYYMMDD-YYYYMMDD'` range |
| [`espn_wbb_scoreboard`](../wbb/reference/site.md#espn_wbb_scoreboard) | ⭐ a richer live/final scoreboard (status, venue, scores) |

April 4, 2025 was the women's **Final Four**. Note: `home_score` / `away_score` from `espn_wbb_schedule` arrive as **strings**, so cast before arithmetic.


```python
final_four = safe('Final Four schedule', lambda: wbb.espn_wbb_schedule(dates=20250404))
(final_four.select(['id', 'date', 'away_display_name', 'away_score', 'home_display_name', 'home_score', 'status_type_completed'])
 if has_rows(final_four) else 'schedule unavailable')
```

    ✅ Final Four schedule





    shape: (2, 7)
    ┌───────────┬───────────────┬──────────────┬────────────┬──────────────┬────────────┬──────────────┐
    │ id        ┆ date          ┆ away_display ┆ away_score ┆ home_display ┆ home_score ┆ status_type_ │
    │ ---       ┆ ---           ┆ _name        ┆ ---        ┆ _name        ┆ ---        ┆ completed    │
    │ str       ┆ str           ┆ ---          ┆ str        ┆ ---          ┆ str        ┆ ---          │
    │           ┆               ┆ str          ┆            ┆ str          ┆            ┆ bool         │
    ╞═══════════╪═══════════════╪══════════════╪════════════╪══════════════╪════════════╪══════════════╡
    │ 401746073 ┆ 2025-04-04T23 ┆ Texas        ┆ 57         ┆ South        ┆ 74         ┆ true         │
    │           ┆ :00Z          ┆ Longhorns    ┆            ┆ Carolina     ┆            ┆              │
    │           ┆               ┆              ┆            ┆ Gamecocks    ┆            ┆              │
    │ 401746074 ┆ 2025-04-05T01 ┆ UConn        ┆ 85         ┆ UCLA Bruins  ┆ 51         ┆ true         │
    │           ┆ :30Z          ┆ Huskies      ┆            ┆              ┆            ┆              │
    └───────────┴───────────────┴──────────────┴────────────┴──────────────┴────────────┴──────────────┘




```python
# ⭐ The scoreboard view of the same date — richer game-state columns
board = safe('Final Four scoreboard', lambda: wbb.espn_wbb_scoreboard(dates=20250404))
keep = ['game_id', 'short_name', 'status_type_completed', 'home_team_short_display_name',
        'home_team_score', 'away_team_short_display_name', 'away_team_score']
(board.select([c for c in keep if c in board.columns])
 if has_rows(board) else 'scoreboard unavailable')
```

    ✅ Final Four scoreboard





    shape: (2, 3)
    ┌───────────┬──────────────┬───────────────────────┐
    │ game_id   ┆ short_name   ┆ status_type_completed │
    │ ---       ┆ ---          ┆ ---                   │
    │ str       ┆ str          ┆ bool                  │
    ╞═══════════╪══════════════╪═══════════════════════╡
    │ 401746073 ┆ TEX VS SC    ┆ true                  │
    │ 401746074 ┆ CONN VS UCLA ┆ true                  │
    └───────────┴──────────────┴───────────────────────┘



## 🎬 Play-by-play

[`espn_wbb_pbp`](../wbb/reference/additional.md#espn_wbb_pbp) returns a **dict** of game components (`plays`, `boxscore`, `header`, `winprobability`, …). The `plays` value is a list of dicts — build a frame with `pl.DataFrame(pbp['plays'], infer_schema_length=None)`. Columns use ESPN dot-notation (`period.number`, `clock.displayValue`, `type.text`, `scoringPlay`).

Game `401746075` is the **2025 national championship**: South Carolina vs. UConn.


```python
pbp = safe('championship pbp', lambda: wbb.espn_wbb_pbp(game_id=401746075))
plays = None
if pbp is not None and isinstance(pbp, dict) and pbp.get('plays'):
    plays = pl.DataFrame(pbp['plays'], infer_schema_length=None)
    print('plays shape:', plays.shape, '| components:', list(pbp.keys())[:8])
(plays.select(['period.number', 'clock.displayValue', 'type.text', 'scoringPlay', 'text']).head()
 if plays is not None else 'pbp unavailable')
```

    ✅ championship pbp
    plays shape: (443, 58) | components: ['gameId', 'plays', 'winprobability', 'boxscore', 'header', 'format', 'broadcasts', 'videos']





    shape: (5, 5)
    ┌───────────────┬────────────────────┬───────────────────┬─────────────┬───────────────────────────┐
    │ period.number ┆ clock.displayValue ┆ type.text         ┆ scoringPlay ┆ text                      │
    │ ---           ┆ ---                ┆ ---               ┆ ---         ┆ ---                       │
    │ i64           ┆ str                ┆ str               ┆ bool        ┆ str                       │
    ╞═══════════════╪════════════════════╪═══════════════════╪═════════════╪═══════════════════════════╡
    │ 1             ┆ 10:00              ┆ Jumpball          ┆ false       ┆ Start game                │
    │ 1             ┆ 9:57               ┆ Jumpball          ┆ false       ┆ Jump Ball won by UConn    │
    │ 1             ┆ 9:57               ┆ Jumpball          ┆ false       ┆ Jump Ball lost by South   │
    │               ┆                    ┆                   ┆             ┆ Caroli…                   │
    │ 1             ┆ 9:40               ┆ JumpShot          ┆ false       ┆ Kaitlyn Chen missed Three │
    │               ┆                    ┆                   ┆             ┆ Poin…                     │
    │ 1             ┆ 9:33               ┆ Offensive Rebound ┆ false       ┆ Paige Bueckers Offensive  │
    │               ┆                    ┆                   ┆             ┆ Rebou…                    │
    └───────────────┴────────────────────┴───────────────────┴─────────────┴───────────────────────────┘




```python
# Scoring plays only, with the running score
(plays.filter(pl.col('scoringPlay') == True)
      .select(['period.number', 'clock.displayValue', 'awayScore', 'homeScore', 'text']).head(8)
 if plays is not None else 'pbp unavailable')
```




    shape: (8, 5)
    ┌───────────────┬────────────────────┬───────────┬───────────┬─────────────────────────────────┐
    │ period.number ┆ clock.displayValue ┆ awayScore ┆ homeScore ┆ text                            │
    │ ---           ┆ ---                ┆ ---       ┆ ---       ┆ ---                             │
    │ i64           ┆ str                ┆ i64       ┆ i64       ┆ str                             │
    ╞═══════════════╪════════════════════╪═══════════╪═══════════╪═════════════════════════════════╡
    │ 1             ┆ 9:18               ┆ 0         ┆ 3         ┆ Te-Hina Paopao made Three Poin… │
    │ 1             ┆ 8:58               ┆ 2         ┆ 3         ┆ Sarah Strong made Jumper.       │
    │ 1             ┆ 8:36               ┆ 2         ┆ 5         ┆ Chloe Kitts made Jumper.        │
    │ 1             ┆ 8:13               ┆ 4         ┆ 5         ┆ Paige Bueckers made Jumper.     │
    │ 1             ┆ 7:24               ┆ 6         ┆ 5         ┆ Azzi Fudd made Jumper. Assiste… │
    │ 1             ┆ 7:00               ┆ 6         ┆ 7         ┆ Raven Johnson made Layup. Assi… │
    │ 1             ┆ 6:40               ┆ 8         ┆ 7         ┆ Kaitlyn Chen made Jumper.       │
    │ 1             ┆ 6:23               ┆ 8         ┆ 9         ┆ Bree Hall made Jumper.          │
    └───────────────┴────────────────────┴───────────┴───────────┴─────────────────────────────────┘



## ⭐ Premium ESPN analytics

This is where `espn_wbb_*` shines. Three live league-wide feeds, each one line:

| Function | Gives you |
|---|---|
| [`espn_wbb_rankings`](../wbb/reference/site.md#espn_wbb_rankings) | the current AP / Coaches poll |
| [`espn_wbb_leaders`](../wbb/reference/web.md#espn_wbb_leaders) | league statistical leaders (PPG, RPG, APG, …) |
| [`espn_wbb_injuries`](../wbb/reference/site.md#espn_wbb_injuries) | the active injury report |

These are **in-season** feeds, so out of season they return empty — our `safe()` helper handles that gracefully.


```python
rankings = safe('rankings (AP/Coaches poll)', wbb.espn_wbb_rankings)
(rankings.head(12) if has_rows(rankings)
 else 'no poll published right now (offseason) — try during the season')
```

    🟡 rankings (AP/Coaches poll) (no rows right now)





    'no poll published right now (offseason) — try during the season'




```python
injuries = safe('injury report', wbb.espn_wbb_injuries)
(injuries.head(10) if has_rows(injuries)
 else 'no active injuries posted right now (offseason)')
```

    🟡 injury report (no rows right now)





    'no active injuries posted right now (offseason)'



## 📊 Basketball Power Index (BPI)

ESPN's **BPI** is a forward-looking team-strength rating — expected point margin per 70 possessions against an average opponent on a neutral floor. [`espn_wbb_season_powerindex`](../wbb/reference/core.md#espn_wbb_season_powerindex) returns one row per ranked team, with a nested `stats` list (BPI, BPI rank, SOS, SOR, …). Let's unnest it into a clean BPI leaderboard for 2024-25.


```python
import ast

spi = safe('season BPI', lambda: wbb.espn_wbb_season_powerindex(season=SEASON))


def pick(stats, name):
    # The nested `stats` value arrives as a Python-repr string — parse it safely
    if isinstance(stats, str):
        try:
            stats = ast.literal_eval(stats)
        except (ValueError, SyntaxError):
            return None
    for s in (stats or []):
        if isinstance(s, dict) and s.get('name') == name:
            return s.get('value')
    return None


if has_rows(spi):
    rows = [
        {
            'bpi_rank': pick(r['stats'], 'bpirank'),
            'bpi': pick(r['stats'], 'bpi'),
            'conference_id': r.get('conference_id'),
            'team_ref': r.get('team_$ref'),
        }
        for r in spi.to_dicts()
    ]
    out = pl.DataFrame(rows).sort('bpi', descending=True, nulls_last=True).head(12)
else:
    out = 'BPI unavailable right now'
out
```

    ✅ season BPI





    shape: (12, 4)
    ┌──────────┬─────────┬───────────────┬─────────────────────────────────┐
    │ bpi_rank ┆ bpi     ┆ conference_id ┆ team_ref                        │
    │ ---      ┆ ---     ┆ ---           ┆ ---                             │
    │ f64      ┆ f64     ┆ i64           ┆ str                             │
    ╞══════════╪═════════╪═══════════════╪═════════════════════════════════╡
    │ 1.0      ┆ 38.1724 ┆ 4             ┆ http://sports.core.api.espn.co… │
    │ 2.0      ┆ 36.4436 ┆ 23            ┆ http://sports.core.api.espn.co… │
    │ 3.0      ┆ 33.1668 ┆ 23            ┆ http://sports.core.api.espn.co… │
    │ 4.0      ┆ 32.1685 ┆ 2             ┆ http://sports.core.api.espn.co… │
    │ 5.0      ┆ 31.8751 ┆ 7             ┆ http://sports.core.api.espn.co… │
    │ …        ┆ …       ┆ …             ┆ …                               │
    │ 8.0      ┆ 28.45   ┆ 23            ┆ http://sports.core.api.espn.co… │
    │ 9.0      ┆ 26.9175 ┆ 8             ┆ http://sports.core.api.espn.co… │
    │ 10.0     ┆ 26.1062 ┆ 23            ┆ http://sports.core.api.espn.co… │
    │ 11.0     ┆ 25.8556 ┆ 23            ┆ http://sports.core.api.espn.co… │
    │ 12.0     ┆ 25.5795 ┆ 8             ┆ http://sports.core.api.espn.co… │
    └──────────┴─────────┴───────────────┴─────────────────────────────────┘



And [`espn_wbb_season_powerindex_leaders`](../wbb/reference/core.md#espn_wbb_season_powerindex_leaders) lists the category leaders — who tops BPI, strength-of-schedule, strength-of-record, and more.


```python
spi_leaders = safe('BPI category leaders', lambda: wbb.espn_wbb_season_powerindex_leaders(season=SEASON))
(spi_leaders.select(['name', 'display_name']).head(10)
 if has_rows(spi_leaders) else 'BPI leaders unavailable')
```

    ✅ BPI category leaders





    shape: (9, 2)
    ┌───────────────────────┬─────────────────────┐
    │ name                  ┆ display_name        │
    │ ---                   ┆ ---                 │
    │ str                   ┆ str                 │
    ╞═══════════════════════╪═════════════════════╡
    │ bpi                   ┆ BPI Leader          │
    │ rpirank               ┆ NCAAM RPI Leader    │
    │ sospast               ┆ SOS Leader          │
    │ sor                   ┆ SOR Leader          │
    │ bpioffense            ┆ BPI Off Leader      │
    │ bpidefense            ┆ BPI Def Leader      │
    │ bpisevendaychangerank ┆ 7-Day RK CHG Leader │
    │ top50bpiwins          ┆ Most Quality Wins   │
    │ sosoutofconfpast      ┆ Non-Conf SOS Leader │
    └───────────────────────┴─────────────────────┘



## 🏆 Standings & conferences

[`espn_wbb_standings`](../wbb/reference/site.md#espn_wbb_standings) returns one wide row per team — records, win %, points for/against, **and** conference membership. [`espn_wbb_conferences`](../wbb/reference/site.md#espn_wbb_conferences) lists the conference groups with their `group_id`s (handy for filtering).


```python
standings = safe('2025 standings', lambda: wbb.espn_wbb_standings(season=SEASON))
(standings.select(['team_display_name', 'conference_abbreviation', 'wins', 'losses', 'win_percent', 'points_for', 'points_against'])
          .sort('win_percent', descending=True, nulls_last=True).head(10)
 if has_rows(standings) else 'standings unavailable')
```

    ✅ 2025 standings





    shape: (10, 7)
    ┌───────────────────┬──────────────────┬──────┬────────┬─────────────┬────────────┬────────────────┐
    │ team_display_name ┆ conference_abbre ┆ wins ┆ losses ┆ win_percent ┆ points_for ┆ points_against │
    │ ---               ┆ viation          ┆ ---  ┆ ---    ┆ ---         ┆ ---        ┆ ---            │
    │ str               ┆ ---              ┆ i64  ┆ i64    ┆ f64         ┆ f64        ┆ f64            │
    │                   ┆ str              ┆      ┆        ┆             ┆            ┆                │
    ╞═══════════════════╪══════════════════╪══════╪════════╪═════════════╪════════════╪════════════════╡
    │ Florida Gulf      ┆ ASUN             ┆ 18   ┆ 0      ┆ 1.0         ┆ 1367.0     ┆ 983.0          │
    │ Coast Eagles      ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ UConn Huskies     ┆ bige             ┆ 18   ┆ 0      ┆ 1.0         ┆ 1480.0     ┆ 866.0          │
    │ Norfolk State     ┆ meac             ┆ 14   ┆ 0      ┆ 1.0         ┆ 1145.0     ┆ 747.0          │
    │ Spartans          ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ Fairleigh         ┆ neast            ┆ 16   ┆ 0      ┆ 1.0         ┆ 1086.0     ┆ 805.0          │
    │ Dickinson Knights ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ South Dakota      ┆ summ             ┆ 16   ┆ 0      ┆ 1.0         ┆ 1258.0     ┆ 933.0          │
    │ State Jackrabbits ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ James Madison     ┆ belt             ┆ 18   ┆ 0      ┆ 1.0         ┆ 1358.0     ┆ 1072.0         │
    │ Dukes             ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ Grand Canyon      ┆ wac              ┆ 16   ┆ 0      ┆ 1.0         ┆ 1219.0     ┆ 894.0          │
    │ Lopes             ┆                  ┆      ┆        ┆             ┆            ┆                │
    │ Green Bay Phoenix ┆ hor              ┆ 19   ┆ 1      ┆ 0.95        ┆ 1408.0     ┆ 1037.0         │
    │ Fairfield Stags   ┆ maac             ┆ 19   ┆ 1      ┆ 0.95        ┆ 1498.0     ┆ 1034.0         │
    │ SE Louisiana Lady ┆ land             ┆ 19   ┆ 1      ┆ 0.95        ┆ 1330.0     ┆ 1013.0         │
    │ Lions             ┆                  ┆      ┆        ┆             ┆            ┆                │
    └───────────────────┴──────────────────┴──────┴────────┴─────────────┴────────────┴────────────────┘




```python
conferences = safe('conferences', wbb.espn_wbb_conferences)
(conferences.select(['group_id', 'name', 'abbreviation', 'short_name']).head(12)
 if has_rows(conferences) else 'conferences unavailable')
```

    ✅ conferences





    shape: (12, 4)
    ┌──────────┬───────────────────────────┬──────────────┬────────────┐
    │ group_id ┆ name                      ┆ abbreviation ┆ short_name │
    │ ---      ┆ ---                       ┆ ---          ┆ ---        │
    │ str      ┆ str                       ┆ str          ┆ str        │
    ╞══════════╪═══════════════════════════╪══════════════╪════════════╡
    │ null     ┆ NCAA Division I           ┆ NCAA         ┆ null       │
    │ null     ┆ America East Conference   ┆ aeast        ┆ null       │
    │ null     ┆ American Conference       ┆ American     ┆ null       │
    │ null     ┆ Atlantic 10 Conference    ┆ atl10        ┆ null       │
    │ null     ┆ Atlantic Coast Conference ┆ acc          ┆ null       │
    │ …        ┆ …                         ┆ …            ┆ …          │
    │ null     ┆ Big East Conference       ┆ bige         ┆ null       │
    │ null     ┆ Big Sky Conference        ┆ bsky         ┆ null       │
    │ null     ┆ Big South Conference      ┆ bsou         ┆ null       │
    │ null     ┆ Big Ten Conference        ┆ big10        ┆ null       │
    │ null     ┆ Big West Conference       ┆ bigw         ┆ null       │
    └──────────┴───────────────────────────┴──────────────┴────────────┘



## 🍳 Cookbook: common WBB tasks

Now the fun part — real tasks you'll reach for constantly, each built on the premium functions above. The `load_wbb_*` loaders below read pre-built parquet releases from [wehoop-wbb-data](https://github.com/sportsdataverse/wehoop-wbb-data), so they're fast and reliable year-round. We base most season-wide recipes on **2024** because that release is fully published; swap the season once newer parquet drops.

First, pull the three season-long parquet releases we'll lean on across the
Cookbook — player boxscores, team boxscores, and play-by-play for 2024. One
load, many recipes.


```python
player_box = wbb.load_wbb_player_boxscore(seasons=[2024])
team_box = wbb.load_wbb_team_boxscore(seasons=[2024])
season_pbp = wbb.load_wbb_pbp(seasons=[2024])
print('player_box:', player_box.shape, '| team_box:', team_box.shape, '| pbp:', season_pbp.shape)
```

    player_box: (167412, 55) | team_box: (11796, 56) | pbp: (1908679, 61)


### Recipe 1 — Win-probability ride of a championship 📈

[`espn_wbb_game_probabilities`](../wbb/reference/core.md#espn_wbb_game_probabilities) returns ESPN's play-by-play win-probability snapshots for a game. Let's watch how UConn's win odds evolved through the 2025 title game (event `401746075`).


```python
wp = safe('win probability', lambda: wbb.espn_wbb_game_probabilities(event_id=401746075))
if has_rows(wp):
    ride = wp.select(['sequence_number', 'home_win_percentage', 'away_win_percentage', 'tie_percentage'])
    print('snapshots:', ride.height,
          '| opening home win%:', round(float(ride['home_win_percentage'][0]) * 100, 1),
          '| final home win%:', round(float(ride['home_win_percentage'][-1]) * 100, 1))
    out = ride.head(6)
else:
    out = 'win probability unavailable'
out
```

    ✅ win probability
    snapshots: 300 | opening home win%: 42.9 | final home win%: 2.1





    shape: (6, 4)
    ┌─────────────────┬─────────────────────┬─────────────────────┬────────────────┐
    │ sequence_number ┆ home_win_percentage ┆ away_win_percentage ┆ tie_percentage │
    │ ---             ┆ ---                 ┆ ---                 ┆ ---            │
    │ str             ┆ f64                 ┆ f64                 ┆ f64            │
    ╞═════════════════╪═════════════════════╪═════════════════════╪════════════════╡
    │ 113521784       ┆ 0.429               ┆ 0.571               ┆ 0.0            │
    │ 113521785       ┆ 0.416               ┆ 0.584               ┆ 0.0            │
    │ 113521801       ┆ 0.468               ┆ 0.532               ┆ 0.0            │
    │ 113521802       ┆ 0.473               ┆ 0.527               ┆ 0.0            │
    │ 113521803       ┆ 0.514               ┆ 0.486               ┆ 0.0            │
    │ 113521804       ┆ 0.474               ┆ 0.526               ┆ 0.0            │
    └─────────────────┴─────────────────────┴─────────────────────┴────────────────┘



### Recipe 2 — BPI matchup preview for a game 🔮

[`espn_wbb_game_predictor`](../wbb/reference/core.md#espn_wbb_game_predictor) gives ESPN's BPI-based projection for a single game — matchup quality, projected game score, and each side's predicted point total. Here's the championship preview.


```python
pred = safe('game predictor (BPI)', lambda: wbb.espn_wbb_game_predictor(event_id=401746075))
if has_rows(pred):
    home_stats = pred['home_team_statistics'][0]
    if isinstance(home_stats, str):  # arrives as a Python-repr string
        home_stats = ast.literal_eval(home_stats)
    preview = pl.DataFrame([
        {'stat': s.get('displayName'), 'value': s.get('displayValue')}
        for s in home_stats if isinstance(s, dict)
    ])
    out = preview.head(10)
else:
    out = 'predictor unavailable'
out
```

    ✅ game predictor (BPI)





    shape: (8, 2)
    ┌───────────────────┬───────┐
    │ stat              ┆ value │
    │ ---               ┆ ---   │
    │ str               ┆ str   │
    ╞═══════════════════╪═══════╡
    │ MATCHUP QUALITY   ┆ 99.0  │
    │ GAME SCORE        ┆       │
    │ WIN PROB          ┆ 42.9% │
    │ PRED PT DIFF      ┆ -1.9  │
    │ OPPONENT WIN PROB ┆ 57.1% │
    │ WIN PROB          ┆ 42.9  │
    │ OPPONENT WIN PROB ┆ 57.1  │
    │ null              ┆ 0.0   │
    └───────────────────┴───────┘



### Recipe 3 — Top scorers of a full season 🥇

Take the season-long player boxscore and aggregate with polars to find the highest per-game scorers (min. 20 games).


```python
top_scorers = (
    player_box
    .group_by(['athlete_id', 'athlete_display_name', 'team_short_display_name'])
    .agg(
        games=pl.len(),
        total_points=pl.col('points').sum(),
        ppg=pl.col('points').mean().round(1),
    )
    .filter(pl.col('games') >= 20)
    .sort('ppg', descending=True)
    .head(10)
)
top_scorers
```




    shape: (10, 6)
    ┌────────────┬──────────────────────┬─────────────────────────┬───────┬──────────────┬──────┐
    │ athlete_id ┆ athlete_display_name ┆ team_short_display_name ┆ games ┆ total_points ┆ ppg  │
    │ ---        ┆ ---                  ┆ ---                     ┆ ---   ┆ ---          ┆ ---  │
    │ i32        ┆ str                  ┆ str                     ┆ u32   ┆ i32          ┆ f64  │
    ╞════════════╪══════════════════════╪═════════════════════════╪═══════╪══════════════╪══════╡
    │ 5177257    ┆ Charlotte Climenhage ┆ Nevada                  ┆ 32    ┆ 0            ┆ null │
    │ 4899637    ┆ Caroline de Klauman  ┆ Manhattan               ┆ 30    ┆ 0            ┆ null │
    │ 5175868    ┆ Montaya Dew          ┆ Arizona                 ┆ 34    ┆ 0            ┆ null │
    │ 5105722    ┆ Jaylah Robinson      ┆ Arizona St              ┆ 31    ┆ 0            ┆ null │
    │ 5175847    ┆ Emma Larios          ┆ High Point              ┆ 32    ┆ 0            ┆ null │
    │ 5109401    ┆ Tiara Bellamy        ┆ Saint Peter's           ┆ 30    ┆ 0            ┆ null │
    │ 4705035    ┆ Diamond Hunter       ┆ Southern                ┆ 30    ┆ 0            ┆ null │
    │ 4704165    ┆ Lindsey Syrek        ┆ NJIT                    ┆ 30    ┆ 0            ┆ null │
    │ 4594867    ┆ Margarita Satini     ┆ Weber St                ┆ 33    ┆ 0            ┆ null │
    │ 5175569    ┆ Crystianna Whitehead ┆ AR-Pine Bluff           ┆ 33    ┆ 0            ┆ null │
    └────────────┴──────────────────────┴─────────────────────────┴───────┴──────────────┴──────┘



### Recipe 4 — Best scoring offenses, joined to records 🤝

Aggregate the team boxscore to rank programs by points per game, then attach each team's W-L from the live standings.


```python
offense = (
    team_box
    .group_by(['team_id', 'team_display_name'])
    .agg(games=pl.len(), ppg=pl.col('team_score').mean().round(1))
    .filter(pl.col('games') >= 20)
    .sort('ppg', descending=True)
    .head(10)
)
if has_rows(standings):
    recs = standings.select(['team_id', 'wins', 'losses']).with_columns(pl.col('team_id').cast(pl.Int64, strict=False))
    offense = offense.with_columns(pl.col('team_id').cast(pl.Int64, strict=False)).join(recs, on='team_id', how='left')
offense
```




    shape: (10, 6)
    ┌─────────┬────────────────────────────┬───────┬──────┬──────┬────────┐
    │ team_id ┆ team_display_name          ┆ games ┆ ppg  ┆ wins ┆ losses │
    │ ---     ┆ ---                        ┆ ---   ┆ ---  ┆ ---  ┆ ---    │
    │ i64     ┆ str                        ┆ u32   ┆ f64  ┆ i64  ┆ i64    │
    ╞═════════╪════════════════════════════╪═══════╪══════╪══════╪════════╡
    │ 2294    ┆ Iowa Hawkeyes              ┆ 39    ┆ 91.0 ┆ 10   ┆ 8      │
    │ 99      ┆ LSU Tigers                 ┆ 37    ┆ 85.9 ┆ 12   ┆ 4      │
    │ 2579    ┆ South Carolina Gamecocks   ┆ 38    ┆ 85.4 ┆ 15   ┆ 1      │
    │ 276     ┆ Marshall Thundering Herd   ┆ 33    ┆ 85.3 ┆ 6    ┆ 12     │
    │ 93      ┆ Murray State Racers        ┆ 32    ┆ 84.5 ┆ 16   ┆ 4      │
    │ 127     ┆ Michigan State Spartans    ┆ 31    ┆ 82.8 ┆ 11   ┆ 7      │
    │ 213     ┆ Penn State Lady Lions      ┆ 35    ┆ 82.7 ┆ 1    ┆ 17     │
    │ 198     ┆ Oral Roberts Golden Eagles ┆ 32    ┆ 82.1 ┆ 12   ┆ 4      │
    │ 2181    ┆ Drake Bulldogs             ┆ 35    ┆ 81.2 ┆ 15   ┆ 5      │
    │ 2653    ┆ Troy Trojans               ┆ 34    ┆ 80.9 ┆ 13   ┆ 5      │
    └─────────┴────────────────────────────┴───────┴──────┴──────┴────────┘



### Recipe 5 — A program's full season slate 🗓️

[`espn_wbb_team_schedule`](../wbb/reference/site.md#espn_wbb_team_schedule) returns one program's complete season — every game with its date, matchup short name, and season type. Here's UConn's 2024-25 road to the title (`team_id=2509`).


```python
tsched = safe('UConn team schedule', lambda: wbb.espn_wbb_team_schedule(team_id=2509, season=SEASON))
if has_rows(tsched):
    keep = ['id', 'date', 'short_name', 'season_type_name', 'week_text']
    out = tsched.select([c for c in keep if c in tsched.columns]).head(12)
    print('games on the slate:', tsched.height)
else:
    out = 'team schedule unavailable (offseason) — try during the season'
out
```

    ✅ UConn team schedule
    games on the slate: 29





    shape: (12, 5)
    ┌───────────┬───────────────────┬────────────┬──────────────────┬───────────┐
    │ id        ┆ date              ┆ short_name ┆ season_type_name ┆ week_text │
    │ ---       ┆ ---               ┆ ---        ┆ ---              ┆ ---       │
    │ str       ┆ str               ┆ str        ┆ str              ┆ str       │
    ╞═══════════╪═══════════════════╪════════════╪══════════════════╪═══════════╡
    │ 401713616 ┆ 2024-11-07T00:00Z ┆ PFW @ PUR  ┆ Regular Season   ┆ Week 1    │
    │ 401703046 ┆ 2024-11-11T00:00Z ┆ ND @ PUR   ┆ Regular Season   ┆ Week 1    │
    │ 401713617 ┆ 2024-11-15T00:30Z ┆ IUIN @ PUR ┆ Regular Season   ┆ Week 2    │
    │ 401713618 ┆ 2024-11-19T00:00Z ┆ BELL @ PUR ┆ Regular Season   ┆ Week 3    │
    │ 401713619 ┆ 2024-11-24T18:00Z ┆ UTA @ PUR  ┆ Regular Season   ┆ Week 3    │
    │ …         ┆ …                 ┆ …          ┆ …                ┆ …         │
    │ 401713620 ┆ 2024-12-05T00:00Z ┆ ME @ PUR   ┆ Regular Season   ┆ Week 5    │
    │ 401721485 ┆ 2024-12-07T19:00Z ┆ MD @ PUR   ┆ Regular Season   ┆ Week 5    │
    │ 401703049 ┆ 2024-12-14T22:00Z ┆ UK @ PUR   ┆ Regular Season   ┆ Week 6    │
    │ 401713615 ┆ 2024-12-18T00:00Z ┆ PUR @ M-OH ┆ Regular Season   ┆ Week 7    │
    │ 401713621 ┆ 2024-12-21T19:00Z ┆ INST @ PUR ┆ Regular Season   ┆ Week 7    │
    └───────────┴───────────────────┴────────────┴──────────────────┴───────────┘



### Recipe 6 — Deadliest three-point shooting teams 🎯

Roll the team boxscore up to season totals and compute each program's three-point percentage. Made ÷ attempted, sorted, min. 20 games.


```python
three_pt = (
    team_box
    .group_by(['team_id', 'team_display_name'])
    .agg(
        games=pl.len(),
        tpm=pl.col('three_point_field_goals_made').sum(),
        tpa=pl.col('three_point_field_goals_attempted').sum(),
    )
    .filter((pl.col('games') >= 20) & (pl.col('tpa') > 0))
    .with_columns((pl.col('tpm') / pl.col('tpa') * 100).round(1).alias('three_pct'))
    .sort('three_pct', descending=True)
    .head(10)
)
three_pt
```




    shape: (10, 6)
    ┌─────────┬────────────────────────────────┬───────┬─────┬──────┬───────────┐
    │ team_id ┆ team_display_name              ┆ games ┆ tpm ┆ tpa  ┆ three_pct │
    │ ---     ┆ ---                            ┆ ---   ┆ --- ┆ ---  ┆ ---       │
    │ i32     ┆ str                            ┆ u32   ┆ i32 ┆ i32  ┆ f64       │
    ╞═════════╪════════════════════════════════╪═══════╪═════╪══════╪═══════════╡
    │ 84      ┆ Indiana Hoosiers               ┆ 32    ┆ 268 ┆ 677  ┆ 39.6      │
    │ 2250    ┆ Gonzaga Bulldogs               ┆ 36    ┆ 336 ┆ 849  ┆ 39.6      │
    │ 2579    ┆ South Carolina Gamecocks       ┆ 38    ┆ 253 ┆ 640  ┆ 39.5      │
    │ 149     ┆ Montana Lady Griz              ┆ 33    ┆ 357 ┆ 927  ┆ 38.5      │
    │ 66      ┆ Iowa State Cyclones            ┆ 33    ┆ 285 ┆ 745  ┆ 38.3      │
    │ 2086    ┆ Butler Bulldogs                ┆ 32    ┆ 266 ┆ 694  ┆ 38.3      │
    │ 2571    ┆ South Dakota State Jackrabbits ┆ 33    ┆ 223 ┆ 586  ┆ 38.1      │
    │ 257     ┆ Richmond Spiders               ┆ 35    ┆ 320 ┆ 840  ┆ 38.1      │
    │ 2294    ┆ Iowa Hawkeyes                  ┆ 39    ┆ 426 ┆ 1132 ┆ 37.6      │
    │ 127     ┆ Michigan State Spartans        ┆ 31    ┆ 283 ┆ 757  ┆ 37.4      │
    └─────────┴────────────────────────────────┴───────┴─────┴──────┴───────────┘



### Recipe 7 — Clutch shot-makers ⏱️

Slice the season-long play-by-play to scoring plays in the **final two minutes of the 4th quarter (or overtime)**, total each player's clutch points, and name them via the player boxscore. Pure ice in the veins.


```python
name_lookup = player_box.select(
    ['athlete_id', 'athlete_display_name', 'team_short_display_name']
).unique(subset=['athlete_id'])

clutch = (
    season_pbp
    .filter(
        (pl.col('period_number') >= 4)
        & (pl.col('scoring_play') == True)
        & (pl.col('start_game_seconds_remaining') <= 120)
        & pl.col('athlete_id_1').is_not_null()
    )
    .group_by('athlete_id_1')
    .agg(clutch_points=pl.col('score_value').sum(), clutch_plays=pl.len())
    .rename({'athlete_id_1': 'athlete_id'})
    .join(name_lookup, on='athlete_id', how='left')
    .sort('clutch_points', descending=True)
    .select(['athlete_display_name', 'team_short_display_name', 'clutch_plays', 'clutch_points'])
    .head(10)
)
clutch
```




    shape: (10, 4)
    ┌──────────────────────┬─────────────────────────┬──────────────┬───────────────┐
    │ athlete_display_name ┆ team_short_display_name ┆ clutch_plays ┆ clutch_points │
    │ ---                  ┆ ---                     ┆ ---          ┆ ---           │
    │ str                  ┆ str                     ┆ u32          ┆ i32           │
    ╞══════════════════════╪═════════════════════════╪══════════════╪═══════════════╡
    │ Maya Wong            ┆ Illinois St             ┆ 63           ┆ 81            │
    │ Dyaisha Fair         ┆ Syracuse                ┆ 59           ┆ 80            │
    │ Jada Guinn           ┆ Chattanooga             ┆ 63           ┆ 77            │
    │ Alyssa Fisher        ┆ Loyola Chicago          ┆ 38           ┆ 71            │
    │ Daisha Bradford      ┆ UL Monroe               ┆ 51           ┆ 68            │
    │ Ta'Niya Latson       ┆ Florida St              ┆ 47           ┆ 65            │
    │ Cheyenne Stubbs      ┆ Utah State              ┆ 36           ┆ 62            │
    │ Chellia Watson       ┆ Buffalo                 ┆ 38           ┆ 61            │
    │ Deja Kelly           ┆ North Carolina          ┆ 51           ┆ 59            │
    │ Lucy Olsen           ┆ Villanova               ┆ 39           ┆ 59            │
    └──────────────────────┴─────────────────────────┴──────────────┴───────────────┘



### Recipe 8 — Where the buckets come from (shot-zone mix) 🗺️

The play-by-play carries `coordinate_x` / `coordinate_y` for shots and a `score_value` (2 or 3). Bucket every made field goal into a zone and see how a season's points break down by shot location.


```python
shot_zones = (
    season_pbp
    .filter(
        (pl.col('scoring_play') == True)
        & (pl.col('score_value') >= 2)
        & pl.col('coordinate_y').is_not_null()
    )
    .with_columns(
        pl.when(pl.col('score_value') == 3).then(pl.lit('3-pointer'))
          .when(pl.col('coordinate_y') <= 8).then(pl.lit('2pt — at the rim'))
          .otherwise(pl.lit('2pt — jumper')).alias('shot_zone')
    )
    .group_by('shot_zone')
    .agg(made_field_goals=pl.len(), points=pl.col('score_value').sum())
    .with_columns(
        (pl.col('made_field_goals') / pl.col('made_field_goals').sum() * 100).round(1).alias('share_pct')
    )
    .sort('made_field_goals', descending=True)
)
shot_zones
```




    shape: (3, 4)
    ┌──────────────────┬──────────────────┬────────┬───────────┐
    │ shot_zone        ┆ made_field_goals ┆ points ┆ share_pct │
    │ ---              ┆ ---              ┆ ---    ┆ ---       │
    │ str              ┆ u32              ┆ i32    ┆ f64       │
    ╞══════════════════╪══════════════════╪════════╪═══════════╡
    │ 2pt — at the rim ┆ 9108             ┆ 18216  ┆ 70.6      │
    │ 3-pointer        ┆ 3314             ┆ 9942   ┆ 25.7      │
    │ 2pt — jumper     ┆ 471              ┆ 942    ┆ 3.7       │
    └──────────────────┴──────────────────┴────────┴───────────┘



### Recipe 9 — Double-double machines 🔄

Flag every player-game with at least two double-digit categories (points / rebounds / assists), then count who racked up the most double-doubles across the season.


```python
dd = (
    player_box
    .with_columns(
        (
            (pl.col('points') >= 10).cast(pl.Int8)
            + (pl.col('rebounds') >= 10).cast(pl.Int8)
            + (pl.col('assists') >= 10).cast(pl.Int8)
        ).alias('double_digit_cats')
    )
    .filter(pl.col('double_digit_cats') >= 2)
    .group_by(['athlete_id', 'athlete_display_name', 'team_short_display_name'])
    .agg(double_doubles=pl.len())
    .sort('double_doubles', descending=True)
    .head(10)
)
dd
```




    shape: (10, 4)
    ┌────────────┬──────────────────────┬─────────────────────────┬────────────────┐
    │ athlete_id ┆ athlete_display_name ┆ team_short_display_name ┆ double_doubles │
    │ ---        ┆ ---                  ┆ ---                     ┆ ---            │
    │ i32        ┆ str                  ┆ str                     ┆ u32            │
    ╞════════════╪══════════════════════╪═════════════════════════╪════════════════╡
    │ 4595746    ┆ Lauren Gustin        ┆ BYU                     ┆ 30             │
    │ 4433402    ┆ Angel Reese          ┆ LSU                     ┆ 27             │
    │ 4705101    ┆ Macy McGlone         ┆ E Illinois              ┆ 26             │
    │ 4433403    ┆ Caitlin Clark        ┆ Iowa                    ┆ 24             │
    │ 4898966    ┆ Adrianna Smith       ┆ Maine                   ┆ 22             │
    │ 4684384    ┆ Aneesah Morrow       ┆ LSU                     ┆ 22             │
    │ 4898391    ┆ Phillipina Kyei      ┆ Oregon                  ┆ 20             │
    │ 4433404    ┆ Cameron Brink        ┆ Stanford                ┆ 20             │
    │ 4899516    ┆ Akasha Davis         ┆ Lamar                   ┆ 20             │
    │ 5108550    ┆ Serah Williams       ┆ Wisconsin               ┆ 20             │
    └────────────┴──────────────────────┴─────────────────────────┴────────────────┘



### Recipe 10 — Find the best defenses (fewest points allowed) 🛡️

Every team boxscore row carries the **opponent's** score, so a single group-by yields points allowed per game. Lowest-scoring opponents = stingiest defenses.


```python
defense = (
    team_box
    .group_by(['team_id', 'team_display_name'])
    .agg(
        games=pl.len(),
        opp_ppg=pl.col('opponent_team_score').mean().round(1),
        own_ppg=pl.col('team_score').mean().round(1),
    )
    .filter(pl.col('games') >= 20)
    .with_columns((pl.col('own_ppg') - pl.col('opp_ppg')).round(1).alias('net_ppg'))
    .sort('opp_ppg')
    .head(10)
)
defense
```




    shape: (10, 6)
    ┌─────────┬───────────────────────────┬───────┬─────────┬─────────┬─────────┐
    │ team_id ┆ team_display_name         ┆ games ┆ opp_ppg ┆ own_ppg ┆ net_ppg │
    │ ---     ┆ ---                       ┆ ---   ┆ ---     ┆ ---     ┆ ---     │
    │ i32     ┆ str                       ┆ u32   ┆ f64     ┆ f64     ┆ f64     │
    ╞═════════╪═══════════════════════════╪═══════╪═════════╪═════════╪═════════╡
    │ 399     ┆ UAlbany Great Danes       ┆ 32    ┆ 51.3    ┆ 60.9    ┆ 9.6     │
    │ 261     ┆ Vermont Catamounts        ┆ 37    ┆ 52.8    ┆ 59.4    ┆ 6.6     │
    │ 2450    ┆ Norfolk State Spartans    ┆ 33    ┆ 53.1    ┆ 67.0    ┆ 13.9    │
    │ 2670    ┆ VCU Rams                  ┆ 32    ┆ 53.2    ┆ 63.3    ┆ 10.1    │
    │ 2603    ┆ Saint Joseph's Hawks      ┆ 34    ┆ 54.5    ┆ 65.3    ┆ 10.8    │
    │ 236     ┆ Chattanooga Mocs          ┆ 33    ┆ 54.5    ┆ 64.2    ┆ 9.7     │
    │ 526     ┆ Florida Gulf Coast Eagles ┆ 34    ┆ 55.0    ┆ 74.9    ┆ 19.9    │
    │ 46      ┆ Georgetown Hoyas          ┆ 35    ┆ 55.1    ┆ 57.9    ┆ 2.8     │
    │ 2097    ┆ Campbell Fighting Camels  ┆ 31    ┆ 55.1    ┆ 60.8    ┆ 5.7     │
    │ 2217    ┆ Fairfield Stags           ┆ 33    ┆ 55.2    ┆ 72.5    ┆ 17.3    │
    └─────────┴───────────────────────────┴───────┴─────────┴─────────┴─────────┘



### Recipe 11 — Rolling form: a team's last 10 games 📊

Filter the team boxscore to one program, sort by date, and take the tail — a quick "how did they finish the year?" view with the scoring margin per game. Here's UConn (`team_id=2509`).


```python
last10 = (
    team_box
    .filter(pl.col('team_id') == 2509)
    .with_columns((pl.col('team_score') - pl.col('opponent_team_score')).alias('margin'))
    .sort('game_date')
    .tail(10)
    .select(['game_date', 'opponent_team_short_display_name', 'team_score', 'opponent_team_score', 'margin'])
)
print('average margin over last 10:', round(last10['margin'].mean(), 1) if last10.height else 'n/a')
last10
```

    average margin over last 10: 1.6





    shape: (10, 5)
    ┌────────────┬─────────────────────────────────┬────────────┬─────────────────────┬────────┐
    │ game_date  ┆ opponent_team_short_display_na… ┆ team_score ┆ opponent_team_score ┆ margin │
    │ ---        ┆ ---                             ┆ ---        ┆ ---                 ┆ ---    │
    │ date       ┆ str                             ┆ i32        ┆ i32                 ┆ i32    │
    ╞════════════╪═════════════════════════════════╪════════════╪═════════════════════╪════════╡
    │ 2024-02-17 ┆ Nebraska                        ┆ 65         ┆ 77                  ┆ -12    │
    │ 2024-02-21 ┆ Michigan St                     ┆ 59         ┆ 68                  ┆ -9     │
    │ 2024-02-25 ┆ Wisconsin                       ┆ 79         ┆ 55                  ┆ 24     │
    │ 2024-02-28 ┆ Penn State                      ┆ 88         ┆ 93                  ┆ -5     │
    │ 2024-03-03 ┆ Michigan                        ┆ 60         ┆ 64                  ┆ -4     │
    │ 2024-03-06 ┆ Northwestern                    ┆ 78         ┆ 72                  ┆ 6      │
    │ 2024-03-07 ┆ Nebraska                        ┆ 56         ┆ 64                  ┆ -8     │
    │ 2024-03-25 ┆ Butler                          ┆ 62         ┆ 51                  ┆ 11     │
    │ 2024-03-28 ┆ Duquesne                        ┆ 71         ┆ 50                  ┆ 21     │
    │ 2024-04-01 ┆ Vermont                         ┆ 59         ┆ 67                  ┆ -8     │
    └────────────┴─────────────────────────────────┴────────────┴─────────────────────┴────────┘



### Recipe 12 — Pandas interop: a season's play-type mix 🐼

Every loader and accessor takes `return_as_pandas=True`. Pull the play-by-play
as pandas, tally the most common play types with a one-liner `value_counts()`,
and you're back in familiar territory for downstream tooling.


```python
pbp_pd = wbb.load_wbb_pbp(seasons=[2024], return_as_pandas=True)
play_mix = (
    pbp_pd['type_text']
    .value_counts()
    .head(10)
    .rename_axis('play_type')
    .reset_index(name='count')
)
play_mix
```




                play_type   count
    0            JumpShot  427608
    1   Defensive Rebound  293368
    2           LayUpShot  250960
    3        PersonalFoul  192322
    4       MadeFreeThrow  188762
    5  Lost Ball Turnover  183859
    6   Offensive Rebound  151143
    7               Steal   89517
    8          Block Shot   36019
    9   OfficialTVTimeOut   24582



## 🎉 Where to go next

- Pass `return_as_pandas=True` to any wrapper for a pandas frame.
- **Premium analytics**: [`espn_wbb_season_powerindex`](../wbb/reference/core.md#espn_wbb_season_powerindex), [`espn_wbb_game_probabilities`](../wbb/reference/core.md#espn_wbb_game_probabilities), and [`espn_wbb_rankings`](../wbb/reference/site.md#espn_wbb_rankings) are the deep cuts.
- **Full reference**: the WBB pages — [core](../wbb/reference/core.md), [site](../wbb/reference/site.md), [web](../wbb/reference/web.md), [additional](../wbb/reference/additional.md), and [loaders](../wbb/reference/loaders.md).
- `dir(sdv.wbb)` shows the full 100+ endpoint surface (player gamelogs, splits, depth charts, transactions, recruits, and more).
- Men's side? See the parallel [`06_mbb_intro.ipynb`](06_mbb_intro.md).
- R user? The same surface lives in [wehoop](https://wehoop.sportsdataverse.org).

Now go find the next national champion! 🏀🏆
