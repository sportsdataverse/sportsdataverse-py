---
title: Other ESPN leagues tutorial
sidebar_label: Other ESPN leagues
sidebar_position: 14
---

# 🏈⚾🏒 Newer ESPN leagues with `sportsdataverse-py`

Spring pro football is back, college diamonds are packed, and NCAA rinks are full. 🎉
In a few lines of Python you're about to pull **live scoreboards**, **schedules**,
**standings**, **team rosters**, and **play-by-play** for **seven leagues**
added to `sportsdataverse-py` in the 0.0.59 ESPN expansion:

| Sport | Leagues |
|---|---|
| Spring / pro football | United Football League (UFL), XFL, Canadian Football League (CFL) |
| College baseball / softball | NCAA Baseball (`college_baseball`), NCAA Softball (`college_softball`) |
| NCAA hockey | Men's College Hockey (`mch`), Women's College Hockey (`wch`) |

Every league rides the same **cross-league ESPN wrapper surface** introduced in
0.0.51 — a single parameterized core (`sportsdataverse._common_espn`) with
thin per-league extension modules. That means **identical function naming** across
all leagues: `espn_{prefix}_scoreboard`, `espn_{prefix}_team_schedule`,
`espn_{prefix}_standings`, `espn_{prefix}_team_roster`, `espn_{prefix}_summary`,
and 80+ more short names per league.

### Namespace note

The canonical import paths are **nested under the sport group**:

```python
from sportsdataverse.football.ufl import espn_ufl_scoreboard
from sportsdataverse.baseball.college_baseball import espn_college_baseball_teams_site
from sportsdataverse.hockey.mch import espn_mch_scoreboard
```

Top-level shortcuts (e.g. `sportsdataverse.ufl`) exist for back-compat but emit a
`DeprecationWarning` — prefer the nested paths shown throughout this notebook.

## 🧰 The toolbox

Every wrapper returns a raw `dict` by default (`return_parsed=False`). Pass
`return_parsed=True` to get a tidy **polars** `DataFrame` where a parser is
registered, or `return_as_pandas=True` for a **pandas** `DataFrame`. The
function surface is identical across all seven leagues below.

| Function pattern | What it gives you |
|---|---|
| `espn_{prefix}_scoreboard(dates=YYYYMMDD)` | Live / historical game slate |
| `espn_{prefix}_team_schedule(team_id=, season=)` | Full team schedule for a season |
| `espn_{prefix}_teams_site()` | All teams in the league (name, abbrev, logo, `team_id`) |
| `espn_{prefix}_standings()` | Conference / division standings table |
| `espn_{prefix}_summary(event_id=)` | Full game summary (~700 KB — box score, plays, leaders) |
| `espn_{prefix}_team_roster(team_id=, season=)` | Roster for one team / season |
| `espn_{prefix}_game_plays(event_id=)` | Play-by-play for one game |
| `espn_{prefix}_leaders()` | League statistical leaders |
| `espn_{prefix}_rankings()` | Polls / rankings (NCAA leagues) |
| `espn_{prefix}_news()` | League news feed |
| `espn_{prefix}_injuries()` | Injury report |
| `espn_{prefix}_player_info(athlete_id=)` | Single-player bio / current info |

## 🔌 Setup

```sh
pip install sportsdataverse
```

No API key required — all seven leagues hit ESPN's public endpoints.


```python
import polars as pl

# Spring / pro football
import sportsdataverse.football.ufl as ufl
import sportsdataverse.football.xfl as xfl
import sportsdataverse.football.cfl as cfl

# College baseball + softball
import sportsdataverse.baseball.college_baseball as cbb
import sportsdataverse.baseball.college_softball as cbs

# NCAA hockey
import sportsdataverse.hockey.mch as mch
import sportsdataverse.hockey.wch as wch

pl.Config.set_tbl_rows(10)
print('sportsdataverse loaded — seven ESPN expansion leagues ready 🚀')
```

    sportsdataverse loaded — seven ESPN expansion leagues ready 🚀


ESPN's live endpoints are seasonal and occasionally rate-limited, so a tiny
`safe()` helper runs each call defensively — you get the result when the feed
is up, and a friendly one-liner when it isn't (never a scary traceback). 🛟

We also pin to a known completed slate for each league so code examples render
consistently whether you run this in-season or in the off-season.


```python
def safe(label, thunk):
    try:
        out = thunk()
        print(f'✅ {label}')
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f'⏭️  {label}: unavailable right now ({type(e).__name__})')
        return None


def _keys(d):
    """Print top-level keys of a raw dict for quick orientation."""
    if isinstance(d, dict):
        print('top-level keys:', list(d.keys())[:15])
    elif d is None:
        print('(no data)')
    else:
        print(type(d))


# Stable completed-season reference dates / ids for each league
UFL_DATE   = 20240525   # UFL Championship Game, May 25 2024
XFL_DATE   = 20230521   # XFL Championship Game, May 21 2023 (last full XFL season)
CFL_DATE   = 20231119   # 111th Grey Cup, Nov 19 2023
CBB_DATE   = 20240624   # NCAA Baseball CWS finals, June 24 2024
CBS_DATE   = 20240606   # NCAA Softball WCWS finals, June 6 2024
MCH_DATE   = 20240413   # Frozen Four semi, April 13 2024
WCH_DATE   = 20240322   # NCAA Women's hockey Frozen Four, March 22 2024

SAMPLE_SEASON = 2024
print('reference constants set')
```

    reference constants set


---

## 🏈 Part 1 — Spring & Pro Football: UFL, XFL, CFL

The United Football League (UFL, formed from the merger of the XFL and USFL),
the XFL, and the Canadian Football League (CFL) are all first-class citizens of
the ESPN Site v2 API surface. Their modules expose the full `_FOOTBALL_WRAPPERS`
set — the same family as `cfb` and `nfl` — so you get deep scoring-play,
power-index, and drive-level detail out of the box.

### 📋 UFL: the scoreboard

[`espn_ufl_scoreboard(dates=YYYYMMDD)`](../football/reference/ufl.md#espn_ufl_scoreboard)
returns the game slate for a specific date. The raw payload is a dict whose
`events` key holds one entry per game with nested team and status objects.
Pass `return_parsed=True` to get a flat polars frame.


```python
# Raw payload — orient yourself with the top-level keys
board_raw = safe('UFL scoreboard (raw)', lambda: ufl.espn_ufl_scoreboard(dates=UFL_DATE))
_keys(board_raw)
```

    ✅ UFL scoreboard (raw)
    <class 'polars.dataframe.frame.DataFrame'>



```python
# Parsed frame — one row per game
board = safe(
    'UFL scoreboard (parsed)',
    lambda: ufl.espn_ufl_scoreboard(dates=UFL_DATE, return_parsed=True),
)
if board is not None and getattr(board, 'height', 0):
    keep = [c for c in board.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    board.select(keep).head() if keep else board.head()
else:
    'scoreboard unavailable for that date'
```

    ✅ UFL scoreboard (parsed)


### 🏫 UFL: teams

[`espn_ufl_teams_site()`](../football/reference/ufl.md#espn_ufl_teams_site)
lists every team with its `team_id` — the key you feed into every
team-scoped call (`espn_ufl_team_schedule`, `espn_ufl_team_roster`, etc.).
There are no required arguments.


```python
teams_ufl = safe('UFL teams', lambda: ufl.espn_ufl_teams_site(return_parsed=True))
if teams_ufl is not None and teams_ufl.height:
    keep = [c for c in teams_ufl.columns
            if c in ('id', 'abbreviation', 'display_name', 'location',
                     'color', 'alternate_color')]
    teams_ufl.select(keep).head(10) if keep else teams_ufl.head(10)
else:
    'teams unavailable'
```

    ✅ UFL teams


### 📊 UFL: standings

[`espn_ufl_standings()`](../football/reference/ufl.md#espn_ufl_standings)
returns the conference / division standings table. Pair with the
team `id` from the teams frame to build a standings dashboard.


```python
standings_ufl = safe('UFL standings', lambda: ufl.espn_ufl_standings(return_parsed=True))
if standings_ufl is not None and standings_ufl.height:
    keep = [c for c in standings_ufl.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'win_percent', 'points_for', 'points_against')]
    standings_ufl.select(keep).head(8) if keep else standings_ufl.head(8)
else:
    'standings unavailable'
```

    ✅ UFL standings


### 📅 UFL: team schedule

[`espn_ufl_team_schedule(team_id=, season=)`](../football/reference/ufl.md#espn_ufl_team_schedule)
returns one row per game for a single team's season — useful for building
a results table or joining standings context onto a game-log.


```python
# team_id=10000 is the Michigan Panthers (2024 UFL champions)
schedule_ufl = safe(
    'UFL team schedule',
    lambda: ufl.espn_ufl_team_schedule(team_id=10000, season=SAMPLE_SEASON,
                                       return_parsed=True),
)
if schedule_ufl is not None and schedule_ufl.height:
    keep = [c for c in schedule_ufl.columns
            if c in ('id', 'week', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    schedule_ufl.select(keep).head() if keep else schedule_ufl.head()
else:
    'schedule unavailable'
```

    ✅ UFL team schedule


### 🏟️ UFL: game summary & play-by-play

[`espn_ufl_summary(event_id=)`](../football/reference/ufl.md#espn_ufl_summary)
returns the full ~700 KB Site v2 summary payload — box score, plays, drives,
scoring plays, leaders, odds, and more. You can extract any sub-frame directly
from the raw dict, or use the `parse_summary` parser (see the architecture
note at the end of this notebook).

`espn_ufl_game_plays(event_id=)` returns a lightweight play list for the same
game without the full summary overhead.


```python
# 2024 UFL Championship game — find the event_id from the scoreboard above
# or look it up via espn_ufl_scoreboard(dates=UFL_DATE)
UFL_CHAMPIONSHIP_ID = 401671648

summary_ufl = safe(
    'UFL game summary (raw)',
    lambda: ufl.espn_ufl_summary(event_id=UFL_CHAMPIONSHIP_ID),
)
_keys(summary_ufl)
```

    ⏭️  UFL game summary (raw): unavailable right now (NoESPNDataError)
    (no data)



```python
# Quick play-by-play via the lightweight plays endpoint
plays_ufl = safe(
    'UFL game plays (raw)',
    lambda: ufl.espn_ufl_game_plays(event_id=UFL_CHAMPIONSHIP_ID),
)
_keys(plays_ufl)
```

    ⏭️  UFL game plays (raw): unavailable right now (NoESPNDataError)
    (no data)


---

### 🏈 XFL: scoreboard, teams & standings

The XFL module (`sportsdataverse.football.xfl`) has the same shape.
The 2023 season is the most recent fully archived XFL slate before the
merger into the UFL.


```python
# XFL teams — good cross-check since some franchises carried over to UFL
teams_xfl = safe('XFL teams', lambda: xfl.espn_xfl_teams_site(return_parsed=True))
if teams_xfl is not None and teams_xfl.height:
    keep = [c for c in teams_xfl.columns
            if c in ('id', 'abbreviation', 'display_name', 'location')]
    teams_xfl.select(keep).head(10) if keep else teams_xfl.head(10)
else:
    'XFL teams unavailable'
```

    ✅ XFL teams



```python
# XFL championship scoreboard — May 2023
board_xfl = safe(
    'XFL scoreboard (parsed)',
    lambda: xfl.espn_xfl_scoreboard(dates=XFL_DATE, return_parsed=True),
)
if board_xfl is not None and getattr(board_xfl, 'height', 0):
    keep = [c for c in board_xfl.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    board_xfl.select(keep).head() if keep else board_xfl.head()
else:
    'XFL scoreboard unavailable'
```

    ✅ XFL scoreboard (parsed)



```python
# XFL standings (2023)
standings_xfl = safe('XFL standings', lambda: xfl.espn_xfl_standings(return_parsed=True))
if standings_xfl is not None and standings_xfl.height:
    keep = [c for c in standings_xfl.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'win_percent', 'points_for', 'points_against')]
    standings_xfl.select(keep).head(8) if keep else standings_xfl.head(8)
else:
    'XFL standings unavailable'
```

    ✅ XFL standings


---

### 🍁 CFL: scoreboard, teams, standings & schedule

The Canadian Football League runs a 9-team league with a 18-game regular season
capped by the **Grey Cup** in November. The `cfl` module exposes the full
football wrapper set, including conference/division standings and the
power-index endpoints.


```python
# CFL Grey Cup 2023 scoreboard
board_cfl = safe(
    'CFL scoreboard (parsed)',
    lambda: cfl.espn_cfl_scoreboard(dates=CFL_DATE, return_parsed=True),
)
if board_cfl is not None and getattr(board_cfl, 'height', 0):
    keep = [c for c in board_cfl.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    board_cfl.select(keep).head() if keep else board_cfl.head()
else:
    'CFL scoreboard unavailable'
```

    ✅ CFL scoreboard (parsed)



```python
# CFL teams
teams_cfl = safe('CFL teams', lambda: cfl.espn_cfl_teams_site(return_parsed=True))
if teams_cfl is not None and teams_cfl.height:
    keep = [c for c in teams_cfl.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    teams_cfl.select(keep).head(10) if keep else teams_cfl.head(10)
else:
    'CFL teams unavailable'
```

    ✅ CFL teams



```python
# CFL standings (2023)
standings_cfl = safe('CFL standings', lambda: cfl.espn_cfl_standings(return_parsed=True))
if standings_cfl is not None and standings_cfl.height:
    keep = [c for c in standings_cfl.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'win_percent', 'points_for', 'points_against')]
    standings_cfl.select(keep).head(9) if keep else standings_cfl.head(9)
else:
    'CFL standings unavailable'
```

    ✅ CFL standings



```python
# CFL team schedule — Winnipeg Blue Bombers (team_id=100) 2023 season
schedule_cfl = safe(
    'CFL team schedule',
    lambda: cfl.espn_cfl_team_schedule(team_id=100, season=2023, return_parsed=True),
)
if schedule_cfl is not None and schedule_cfl.height:
    keep = [c for c in schedule_cfl.columns
            if c in ('id', 'week', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    schedule_cfl.select(keep).head(8) if keep else schedule_cfl.head(8)
else:
    'CFL schedule unavailable'
```

    ✅ CFL team schedule


---

## ⚾ Part 2 — College Baseball & Softball

NCAA Baseball and NCAA Softball sit under `sportsdataverse.baseball`.
Both modules expose the full `_NCAA_WRAPPERS` set — which adds
`espn_{prefix}_rankings` and `espn_{prefix}_season_recruits` on top of
the universal surface — giving you AP-style polls and recruiting boards
alongside the standard scoreboard/schedule/teams/standings family.

### ⚾ College baseball: scoreboard & teams

[`espn_college_baseball_scoreboard(dates=YYYYMMDD)`](../baseball/reference/college_baseball.md#espn_college_baseball_scoreboard)
returns the game slate for a date. The College World Series in June is peak
traffic, so games should be available; the off-season window (November – January)
returns an empty slate.


```python
# College World Series finals day, 2024
board_cbb = safe(
    'college baseball scoreboard (parsed)',
    lambda: cbb.espn_college_baseball_scoreboard(dates=CBB_DATE, return_parsed=True),
)
if board_cbb is not None and getattr(board_cbb, 'height', 0):
    keep = [c for c in board_cbb.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    board_cbb.select(keep).head() if keep else board_cbb.head()
else:
    'college baseball scoreboard unavailable'
```

    ✅ college baseball scoreboard (parsed)



```python
# All NCAA baseball teams with their team_id
teams_cbb = safe(
    'college baseball teams',
    lambda: cbb.espn_college_baseball_teams_site(return_parsed=True),
)
if teams_cbb is not None and teams_cbb.height:
    keep = [c for c in teams_cbb.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    print('Total teams:', teams_cbb.height)
    teams_cbb.select(keep).head(10) if keep else teams_cbb.head(10)
else:
    'teams unavailable'
```

    ✅ college baseball teams
    Total teams: 437


### 📊 College baseball: standings & rankings

[`espn_college_baseball_standings()`](../baseball/reference/college_baseball.md#espn_college_baseball_standings)
returns conference standings. [`espn_college_baseball_rankings()`](../baseball/reference/college_baseball.md#espn_college_baseball_rankings)
returns the current AP / USA Today poll — one of the NCAA extras added by the
`_NCAA_WRAPPERS` family.


```python
# Conference standings
standings_cbb = safe(
    'college baseball standings',
    lambda: cbb.espn_college_baseball_standings(return_parsed=True),
)
if standings_cbb is not None and standings_cbb.height:
    keep = [c for c in standings_cbb.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'win_percent')]
    standings_cbb.select(keep).head(10) if keep else standings_cbb.head(10)
else:
    'standings unavailable'
```

    ✅ college baseball standings



```python
# National polls — AP / USA Today baseball rankings
rankings_cbb = safe(
    'college baseball rankings',
    lambda: cbb.espn_college_baseball_rankings(return_parsed=True),
)
if rankings_cbb is not None and rankings_cbb.height:
    keep = [c for c in rankings_cbb.columns
            if c in ('rank', 'team_id', 'team_name', 'points', 'first_place_votes',
                     'wins', 'losses')]
    rankings_cbb.select(keep).head(10) if keep else rankings_cbb.head(10)
else:
    'rankings unavailable (only current during the season)'
```

    ⏭️  college baseball rankings: unavailable right now (NoESPNDataError)


### 📅 College baseball: team schedule

Pick any `team_id` from the teams frame above and pull its full schedule.
Texas (team_id=251) is a perennial CWS contender — a good reference point.


```python
# Texas Longhorns baseball 2024 schedule
schedule_cbb = safe(
    'college baseball team schedule',
    lambda: cbb.espn_college_baseball_team_schedule(
        team_id=251, season=SAMPLE_SEASON, return_parsed=True
    ),
)
if schedule_cbb is not None and schedule_cbb.height:
    keep = [c for c in schedule_cbb.columns
            if c in ('id', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    schedule_cbb.select(keep).head(8) if keep else schedule_cbb.head(8)
else:
    'schedule unavailable'
```

    ✅ college baseball team schedule


### 🥎 College softball: scoreboard & teams

The `college_softball` module mirrors `college_baseball` exactly — same wrapper
family, just a different league slug. The Women's College World Series (WCWS)
wraps up in early June.


```python
# WCWS finals day, June 2024
board_cbs = safe(
    'college softball scoreboard (parsed)',
    lambda: cbs.espn_college_softball_scoreboard(dates=CBS_DATE, return_parsed=True),
)
if board_cbs is not None and getattr(board_cbs, 'height', 0):
    keep = [c for c in board_cbs.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    board_cbs.select(keep).head() if keep else board_cbs.head()
else:
    'college softball scoreboard unavailable'
```

    ✅ college softball scoreboard (parsed)



```python
# College softball teams
teams_cbs = safe(
    'college softball teams',
    lambda: cbs.espn_college_softball_teams_site(return_parsed=True),
)
if teams_cbs is not None and teams_cbs.height:
    keep = [c for c in teams_cbs.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    print('Total teams:', teams_cbs.height)
    teams_cbs.select(keep).head(10) if keep else teams_cbs.head(10)
else:
    'teams unavailable'
```

    ✅ college softball teams
    Total teams: 446



```python
# College softball rankings
rankings_cbs = safe(
    'college softball rankings',
    lambda: cbs.espn_college_softball_rankings(return_parsed=True),
)
if rankings_cbs is not None and rankings_cbs.height:
    keep = [c for c in rankings_cbs.columns
            if c in ('rank', 'team_id', 'team_name', 'points',
                     'first_place_votes', 'wins', 'losses')]
    rankings_cbs.select(keep).head(10) if keep else rankings_cbs.head(10)
else:
    'rankings unavailable (only current during the season)'
```

    ⏭️  college softball rankings: unavailable right now (NoESPNDataError)


### 🥎 College softball: game summary

The same `espn_{prefix}_summary(event_id=)` pattern works for softball.
The raw dict contains a `boxscore` key (team batting/pitching lines),
a `scoringPlays` array, and — for parsed callers — all 21 sub-frames
from the shared `parse_summary` dispatcher.


```python
# 2024 WCWS Championship Game 2 — Oklahoma vs Texas
CBS_CHAMPIONSHIP_ID = 401581278

summary_cbs = safe(
    'college softball game summary (raw)',
    lambda: cbs.espn_college_softball_summary(event_id=CBS_CHAMPIONSHIP_ID),
)
_keys(summary_cbs)
```

    ⏭️  college softball game summary (raw): unavailable right now (NoESPNDataError)
    (no data)


---

## 🏒 Part 3 — NCAA Men's & Women's College Hockey

Men's College Hockey (`mch`) and Women's College Hockey (`wch`) round out the
seven-league expansion. Both expose the full `_NCAA_WRAPPERS` set. The
signature events are the **Frozen Four** in April (men's) and March (women's).

### 🏒 Men's college hockey: scoreboard & teams

[`espn_mch_scoreboard(dates=YYYYMMDD)`](../hockey/reference/mch.md#espn_mch_scoreboard)
returns the game slate. [`espn_mch_teams_site()`](../hockey/reference/mch.md#espn_mch_teams_site)
lists all Division I programs — currently 60 teams.


```python
# Frozen Four semi-final day, April 2024
board_mch = safe(
    'men\'s college hockey scoreboard (parsed)',
    lambda: mch.espn_mch_scoreboard(dates=MCH_DATE, return_parsed=True),
)
if board_mch is not None and getattr(board_mch, 'height', 0):
    keep = [c for c in board_mch.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    board_mch.select(keep).head() if keep else board_mch.head()
else:
    'men\'s college hockey scoreboard unavailable'
```

    ✅ men's college hockey scoreboard (parsed)



```python
# Men's college hockey teams
teams_mch = safe(
    'men\'s college hockey teams',
    lambda: mch.espn_mch_teams_site(return_parsed=True),
)
if teams_mch is not None and teams_mch.height:
    keep = [c for c in teams_mch.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    print('Total D-I men\'s programs:', teams_mch.height)
    teams_mch.select(keep).head(10) if keep else teams_mch.head(10)
else:
    'teams unavailable'
```

    ✅ men's college hockey teams
    Total D-I men's programs: 116


### 📊 Men's college hockey: standings & rankings

Conference standings are the primary puck-nerd query —
which NCHC / Hockey East / Big Ten team leads their division?
The rankings endpoint returns the USCHO / USA Today poll.


```python
# Conference standings
standings_mch = safe(
    "men's college hockey standings",
    lambda: mch.espn_mch_standings(return_parsed=True),
)
if standings_mch is not None and standings_mch.height:
    keep = [c for c in standings_mch.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'ties', 'win_percent')]
    standings_mch.select(keep).head(10) if keep else standings_mch.head(10)
else:
    'standings unavailable'
```

    ✅ men's college hockey standings



```python
# National rankings
rankings_mch = safe(
    "men's college hockey rankings",
    lambda: mch.espn_mch_rankings(return_parsed=True),
)
if rankings_mch is not None and rankings_mch.height:
    keep = [c for c in rankings_mch.columns
            if c in ('rank', 'team_id', 'team_name', 'points',
                     'first_place_votes', 'wins', 'losses', 'ties')]
    rankings_mch.select(keep).head(10) if keep else rankings_mch.head(10)
else:
    'rankings unavailable (only current during the season)'
```

    ✅ men's college hockey rankings


### 📅 Men's college hockey: team schedule & roster

Pull a program's full schedule with
[`espn_mch_team_schedule(team_id=, season=)`](../hockey/reference/mch.md#espn_mch_team_schedule)
and their roster with
[`espn_mch_team_roster(team_id=, season=)`](../hockey/reference/mch.md#espn_mch_team_roster).
Boston University (team_id=103) won the 2024 national title — a good
reference point.


```python
# BU Terriers schedule 2023-24
schedule_mch = safe(
    "men's college hockey team schedule",
    lambda: mch.espn_mch_team_schedule(team_id=103, season=SAMPLE_SEASON,
                                       return_parsed=True),
)
if schedule_mch is not None and schedule_mch.height:
    keep = [c for c in schedule_mch.columns
            if c in ('id', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    schedule_mch.select(keep).head(8) if keep else schedule_mch.head(8)
else:
    'schedule unavailable'
```

    ✅ men's college hockey team schedule



```python
# BU Terriers roster 2023-24
roster_mch = safe(
    "men's college hockey roster",
    lambda: mch.espn_mch_team_roster(team_id=103, season=SAMPLE_SEASON,
                                     return_parsed=True),
)
if roster_mch is not None and roster_mch.height:
    keep = [c for c in roster_mch.columns
            if c in ('id', 'first_name', 'last_name', 'display_name',
                     'position_abbreviation', 'jersey', 'birth_country')]
    roster_mch.select(keep).head(10) if keep else roster_mch.head(10)
else:
    'roster unavailable'
```

    ⏭️  men's college hockey roster: unavailable right now (TypeError)


---

### 👩 Women's college hockey: scoreboard, teams & schedule

The `wch` module is the women's mirror. The NCAA Women's Frozen Four runs
in late March. Wisconsin is the perennial power — 12 national championships.


```python
# NCAA Women's Frozen Four 2024
board_wch = safe(
    "women's college hockey scoreboard (parsed)",
    lambda: wch.espn_wch_scoreboard(dates=WCH_DATE, return_parsed=True),
)
if board_wch is not None and getattr(board_wch, 'height', 0):
    keep = [c for c in board_wch.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    board_wch.select(keep).head() if keep else board_wch.head()
else:
    "women's college hockey scoreboard unavailable"
```

    ✅ women's college hockey scoreboard (parsed)



```python
# Women's college hockey teams
teams_wch = safe(
    "women's college hockey teams",
    lambda: wch.espn_wch_teams_site(return_parsed=True),
)
if teams_wch is not None and teams_wch.height:
    keep = [c for c in teams_wch.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    print("Total women's D-I programs:", teams_wch.height)
    teams_wch.select(keep).head(10) if keep else teams_wch.head(10)
else:
    'teams unavailable'
```

    ✅ women's college hockey teams
    Total women's D-I programs: 47



```python
# Wisconsin Badgers women's hockey schedule 2023-24 (team_id=275)
schedule_wch = safe(
    "women's college hockey team schedule",
    lambda: wch.espn_wch_team_schedule(team_id=275, season=SAMPLE_SEASON,
                                       return_parsed=True),
)
if schedule_wch is not None and schedule_wch.height:
    keep = [c for c in schedule_wch.columns
            if c in ('id', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    schedule_wch.select(keep).head(8) if keep else schedule_wch.head(8)
else:
    'schedule unavailable'
```

    ✅ women's college hockey team schedule



```python
# Women's college hockey rankings
rankings_wch = safe(
    "women's college hockey rankings",
    lambda: wch.espn_wch_rankings(return_parsed=True),
)
if rankings_wch is not None and rankings_wch.height:
    keep = [c for c in rankings_wch.columns
            if c in ('rank', 'team_id', 'team_name', 'points',
                     'first_place_votes', 'wins', 'losses', 'ties')]
    rankings_wch.select(keep).head(10) if keep else rankings_wch.head(10)
else:
    "rankings unavailable (only current during the season)"
```

    ✅ women's college hockey rankings


---

## 🍳 Cookbook: cross-league recipes

A handful of patterns you'll reach for constantly across all seven leagues.

### Recipe 1 — Collect all teams from every league into one frame 🗂️

All seven modules expose `espn_{prefix}_teams_site()`. Stack them with
`pl.concat` to build a cross-league lookup table.


```python
import importlib

LEAGUE_MODULES = [
    ('ufl',              ufl,  'espn_ufl_teams_site'),
    ('xfl',              xfl,  'espn_xfl_teams_site'),
    ('cfl',              cfl,  'espn_cfl_teams_site'),
    ('college_baseball', cbb,  'espn_college_baseball_teams_site'),
    ('college_softball', cbs,  'espn_college_softball_teams_site'),
    ('mch',              mch,  'espn_mch_teams_site'),
    ('wch',              wch,  'espn_wch_teams_site'),
]

frames = []
for league_name, mod, fn_name in LEAGUE_MODULES:
    fn = getattr(mod, fn_name)
    df = safe(f'{league_name} teams', lambda fn=fn: fn(return_parsed=True))
    if df is not None and df.height:
        # parsed teams frames use team_id / team_display_name column names
        id_col = 'team_id' if 'team_id' in df.columns else 'id' if 'id' in df.columns else df.columns[0]
        name_col = 'team_display_name' if 'team_display_name' in df.columns else 'display_name' if 'display_name' in df.columns else df.columns[1]
        frames.append(
            df.select([id_col, name_col])
              .rename({id_col: 'id', name_col: 'display_name'})
              .with_columns(pl.lit(league_name).alias('league'))
        )

if frames:
    all_teams = pl.concat(frames, how='diagonal_relaxed')
    print('Total teams across all 7 leagues:', all_teams.height)
    all_teams.group_by('league').len().sort('len', descending=True)
else:
    'no team data available right now'
```

    ✅ ufl teams


    ✅ xfl teams


    ✅ cfl teams


    ✅ college_baseball teams


    ✅ college_softball teams
    ✅ mch teams


    ✅ wch teams
    Total teams across all 7 leagues: 1067


### Recipe 2 — Today's slate for any league 📅

Pass `dates=` as `YYYYMMDD` to any scoreboard wrapper. Omitting the argument
returns ESPN's *current* default date (today or the most recent active day),
which is handy during the season.


```python
from datetime import date

today_str = int(date.today().strftime('%Y%m%d'))

# Swap in any module + scoreboard function to check a different league
todays_cfl = safe(
    f"today's CFL slate ({today_str})",
    lambda: cfl.espn_cfl_scoreboard(dates=today_str, return_parsed=True),
)
if todays_cfl is not None and getattr(todays_cfl, 'height', 0):
    print(f"{todays_cfl.height} game(s) today")
    todays_cfl.head()
else:
    'no CFL games today (or offseason)'
```

    ✅ today's CFL slate (20260713)


### Recipe 3 — Player info for any athlete 🧑‍💻

Every league exposes `espn_{prefix}_player_info(athlete_id=)`. Find an
`athlete_id` via the roster endpoint, then pull richer bio / career-stats
detail with `espn_{prefix}_player_career_stats(athlete_id=)`.


```python
# Look up a player from the men's college hockey roster we pulled earlier
# This uses the first athlete_id in the roster frame if it's available
if (
    'roster_mch' in dir()
    and roster_mch is not None
    and roster_mch.height
    and 'id' in roster_mch.columns
):
    first_athlete_id = roster_mch['id'][0]
    player_info = safe(
        f'MCH player info (id={first_athlete_id})',
        lambda: mch.espn_mch_player_info(athlete_id=first_athlete_id),
    )
    _keys(player_info)
else:
    print('roster not loaded — skipping player info example')
```

    roster not loaded — skipping player info example


### Recipe 4 — News & injuries for any league 📰

[`espn_{prefix}_news()`](../football/reference/ufl.md#espn_ufl_news) and
[`espn_{prefix}_injuries()`](../football/reference/ufl.md#espn_ufl_injuries)
need no arguments — just call them for the latest feed. Both return raw
dicts; parsed versions are available where a parser is registered.


```python
# UFL news (current, no date needed)
news_ufl = safe('UFL news', lambda: ufl.espn_ufl_news())
_keys(news_ufl)

# CFL injuries
injuries_cfl = safe('CFL injuries', lambda: cfl.espn_cfl_injuries())
_keys(injuries_cfl)
```

    ✅ UFL news
    <class 'polars.dataframe.frame.DataFrame'>
    ✅ CFL injuries
    <class 'polars.dataframe.frame.DataFrame'>


---

## 🏗️ The shared cross-league architecture

All seven leagues share exactly **one** parameterized core:
`sportsdataverse._common_espn`. The architecture is worth understanding
because it makes every league behave identically:

```
_common_espn.py
  ├── ~80 core functions, each parameterized on (sport, league) slugs
  ├── _UNIVERSAL_WRAPPERS  — scoreboard, teams, standings, schedule, …
  ├── _NCAA_WRAPPERS       — +rankings, +season_recruits  (CBB/CBS/MCH/WCH)
  ├── _FOOTBALL_WRAPPERS   — +drives, +powerindex, +draft  (UFL/XFL/CFL)
  └── make_league_module(sport, league, prefix, namespace)
          └── _bind(core_fn, sport, league)  →  espn_{prefix}_{short}

sportsdataverse/football/ufl.py        # 4 lines — calls make_league_module
sportsdataverse/football/xfl.py        # 4 lines
sportsdataverse/football/cfl.py        # 4 lines
sportsdataverse/baseball/college_baseball.py  # 4 lines
sportsdataverse/baseball/college_softball.py  # 4 lines
sportsdataverse/hockey/mch.py          # 4 lines
sportsdataverse/hockey/wch.py          # 4 lines
```

This means:

- Any bug fix in `_common_espn` applies to all 7 leagues at once.
- Every league has an identical `dir()` surface — the only difference
  is the prefix string (`ufl`, `xfl`, `cfl`, `college_baseball`, …).
- The `return_parsed=True` path routes through `ENDPOINT_PARSERS` — a
  registry mapping each short name to a dedicated parser function (or
  a generic fall-through like `parse_items`, `parse_single_entity`, or
  `parse_summary`). You can always get the raw dict by omitting the kwarg.

**Total wrappers across all eight SDV leagues** (including the 0.0.51 originals
plus the 0.0.59 expansion): **819 registered functions**.

### Listing all functions on any league module


```python
# All espn_ufl_* functions — the same list shape exists for every league
ufl_fns = [f for f in dir(ufl) if f.startswith('espn_')]
print(f'{len(ufl_fns)} functions on sportsdataverse.football.ufl')
print('sample:', ufl_fns[:8], '...')
```

    110 functions on sportsdataverse.football.ufl
    sample: ['espn_ufl_award', 'espn_ufl_awards', 'espn_ufl_calendar', 'espn_ufl_coach', 'espn_ufl_coach_record', 'espn_ufl_coach_season', 'espn_ufl_conferences', 'espn_ufl_draft'] ...



```python
# Quick cross-league count
for name, mod in [('ufl', ufl), ('xfl', xfl), ('cfl', cfl),
                  ('college_baseball', cbb), ('college_softball', cbs),
                  ('mch', mch), ('wch', wch)]:
    n = len([f for f in dir(mod) if f.startswith('espn_')])
    print(f'  {name:20s}: {n} wrappers')
```

      ufl                 : 110 wrappers
      xfl                 : 110 wrappers
      cfl                 : 110 wrappers
      college_baseball    : 116 wrappers
      college_softball    : 116 wrappers
      mch                 : 116 wrappers
      wch                 : 116 wrappers


---

## 🔗 See Also

- **Spring / pro football reference docs:**
  [UFL](../football/reference/ufl.md) ·
  [XFL](../football/reference/xfl.md) ·
  [CFL](../football/reference/cfl.md)
- **College baseball / softball reference docs:**
  [College Baseball](../baseball/reference/college_baseball.md) ·
  [College Softball](../baseball/reference/college_softball.md)
- **NCAA hockey reference docs:**
  [Men's College Hockey](../hockey/reference/mch.md) ·
  [Women's College Hockey](../hockey/reference/wch.md)
- **Cross-league architecture deep-dive:** `docs/docs/architecture/espn-cross-league.md`
- **Parser layer:** `docs/docs/parsers/`
- **Companion notebooks:**
  `02_cfb_intro.ipynb` (CFB — the original football tutorial),
  `07_nhl_intro.ipynb` (NHL — native feed + ESPN fallback),
  `09_mlb_intro.ipynb` (MLB — Stats API + Statcast)
- **R package companions:**
  [cfbfastR](https://cfbfastR.sportsdataverse.org) ·
  [hoopR](https://hoopR.sportsdataverse.org) ·
  [wehoop](https://wehoop.sportsdataverse.org) ·
  [fastRhockey](https://fastRhockey.sportsdataverse.org) ·
  [baseballr](https://baseballr.sportsdataverse.org)
