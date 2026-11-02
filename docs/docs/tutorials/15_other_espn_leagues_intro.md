---
title: Other ESPN leagues tutorial
description: "Spring pro football is back, college diamonds are packed, and NCAA rinks are full."
sidebar_label: Other ESPN leagues
sidebar_position: 14
custom_edit_url: https://github.com/sportsdataverse/sportsdataverse-py/edit/main/examples/notebooks/15_other_espn_leagues_intro.ipynb
---

# 🏈⚾🏒 Newer ESPN leagues with `sportsdataverse-py`

> This page is the executed notebook [`15_other_espn_leagues_intro.ipynb`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/15_other_espn_leagues_intro.ipynb): [download it](https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/main/examples/notebooks/15_other_espn_leagues_intro.ipynb) to run it yourself.

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
0.0.51 — one code generator that writes a concrete `<prefix>_espn_ext.py` module
per league from the same endpoint definitions. That means **identical function naming** across
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
`` to get a tidy **polars** `DataFrame` where a parser is
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
pip install --pre sportsdataverse
```

No API key required — all seven leagues hit ESPN's public endpoints.


```python
import polars as pl
from IPython.display import display

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
from sportsdataverse.errors import AssetFetchError, NoDataError

def safe(label, thunk):
    try:
        out = thunk()
        print(f'✅ {label}')
        return out
    except (NoDataError, AssetFetchError) as e:
        print(f"\u23ed\ufe0f  {label}: {type(e).__name__}: {e}")
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
UFL_DATE   = 20250614   # UFL Championship Game, June 14 2025
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

[`espn_ufl_scoreboard(dates=YYYYMMDD)`](../ufl/reference/site.md#espn_ufl_scoreboard)
returns the game slate for a specific date. The raw payload is a dict whose
`events` key holds one entry per game with nested team and status objects.
Pass `` to get a flat polars frame.


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
    lambda: ufl.espn_ufl_scoreboard(dates=UFL_DATE),
)
if board is not None and getattr(board, 'height', 0):
    keep = [c for c in board.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    display(board.select(keep).head() if keep else board.head())
else:
    print('scoreboard unavailable for that date')
```

    ✅ UFL scoreboard (parsed)



    shape: (1, 5)
    ┌─────────────────────────────────┬────────────┬─────────────────────────┬────────────┬────────────┐
    │ name                            ┆ short_name ┆ status_type_description ┆ home_score ┆ away_score │
    │ ---                             ┆ ---        ┆ ---                     ┆ ---        ┆ ---        │
    │ str                             ┆ str        ┆ str                     ┆ str        ┆ str        │
    ╞═════════════════════════════════╪════════════╪═════════════════════════╪════════════╪════════════╡
    │ D.C. Defenders at Michigan Pan… ┆ DC VS MIC  ┆ Final                   ┆ 34         ┆ 58         │
    └─────────────────────────────────┴────────────┴─────────────────────────┴────────────┴────────────┘


### 🏫 UFL: teams

[`espn_ufl_teams_site()`](../ufl/reference/site.md#espn_ufl_teams_site)
lists every team with its `team_id` — the key you feed into every
team-scoped call (`espn_ufl_team_schedule`, `espn_ufl_team_roster`, etc.).
There are no required arguments.


```python
teams_ufl = safe('UFL teams', lambda: ufl.espn_ufl_teams_site())
if teams_ufl is not None and teams_ufl.height:
    keep = [c for c in teams_ufl.columns
            if c in ('id', 'abbreviation', 'display_name', 'location',
                     'color', 'alternate_color')]
    display(teams_ufl.select(keep).head(10) if keep else teams_ufl.head(10))
else:
    print('teams unavailable')
```

    ✅ UFL teams



    shape: (8, 14)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ team_abbr ┆ team_alte ┆ team_colo ┆ team_disp ┆ … ┆ team_nick ┆ team_shor ┆ team_slug ┆ team_uid │
    │ eviation  ┆ rnate_col ┆ r         ┆ lay_name  ┆   ┆ name      ┆ t_display ┆ ---       ┆ ---      │
    │ ---       ┆ or        ┆ ---       ┆ ---       ┆   ┆ ---       ┆ _name     ┆ str       ┆ str      │
    │ str       ┆ ---       ┆ str       ┆ str       ┆   ┆ str       ┆ ---       ┆           ┆          │
    │           ┆ str       ┆           ┆           ┆   ┆           ┆ str       ┆           ┆          │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ BHAM      ┆ c6b784    ┆ a32035    ┆ Birmingha ┆ … ┆ Stallions ┆ Stallions ┆ birmingha ┆ s:20~l:3 │
    │           ┆           ┆           ┆ m         ┆   ┆           ┆           ┆ m-stallio ┆ 7~t:1260 │
    │           ┆           ┆           ┆ Stallions ┆   ┆           ┆           ┆ ns        ┆ 73       │
    │ CLB       ┆ 69b3e7    ┆ 0c2340    ┆ Columbus  ┆ … ┆ Aviators  ┆ Aviators  ┆ columbus- ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Aviators  ┆   ┆           ┆           ┆ aviators  ┆ 7~t:1322 │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ 61       │
    │ DC        ┆ ffffff    ┆ c8102e    ┆ DC        ┆ … ┆ Defenders ┆ DEFENDERS ┆ dc-defend ┆ s:20~l:3 │
    │           ┆           ┆           ┆ DEFENDERS ┆   ┆           ┆           ┆ ers       ┆ 7~t:1126 │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ 46       │
    │ DAL       ┆ 000000    ┆ 69b3e7    ┆ Dallas    ┆ … ┆ Renegades ┆ Renegades ┆ dallas-re ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Renegades ┆   ┆           ┆           ┆ negades   ┆ 7~t:1126 │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ 47       │
    │ HOU       ┆ e4002b    ┆ 000000    ┆ Houston   ┆ … ┆ Gamblers  ┆ Gamblers  ┆ houston-g ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Gamblers  ┆   ┆           ┆           ┆ amblers   ┆ 7~t:1260 │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ 75       │
    │ LOU       ┆ 97d700    ┆ 06190a    ┆ Louisvill ┆ … ┆ Kings     ┆ Kings     ┆ louisvill ┆ s:20~l:3 │
    │           ┆           ┆           ┆ e Kings   ┆   ┆           ┆           ┆ e-kings   ┆ 7~t:1322 │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ 62       │
    │ ORL       ┆ 252a36    ┆ fe5000    ┆ Orlando   ┆ … ┆ Storm     ┆ Storm     ┆ orlando-s ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Storm     ┆   ┆           ┆           ┆ torm      ┆ 7~t:1322 │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ 63       │
    │ STL       ┆ a2aaad    ┆ 002677    ┆ St. Louis ┆ … ┆ Battlehaw ┆ Battlehaw ┆ st-louis- ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Battlehaw ┆   ┆ ks        ┆ ks        ┆ battlehaw ┆ 7~t:1126 │
    │           ┆           ┆           ┆ ks        ┆   ┆           ┆           ┆ ks        ┆ 51       │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘


### 📊 UFL: standings

[`espn_ufl_standings()`](../ufl/reference/site.md#espn_ufl_standings)
returns the conference / division standings table. Pair with the
team `id` from the teams frame to build a standings dashboard.


```python
standings_ufl = safe('UFL standings', lambda: ufl.espn_ufl_standings())
if standings_ufl is not None and standings_ufl.height:
    keep = [c for c in standings_ufl.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'win_percent', 'points_for', 'points_against')]
    display(standings_ufl.select(keep).head(8) if keep else standings_ufl.head(8))
else:
    print('standings unavailable')
```

    ✅ UFL standings



    shape: (8, 8)
    ┌─────────┬─────────────┬───────────────┬──────┬────────┬─────────────┬────────────┬───────────────┐
    │ team_id ┆ team_name   ┆ team_abbrevia ┆ wins ┆ losses ┆ win_percent ┆ points_for ┆ points_agains │
    │ ---     ┆ ---         ┆ tion          ┆ ---  ┆ ---    ┆ ---         ┆ ---        ┆ t             │
    │ i64     ┆ str         ┆ ---           ┆ f64  ┆ f64    ┆ f64         ┆ f64        ┆ ---           │
    │         ┆             ┆ str           ┆      ┆        ┆             ┆            ┆ f64           │
    ╞═════════╪═════════════╪═══════════════╪══════╪════════╪═════════════╪════════════╪═══════════════╡
    │ 132263  ┆ Storm       ┆ ORL           ┆ 8.0  ┆ 2.0    ┆ 0.8         ┆ 232.0      ┆ 186.0         │
    │ 112651  ┆ Battlehawks ┆ STL           ┆ 6.0  ┆ 4.0    ┆ 0.6         ┆ 212.0      ┆ 197.0         │
    │ 132262  ┆ Kings       ┆ LOU           ┆ 6.0  ┆ 4.0    ┆ 0.6         ┆ 265.0      ┆ 219.0         │
    │ 112646  ┆ Defenders   ┆ DC            ┆ 5.0  ┆ 5.0    ┆ 0.5         ┆ 281.0      ┆ 224.0         │
    │ 126075  ┆ Gamblers    ┆ HOU           ┆ 4.0  ┆ 6.0    ┆ 0.4         ┆ 189.0      ┆ 236.0         │
    │ 126073  ┆ Stallions   ┆ BHAM          ┆ 4.0  ┆ 6.0    ┆ 0.4         ┆ 190.0      ┆ 229.0         │
    │ 112647  ┆ Renegades   ┆ DAL           ┆ 4.0  ┆ 6.0    ┆ 0.4         ┆ 224.0      ┆ 259.0         │
    │ 132261  ┆ Aviators    ┆ CLB           ┆ 3.0  ┆ 7.0    ┆ 0.3         ┆ 216.0      ┆ 259.0         │
    └─────────┴─────────────┴───────────────┴──────┴────────┴─────────────┴────────────┴───────────────┘


### 📅 UFL: team schedule

[`espn_ufl_team_schedule(team_id=, season=)`](../ufl/reference/site.md#espn_ufl_team_schedule)
returns one row per game for a single team's season — useful for building
a results table or joining standings context onto a game-log.


```python
# team_id=112646 is the DC Defenders (2024 UFL season onward)
schedule_ufl = safe(
    'UFL team schedule',
    lambda: ufl.espn_ufl_team_schedule(team_id=112646, season=SAMPLE_SEASON,
                                       ),
)
if schedule_ufl is not None and schedule_ufl.height:
    keep = [c for c in schedule_ufl.columns
            if c in ('id', 'week', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    display(schedule_ufl.select(keep).head() if keep else schedule_ufl.head())
else:
    print('schedule unavailable')
```

    ✅ UFL team schedule



    shape: (5, 3)
    ┌───────────┬─────────────────────────────────┬────────────┐
    │ id        ┆ name                            ┆ short_name │
    │ ---       ┆ ---                             ┆ ---        │
    │ i64       ┆ str                             ┆ str        │
    ╞═══════════╪═════════════════════════════════╪════════════╡
    │ 401638301 ┆ D.C. Defenders at San Antonio … ┆ DC @ SA    │
    │ 401638306 ┆ Houston Roughnecks at D.C. Def… ┆ HOU @ DC   │
    │ 401638307 ┆ D.C. Defenders at Arlington Re… ┆ DC @ ARL   │
    │ 401638312 ┆ D.C. Defenders at Birmingham S… ┆ DC @ BIR   │
    │ 401638317 ┆ St. Louis Battlehawks at D.C. … ┆ STL @ DC   │
    └───────────┴─────────────────────────────────┴────────────┘


### 🏟️ UFL: game summary & play-by-play

[`espn_ufl_summary(event_id=)`](../ufl/reference/site.md#espn_ufl_summary)
returns the full ~700 KB Site v2 summary payload — box score, plays, drives,
scoring plays, leaders, odds, and more. You can extract any sub-frame directly
from the raw dict, or use the `parse_summary` parser (see the architecture
note at the end of this notebook).

`espn_ufl_game_plays(event_id=)` returns a lightweight play list for the same
game without the full summary overhead. ESPN publishes no plays for UFL games, so for this
league both come back without play-by-play.


```python
# The championship's event_id comes from the scoreboard above, so it cannot go stale
UFL_CHAMPIONSHIP_ID = int(board['game_id'][0]) if board is not None and board.height else None

summary_ufl = safe(
    'UFL game summary (raw)',
    lambda: ufl.espn_ufl_summary(event_id=UFL_CHAMPIONSHIP_ID),
)
_keys(summary_ufl)
```

    ✅ UFL game summary (raw)
    top-level keys: ['boxscore_player', 'boxscore_team', 'plays', 'winprobability', 'leaders', 'game_info', 'officials', 'header', 'season_series', 'against_the_spread', 'standings', 'broadcasts', 'format', 'pickcenter', 'odds']



```python
# Quick play-by-play via the lightweight plays endpoint
plays_ufl = safe(
    'UFL game plays',
    lambda: ufl.espn_ufl_game_plays(event_id=UFL_CHAMPIONSHIP_ID),
) if UFL_CHAMPIONSHIP_ID else None
if plays_ufl is not None and plays_ufl.height:
    display(plays_ufl.head())
else:
    print('ESPN publishes no play-by-play for UFL games: the plays feed for this game is empty')

```

    ✅ UFL game plays
    ESPN publishes no play-by-play for UFL games: the plays feed for this game is empty


---

### 🏈 XFL: scoreboard, teams & standings

The XFL module (`sportsdataverse.football.xfl`) has the same shape.
The 2023 season is the most recent fully archived XFL slate before the
merger into the UFL.


```python
# XFL teams — good cross-check since some franchises carried over to UFL
teams_xfl = safe('XFL teams', lambda: xfl.espn_xfl_teams_site())
if teams_xfl is not None and teams_xfl.height:
    keep = [c for c in teams_xfl.columns
            if c in ('id', 'abbreviation', 'display_name', 'location')]
    display(teams_xfl.select(keep).head(10) if keep else teams_xfl.head(10))
else:
    print('XFL teams unavailable')
```

    ✅ XFL teams



    shape: (4, 14)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ team_abbr ┆ team_alte ┆ team_colo ┆ team_disp ┆ … ┆ team_nick ┆ team_shor ┆ team_slug ┆ team_uid │
    │ eviation  ┆ rnate_col ┆ r         ┆ lay_name  ┆   ┆ name      ┆ t_display ┆ ---       ┆ ---      │
    │ ---       ┆ or        ┆ ---       ┆ ---       ┆   ┆ ---       ┆ _name     ┆ str       ┆ str      │
    │ str       ┆ ---       ┆ str       ┆ str       ┆   ┆ str       ┆ ---       ┆           ┆          │
    │           ┆ str       ┆           ┆           ┆   ┆           ┆ str       ┆           ┆          │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ HOU       ┆ ba0c2f    ┆ 0c2340    ┆ Houston   ┆ … ┆ Houston   ┆ Roughneck ┆ houston-r ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Roughneck ┆   ┆           ┆ s         ┆ oughnecks ┆ 6~t:1126 │
    │           ┆           ┆           ┆ s         ┆   ┆           ┆           ┆           ┆ 48       │
    │ ORL       ┆ a3a6ad    ┆ 1a2b1e    ┆ Orlando   ┆ … ┆ Orlando   ┆ Guardians ┆ orlando-g ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Guardians ┆   ┆           ┆           ┆ uardians  ┆ 6~t:1267 │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ 48       │
    │ SEA       ┆ dc4405    ┆ 00843d    ┆ Seattle   ┆ … ┆ Seattle   ┆ Sea       ┆ seattle-s ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Sea       ┆   ┆           ┆ Dragons   ┆ ea-dragon ┆ 6~t:1126 │
    │           ┆           ┆           ┆ Dragons   ┆   ┆           ┆           ┆ s         ┆ 52       │
    │ VGS       ┆ 373a36    ┆ e10600    ┆ Vegas     ┆ … ┆ Vegas     ┆ Vipers    ┆ vegas-vip ┆ s:20~l:3 │
    │           ┆           ┆           ┆ Vipers    ┆   ┆           ┆           ┆ ers       ┆ 6~t:1267 │
    │           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ 47       │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘



```python
# XFL championship scoreboard — May 2023
board_xfl = safe(
    'XFL scoreboard (parsed)',
    lambda: xfl.espn_xfl_scoreboard(dates=XFL_DATE),
)
if board_xfl is not None and getattr(board_xfl, 'height', 0):
    keep = [c for c in board_xfl.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    display(board_xfl.select(keep).head() if keep else board_xfl.head())
else:
    print('XFL scoreboard unavailable')
```

    ✅ XFL scoreboard (parsed)
    XFL scoreboard unavailable



```python
# XFL standings (2023)
standings_xfl = safe('XFL standings', lambda: xfl.espn_xfl_standings())
if standings_xfl is not None and standings_xfl.height:
    keep = [c for c in standings_xfl.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'win_percent', 'points_for', 'points_against')]
    display(standings_xfl.select(keep).head(8) if keep else standings_xfl.head(8))
else:
    print('XFL standings unavailable')
```

    ✅ XFL standings



    shape: (7, 8)
    ┌─────────┬─────────────┬───────────────┬────────┬───────────────┬────────────┬─────────────┬──────┐
    │ team_id ┆ team_name   ┆ team_abbrevia ┆ losses ┆ points_agains ┆ points_for ┆ win_percent ┆ wins │
    │ ---     ┆ ---         ┆ tion          ┆ ---    ┆ t             ┆ ---        ┆ ---         ┆ ---  │
    │ i64     ┆ str         ┆ ---           ┆ f64    ┆ ---           ┆ f64        ┆ f64         ┆ f64  │
    │         ┆             ┆ str           ┆        ┆ f64           ┆            ┆             ┆      │
    ╞═════════╪═════════════╪═══════════════╪════════╪═══════════════╪════════════╪═════════════╪══════╡
    │ 112652  ┆ Sea Dragons ┆ SEA           ┆ 3.0    ┆ 177.0         ┆ 243.0      ┆ 0.7         ┆ 7.0  │
    │ 112651  ┆ Battlehawks ┆ STL           ┆ 3.0    ┆ 202.0         ┆ 249.0      ┆ 0.7         ┆ 7.0  │
    │ 126747  ┆ Vipers      ┆ VGS           ┆ 8.0    ┆ 252.0         ┆ 184.0      ┆ 0.2         ┆ 2.0  │
    │ 112646  ┆ Defenders   ┆ DC            ┆ 1.0    ┆ 240.0         ┆ 298.0      ┆ 0.9         ┆ 9.0  │
    │ 126746  ┆ Brahmas     ┆ SA            ┆ 7.0    ┆ 183.0         ┆ 169.0      ┆ 0.3         ┆ 3.0  │
    │ 126748  ┆ Guardians   ┆ ORL           ┆ 9.0    ┆ 310.0         ┆ 204.0      ┆ 0.1         ┆ 1.0  │
    │ 112648  ┆ Roughnecks  ┆ HOU           ┆ 3.0    ┆ 182.0         ┆ 247.0      ┆ 0.7         ┆ 7.0  │
    └─────────┴─────────────┴───────────────┴────────┴───────────────┴────────────┴─────────────┴──────┘


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
    lambda: cfl.espn_cfl_scoreboard(dates=CFL_DATE),
)
if board_cfl is not None and getattr(board_cfl, 'height', 0):
    keep = [c for c in board_cfl.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    display(board_cfl.select(keep).head() if keep else board_cfl.head())
else:
    print('CFL scoreboard unavailable')
```

    ✅ CFL scoreboard (parsed)
    CFL scoreboard unavailable



```python
# CFL teams
teams_cfl = safe('CFL teams', lambda: cfl.espn_cfl_teams_site())
if teams_cfl is not None and teams_cfl.height:
    keep = [c for c in teams_cfl.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    display(teams_cfl.select(keep).head(10) if keep else teams_cfl.head(10))
else:
    print('CFL teams unavailable')
```

    ✅ CFL teams



    shape: (9, 11)
    ┌────────────┬───────────┬─────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
    │ team_abbre ┆ team_disp ┆ team_id ┆ team_is_a ┆ … ┆ team_nick ┆ team_shor ┆ team_slug ┆ team_uid  │
    │ viation    ┆ lay_name  ┆ ---     ┆ ctive     ┆   ┆ name      ┆ t_display ┆ ---       ┆ ---       │
    │ ---        ┆ ---       ┆ i64     ┆ ---       ┆   ┆ ---       ┆ _name     ┆ str       ┆ str       │
    │ str        ┆ str       ┆         ┆ bool      ┆   ┆ str       ┆ ---       ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ str       ┆           ┆           │
    ╞════════════╪═══════════╪═════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ BCL        ┆ BC Lions  ┆ 79      ┆ true      ┆ … ┆ BC        ┆ Lions     ┆ bc-lions  ┆ s:20~l:33 │
    │            ┆           ┆         ┆           ┆   ┆           ┆           ┆           ┆ ~t:79     │
    │ CSP        ┆ Calgary   ┆ 80      ┆ true      ┆ … ┆ Calgary   ┆ Stampeder ┆ calgary-s ┆ s:20~l:33 │
    │            ┆ Stampeder ┆         ┆           ┆   ┆           ┆ s         ┆ tampeders ┆ ~t:80     │
    │            ┆ s         ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
    │ EES        ┆ Edmonton  ┆ 81      ┆ true      ┆ … ┆ Edmonton  ┆ Elks      ┆ edmonton- ┆ s:20~l:33 │
    │            ┆ Elks      ┆         ┆           ┆   ┆           ┆           ┆ elks      ┆ ~t:81     │
    │ HTC        ┆ Hamilton  ┆ 82      ┆ true      ┆ … ┆ Hamilton  ┆ Tiger-Cat ┆ hamilton- ┆ s:20~l:33 │
    │            ┆ Tiger-Cat ┆         ┆           ┆   ┆           ┆ s         ┆ tiger-cat ┆ ~t:82     │
    │            ┆ s         ┆         ┆           ┆   ┆           ┆           ┆ s         ┆           │
    │ MTA        ┆ Montreal  ┆ 83      ┆ true      ┆ … ┆ Montreal  ┆ Alouettes ┆ montreal- ┆ s:20~l:33 │
    │            ┆ Alouettes ┆         ┆           ┆   ┆           ┆           ┆ alouettes ┆ ~t:83     │
    │ ORB        ┆ Ottawa    ┆ 87      ┆ true      ┆ … ┆ Ottawa    ┆ Red       ┆ ottawa-re ┆ s:20~l:33 │
    │            ┆ Red       ┆         ┆           ┆   ┆           ┆ Blacks    ┆ d-blacks  ┆ ~t:87     │
    │            ┆ Blacks    ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
    │ SRR        ┆ Saskatche ┆ 84      ┆ true      ┆ … ┆ Saskatche ┆ Roughride ┆ saskatche ┆ s:20~l:33 │
    │            ┆ wan Rough ┆         ┆           ┆   ┆ wan       ┆ rs        ┆ wan-rough ┆ ~t:84     │
    │            ┆ riders    ┆         ┆           ┆   ┆           ┆           ┆ riders    ┆           │
    │ TAT        ┆ Toronto   ┆ 85      ┆ true      ┆ … ┆ Toronto   ┆ Argonauts ┆ toronto-a ┆ s:20~l:33 │
    │            ┆ Argonauts ┆         ┆           ┆   ┆           ┆           ┆ rgonauts  ┆ ~t:85     │
    │ WBB        ┆ Winnipeg  ┆ 86      ┆ true      ┆ … ┆ Winnipeg  ┆ Blue      ┆ winnipeg- ┆ s:20~l:33 │
    │            ┆ Blue      ┆         ┆           ┆   ┆           ┆ Bombers   ┆ blue-bomb ┆ ~t:86     │
    │            ┆ Bombers   ┆         ┆           ┆   ┆           ┆           ┆ ers       ┆           │
    └────────────┴───────────┴─────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘



```python
# CFL standings (2023)
standings_cfl = safe('CFL standings', lambda: cfl.espn_cfl_standings())
if standings_cfl is not None and standings_cfl.height:
    keep = [c for c in standings_cfl.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'win_percent', 'points_for', 'points_against')]
    display(standings_cfl.select(keep).head(9) if keep else standings_cfl.head(9))
else:
    print('CFL standings unavailable')
```

    ✅ CFL standings
    CFL standings unavailable



```python
# CFL team schedule — Winnipeg Blue Bombers (team_id=86) 2023 season
schedule_cfl = safe(
    'CFL team schedule',
    lambda: cfl.espn_cfl_team_schedule(team_id=86, season=2023),
)
if schedule_cfl is not None and schedule_cfl.height:
    keep = [c for c in schedule_cfl.columns
            if c in ('id', 'week', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    display(schedule_cfl.select(keep).head(8) if keep else schedule_cfl.head(8))
else:
    print('CFL schedule unavailable')
```

    ✅ CFL team schedule
    CFL schedule unavailable


---

## ⚾ Part 2 — College Baseball & Softball

NCAA Baseball and NCAA Softball sit under `sportsdataverse.baseball`.
Both modules expose the full `_NCAA_WRAPPERS` set — which adds
`espn_{prefix}_rankings` and `espn_{prefix}_season_recruits` on top of
the universal surface — giving you AP-style polls and recruiting boards
alongside the standard scoreboard/schedule/teams/standings family.

### ⚾ College baseball: scoreboard & teams

[`espn_college_baseball_scoreboard(dates=YYYYMMDD)`](../college_baseball/reference/site.md#espn_college_baseball_scoreboard)
returns the game slate for a date. The College World Series in June is peak
traffic, so games should be available; the off-season window (November – January)
returns an empty slate.


```python
# College World Series finals day, 2024
board_cbb = safe(
    'college baseball scoreboard (parsed)',
    lambda: cbb.espn_college_baseball_scoreboard(dates=CBB_DATE),
)
if board_cbb is not None and getattr(board_cbb, 'height', 0):
    keep = [c for c in board_cbb.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    display(board_cbb.select(keep).head() if keep else board_cbb.head())
else:
    print('college baseball scoreboard unavailable')
```

    ✅ college baseball scoreboard (parsed)



    shape: (1, 5)
    ┌────────────────────────────────┬─────────────┬─────────────────────────┬────────────┬────────────┐
    │ name                           ┆ short_name  ┆ status_type_description ┆ home_score ┆ away_score │
    │ ---                            ┆ ---         ┆ ---                     ┆ ---        ┆ ---        │
    │ str                            ┆ str         ┆ str                     ┆ str        ┆ str        │
    ╞════════════════════════════════╪═════════════╪═════════════════════════╪════════════╪════════════╡
    │ Texas A&M Aggies at Tennessee  ┆ TA&M @ TENN ┆ Final                   ┆ 6          ┆ 5          │
    │ …                              ┆             ┆                         ┆            ┆            │
    └────────────────────────────────┴─────────────┴─────────────────────────┴────────────┴────────────┘



```python
# All NCAA baseball teams with their team_id
teams_cbb = safe(
    'college baseball teams',
    lambda: cbb.espn_college_baseball_teams_site(),
)
if teams_cbb is not None and teams_cbb.height:
    keep = [c for c in teams_cbb.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    print('Total teams:', teams_cbb.height)
    display(teams_cbb.select(keep).head(10) if keep else teams_cbb.head(10))
else:
    print('teams unavailable')
```

    ✅ college baseball teams
    Total teams: 437



    shape: (10, 13)
    ┌────────────┬───────────┬───────────┬─────────┬───┬───────────┬───────────┬───────────┬───────────┐
    │ team_abbre ┆ team_colo ┆ team_disp ┆ team_id ┆ … ┆ team_nick ┆ team_shor ┆ team_slug ┆ team_uid  │
    │ viation    ┆ r         ┆ lay_name  ┆ ---     ┆   ┆ name      ┆ t_display ┆ ---       ┆ ---       │
    │ ---        ┆ ---       ┆ ---       ┆ i64     ┆   ┆ ---       ┆ _name     ┆ str       ┆ str       │
    │ str        ┆ str       ┆ str       ┆         ┆   ┆ str       ┆ ---       ┆           ┆           │
    │            ┆           ┆           ┆         ┆   ┆           ┆ str       ┆           ┆           │
    ╞════════════╪═══════════╪═══════════╪═════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ ACU        ┆ 000000    ┆ Abilene   ┆ 315     ┆ … ┆ Abilene   ┆ Abilene   ┆ abilene-c ┆ s:1~l:14~ │
    │            ┆           ┆ Christian ┆         ┆   ┆ Chrstn    ┆ Chrstn    ┆ hristian- ┆ t:315     │
    │            ┆           ┆ Wildcats  ┆         ┆   ┆           ┆           ┆ wildcats  ┆           │
    │ AFA        ┆ 004A7B    ┆ Air Force ┆ 155     ┆ … ┆ Air Force ┆ Air Force ┆ air-force ┆ s:1~l:14~ │
    │            ┆           ┆ Falcons   ┆         ┆   ┆           ┆           ┆ -falcons  ┆ t:155     │
    │ AKR        ┆ 0A1E5B    ┆ Akron     ┆ 316     ┆ … ┆ Akron     ┆ Akron     ┆ akron-zip ┆ s:1~l:14~ │
    │            ┆           ┆ Zips      ┆         ┆   ┆           ┆           ┆ s         ┆ t:316     │
    │ AAMU       ┆ 790000    ┆ Alabama   ┆ 317     ┆ … ┆ Alabama   ┆ Alabama   ┆ alabama-a ┆ s:1~l:14~ │
    │            ┆           ┆ A&M       ┆         ┆   ┆ A&M       ┆ A&M       ┆ m-bulldog ┆ t:317     │
    │            ┆           ┆ Bulldogs  ┆         ┆   ┆           ┆           ┆ s         ┆           │
    │ ALA        ┆ A42D47    ┆ Alabama   ┆ 148     ┆ … ┆ Alabama   ┆ Alabama   ┆ alabama-c ┆ s:1~l:14~ │
    │            ┆           ┆ Crimson   ┆         ┆   ┆           ┆           ┆ rimson-ti ┆ t:148     │
    │            ┆           ┆ Tide      ┆         ┆   ┆           ┆           ┆ de        ┆           │
    │ ALST       ┆ 603913    ┆ Alabama   ┆ 318     ┆ … ┆ Alabama   ┆ Alabama   ┆ alabama-s ┆ s:1~l:14~ │
    │            ┆           ┆ State     ┆         ┆   ┆ St        ┆ St        ┆ tate-horn ┆ t:318     │
    │            ┆           ┆ Hornets   ┆         ┆   ┆           ┆           ┆ ets       ┆           │
    │ ALCN       ┆ C1B881    ┆ Alcorn    ┆ 260     ┆ … ┆ Alcorn St ┆ Alcorn St ┆ alcorn-st ┆ s:1~l:14~ │
    │            ┆           ┆ State     ┆         ┆   ┆           ┆           ┆ ate-brave ┆ t:260     │
    │            ┆           ┆ Braves    ┆         ┆   ┆           ┆           ┆ s         ┆           │
    │ ALMA       ┆ null      ┆ Alma      ┆ 821     ┆ … ┆ Alma      ┆ Alma      ┆ alma-scot ┆ s:1~l:14~ │
    │            ┆           ┆ Scots     ┆         ┆   ┆           ┆           ┆ s         ┆ t:821     │
    │ AMER       ┆ c41130    ┆ American  ┆ 319     ┆ … ┆ American  ┆ American  ┆ american- ┆ s:1~l:14~ │
    │            ┆           ┆ Universit ┆         ┆   ┆           ┆           ┆ universit ┆ t:319     │
    │            ┆           ┆ y Eagles  ┆         ┆   ┆           ┆           ┆ y-eagles  ┆           │
    │ ANDC       ┆ null      ┆ Anderson  ┆ 822     ┆ … ┆ Anderson  ┆ Anderson  ┆ anderson- ┆ s:1~l:14~ │
    │            ┆           ┆ (IN)      ┆         ┆   ┆ IND       ┆ IND       ┆ in-ravens ┆ t:822     │
    │            ┆           ┆ Ravens    ┆         ┆   ┆           ┆           ┆           ┆           │
    └────────────┴───────────┴───────────┴─────────┴───┴───────────┴───────────┴───────────┴───────────┘


### 📊 College baseball: standings & rankings

[`espn_college_baseball_standings()`](../college_baseball/reference/site.md#espn_college_baseball_standings)
returns conference standings. [`espn_college_baseball_rankings()`](../college_baseball/reference/site.md#espn_college_baseball_rankings)
returns the current AP / USA Today poll — one of the NCAA extras added by the
`_NCAA_WRAPPERS` family.


```python
# Conference standings
standings_cbb = safe(
    'college baseball standings',
    lambda: cbb.espn_college_baseball_standings(),
)
if standings_cbb is not None and standings_cbb.height:
    keep = [c for c in standings_cbb.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'win_percent')]
    display(standings_cbb.select(keep).head(10) if keep else standings_cbb.head(10))
else:
    print('standings unavailable')
```

    ✅ college baseball standings



    shape: (10, 6)
    ┌─────────┬──────────────┬───────────────────┬────────┬─────────────┬──────┐
    │ team_id ┆ team_name    ┆ team_abbreviation ┆ losses ┆ win_percent ┆ wins │
    │ ---     ┆ ---          ┆ ---               ┆ ---    ┆ ---         ┆ ---  │
    │ i64     ┆ str          ┆ str               ┆ f64    ┆ f64         ┆ f64  │
    ╞═════════╪══════════════╪═══════════════════╪════════╪═════════════╪══════╡
    │ 146     ┆ Chanticleers ┆ CCU               ┆ 0.0    ┆ 1.0         ┆ 3.0  │
    │ 58      ┆ Razorbacks   ┆ ARK               ┆ 13.0   ┆ 0.779661    ┆ 46.0 │
    │ 96      ┆ Tar Heels    ┆ UNC               ┆ 13.0   ┆ 0.7758621   ┆ 45.0 │
    │ 126     ┆ Longhorns    ┆ TEX               ┆ 14.0   ┆ 0.7586207   ┆ 44.0 │
    │ 136     ┆ Mountaineers ┆ WVU               ┆ 14.0   ┆ 0.7586207   ┆ 44.0 │
    │ 85      ┆ Tigers       ┆ LSU               ┆ 15.0   ┆ 0.754098    ┆ 46.0 │
    │ 72      ┆ Seminoles    ┆ FSU               ┆ 14.0   ┆ 0.745455    ┆ 41.0 │
    │ 66      ┆ Bruins       ┆ UCLA              ┆ 16.0   ┆ 0.737705    ┆ 45.0 │
    │ 199     ┆ Volunteers   ┆ TENN              ┆ 17.0   ┆ 0.730159    ┆ 46.0 │
    │ 273     ┆ Ducks        ┆ ORE               ┆ 16.0   ┆ 0.7241379   ┆ 42.0 │
    └─────────┴──────────────┴───────────────────┴────────┴─────────────┴──────┘



```python
# National polls — AP / USA Today baseball rankings
rankings_cbb = safe(
    'college baseball rankings',
    lambda: cbb.espn_college_baseball_rankings(),
)
if rankings_cbb is not None and rankings_cbb.height:
    keep = [c for c in rankings_cbb.columns
            if c in ('rank', 'team_id', 'team_name', 'points', 'first_place_votes',
                     'wins', 'losses')]
    display(rankings_cbb.select(keep).head(10) if keep else rankings_cbb.head(10))
else:
    print('rankings unavailable (only current during the season)')
```

    ⏭️  college baseball rankings: NoDataError: No data found for https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/rankings
    rankings unavailable (only current during the season)


### 📅 College baseball: team schedule

Pick any `team_id` from the teams frame above and pull its full schedule.
Texas (team_id=251) is a perennial CWS contender — a good reference point.


```python
# Texas Longhorns baseball schedule (team_id=126; 251 is Texas in football/basketball)
schedule_cbb = safe(
    'college baseball team schedule',
    lambda: cbb.espn_college_baseball_team_schedule(
        team_id=126, season=SAMPLE_SEASON
    ),
)
if schedule_cbb is not None and schedule_cbb.height:
    keep = [c for c in schedule_cbb.columns
            if c in ('id', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    display(schedule_cbb.select(keep).head(8) if keep else schedule_cbb.head(8))
else:
    print('schedule unavailable')
```

    ✅ college baseball team schedule



    shape: (8, 3)
    ┌───────────┬─────────────────────────────────┬────────────┐
    │ id        ┆ name                            ┆ short_name │
    │ ---       ┆ ---                             ┆ ---        │
    │ i64       ┆ str                             ┆ str        │
    ╞═══════════╪═════════════════════════════════╪════════════╡
    │ 401631945 ┆ San Diego Toreros at Texas Lon… ┆ USD @ TEX  │
    │ 401631946 ┆ San Diego Toreros at Texas Lon… ┆ USD @ TEX  │
    │ 401631947 ┆ San Diego Toreros at Texas Lon… ┆ USD @ TEX  │
    │ 401633177 ┆ Houston Christian Huskies at T… ┆ HCU @ TEX  │
    │ 401633178 ┆ Cal Poly Mustangs at Texas Lon… ┆ CP @ TEX   │
    │ 401633179 ┆ Cal Poly Mustangs at Texas Lon… ┆ CP @ TEX   │
    │ 401633180 ┆ Cal Poly Mustangs at Texas Lon… ┆ CP @ TEX   │
    │ 401633181 ┆ St. John's Red Storm at Texas … ┆ SJU @ TEX  │
    └───────────┴─────────────────────────────────┴────────────┘


### 🥎 College softball: scoreboard & teams

The `college_softball` module mirrors `college_baseball` exactly — same wrapper
family, just a different league slug. The Women's College World Series (WCWS)
wraps up in early June.


```python
# WCWS finals day, June 2024
board_cbs = safe(
    'college softball scoreboard (parsed)',
    lambda: cbs.espn_college_softball_scoreboard(dates=CBS_DATE),
)
if board_cbs is not None and getattr(board_cbs, 'height', 0):
    keep = [c for c in board_cbs.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    display(board_cbs.select(keep).head() if keep else board_cbs.head())
else:
    print('college softball scoreboard unavailable')
```

    ✅ college softball scoreboard (parsed)



    shape: (1, 5)
    ┌─────────────────────────────────┬────────────┬─────────────────────────┬────────────┬────────────┐
    │ name                            ┆ short_name ┆ status_type_description ┆ home_score ┆ away_score │
    │ ---                             ┆ ---        ┆ ---                     ┆ ---        ┆ ---        │
    │ str                             ┆ str        ┆ str                     ┆ str        ┆ str        │
    ╞═════════════════════════════════╪════════════╪═════════════════════════╪════════════╪════════════╡
    │ Texas Longhorns at Oklahoma So… ┆ TEX @ OU   ┆ Final                   ┆ 8          ┆ 4          │
    └─────────────────────────────────┴────────────┴─────────────────────────┴────────────┴────────────┘



```python
# College softball teams
teams_cbs = safe(
    'college softball teams',
    lambda: cbs.espn_college_softball_teams_site(),
)
if teams_cbs is not None and teams_cbs.height:
    keep = [c for c in teams_cbs.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    print('Total teams:', teams_cbs.height)
    display(teams_cbs.select(keep).head(10) if keep else teams_cbs.head(10))
else:
    print('teams unavailable')
```

    ✅ college softball teams
    Total teams: 446



    shape: (10, 13)
    ┌────────────┬───────────┬───────────┬─────────┬───┬───────────┬───────────┬───────────┬───────────┐
    │ team_abbre ┆ team_colo ┆ team_disp ┆ team_id ┆ … ┆ team_nick ┆ team_shor ┆ team_slug ┆ team_uid  │
    │ viation    ┆ r         ┆ lay_name  ┆ ---     ┆   ┆ name      ┆ t_display ┆ ---       ┆ ---       │
    │ ---        ┆ ---       ┆ ---       ┆ i64     ┆   ┆ ---       ┆ _name     ┆ str       ┆ str       │
    │ str        ┆ str       ┆ str       ┆         ┆   ┆ str       ┆ ---       ┆           ┆           │
    │            ┆           ┆           ┆         ┆   ┆           ┆ str       ┆           ┆           │
    ╞════════════╪═══════════╪═══════════╪═════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ ACU        ┆ 000000    ┆ Abilene   ┆ 669     ┆ … ┆ Abilene   ┆ Abilene   ┆ abilene-c ┆ s:1~l:102 │
    │            ┆           ┆ Christian ┆         ┆   ┆ Chrstn    ┆ Chrstn    ┆ hristian- ┆ ~t:669    │
    │            ┆           ┆ Wildcats  ┆         ┆   ┆           ┆           ┆ wildcats  ┆           │
    │ AFA        ┆ 004A7B    ┆ Air Force ┆ 567     ┆ … ┆ Air Force ┆ Air Force ┆ air-force ┆ s:1~l:102 │
    │            ┆           ┆ Falcons   ┆         ┆   ┆           ┆           ┆ -falcons  ┆ ~t:567    │
    │ AKR        ┆ 0A1E5B    ┆ Akron     ┆ 670     ┆ … ┆ Akron     ┆ Akron     ┆ akron-zip ┆ s:1~l:102 │
    │            ┆           ┆ Zips      ┆         ┆   ┆           ┆           ┆ s         ┆ ~t:670    │
    │ AAMU       ┆ 790000    ┆ Alabama   ┆ 671     ┆ … ┆ Alabama   ┆ Alabama   ┆ alabama-a ┆ s:1~l:102 │
    │            ┆           ┆ A&M       ┆         ┆   ┆ A&M       ┆ A&M       ┆ m-bulldog ┆ ~t:671    │
    │            ┆           ┆ Bulldogs  ┆         ┆   ┆           ┆           ┆ s         ┆           │
    │ ALA        ┆ A42D47    ┆ Alabama   ┆ 560     ┆ … ┆ Alabama   ┆ Alabama   ┆ alabama-c ┆ s:1~l:102 │
    │            ┆           ┆ Crimson   ┆         ┆   ┆           ┆           ┆ rimson-ti ┆ ~t:560    │
    │            ┆           ┆ Tide      ┆         ┆   ┆           ┆           ┆ de        ┆           │
    │ AH         ┆ null      ┆ Alabama   ┆ 860     ┆ … ┆ UA-Huntsv ┆ UA-Huntsv ┆ alabama-h ┆ s:1~l:102 │
    │            ┆           ┆ Huntsvill ┆         ┆   ┆ ille      ┆ ille      ┆ untsville ┆ ~t:860    │
    │            ┆           ┆ e         ┆         ┆   ┆           ┆           ┆ -chargers ┆           │
    │            ┆           ┆ Chargers  ┆         ┆   ┆           ┆           ┆           ┆           │
    │ ALST       ┆ 603913    ┆ Alabama   ┆ 672     ┆ … ┆ Alabama   ┆ Alabama   ┆ alabama-s ┆ s:1~l:102 │
    │            ┆           ┆ State     ┆         ┆   ┆ St        ┆ St        ┆ tate-horn ┆ ~t:672    │
    │            ┆           ┆ Hornets   ┆         ┆   ┆           ┆           ┆ ets       ┆           │
    │ ALBS       ┆ null      ┆ Albany    ┆ 861     ┆ … ┆ Albany St ┆ Albany St ┆ albany-st ┆ s:1~l:102 │
    │            ┆           ┆ State     ┆         ┆   ┆           ┆           ┆ ate-golde ┆ ~t:861    │
    │            ┆           ┆ Golden    ┆         ┆   ┆           ┆           ┆ n-rams    ┆           │
    │            ┆           ┆ Rams      ┆         ┆   ┆           ┆           ┆           ┆           │
    │ ALCN       ┆ C1B881    ┆ Alcorn    ┆ 623     ┆ … ┆ Alcorn St ┆ Alcorn St ┆ alcorn-st ┆ s:1~l:102 │
    │            ┆           ┆ State     ┆         ┆   ┆           ┆           ┆ ate-brave ┆ ~t:623    │
    │            ┆           ┆ Braves    ┆         ┆   ┆           ┆           ┆ s         ┆           │
    │ AMER       ┆ c41130    ┆ American  ┆ 673     ┆ … ┆ American  ┆ American  ┆ american- ┆ s:1~l:102 │
    │            ┆           ┆ Universit ┆         ┆   ┆           ┆           ┆ universit ┆ ~t:673    │
    │            ┆           ┆ y Eagles  ┆         ┆   ┆           ┆           ┆ y-eagles  ┆           │
    └────────────┴───────────┴───────────┴─────────┴───┴───────────┴───────────┴───────────┴───────────┘



```python
# College softball rankings
rankings_cbs = safe(
    'college softball rankings',
    lambda: cbs.espn_college_softball_rankings(),
)
if rankings_cbs is not None and rankings_cbs.height:
    keep = [c for c in rankings_cbs.columns
            if c in ('rank', 'team_id', 'team_name', 'points',
                     'first_place_votes', 'wins', 'losses')]
    display(rankings_cbs.select(keep).head(10) if keep else rankings_cbs.head(10))
else:
    print('rankings unavailable (only current during the season)')
```

    ⏭️  college softball rankings: NoDataError: No data found for https://site.api.espn.com/apis/site/v2/sports/baseball/college-softball/rankings
    rankings unavailable (only current during the season)


### 🥎 College softball: game summary

The same `espn_{prefix}_summary(event_id=)` pattern works for softball.
The raw dict contains a `boxscore` key (team batting/pitching lines),
a `scoringPlays` array, and — for parsed callers — all 21 sub-frames
from the shared `parse_summary` dispatcher.


```python
# 2024 WCWS Championship Game 2 — Oklahoma vs Texas, its event_id taken from the scoreboard above
CBS_CHAMPIONSHIP_ID = int(board_cbs['game_id'][0]) if board_cbs is not None and board_cbs.height else None

summary_cbs = safe(
    'college softball game summary (raw)',
    lambda: cbs.espn_college_softball_summary(event_id=CBS_CHAMPIONSHIP_ID),
)
_keys(summary_cbs)
```

    ✅ college softball game summary (raw)
    top-level keys: ['boxscore_player', 'boxscore_team', 'plays', 'winprobability', 'leaders', 'game_info', 'officials', 'header', 'season_series', 'against_the_spread', 'standings', 'broadcasts', 'format', 'pickcenter', 'odds']


---

## 🏒 Part 3 — NCAA Men's & Women's College Hockey

Men's College Hockey (`mch`) and Women's College Hockey (`wch`) round out the
seven-league expansion. Both expose the full `_NCAA_WRAPPERS` set. The
signature events are the **Frozen Four** in April (men's) and March (women's).

### 🏒 Men's college hockey: scoreboard & teams

[`espn_mch_scoreboard(dates=YYYYMMDD)`](../mch/reference/site.md#espn_mch_scoreboard)
returns the game slate. [`espn_mch_teams_site()`](../mch/reference/site.md#espn_mch_teams_site)
lists all Division I programs — currently 60 teams.


```python
# Frozen Four semi-final day, April 2024
board_mch = safe(
    'men\'s college hockey scoreboard (parsed)',
    lambda: mch.espn_mch_scoreboard(dates=MCH_DATE),
)
if board_mch is not None and getattr(board_mch, 'height', 0):
    keep = [c for c in board_mch.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    display(board_mch.select(keep).head() if keep else board_mch.head())
else:
    print('men\'s college hockey scoreboard unavailable')
```

    ✅ men's college hockey scoreboard (parsed)



    shape: (1, 5)
    ┌─────────────────────────────────┬────────────┬─────────────────────────┬────────────┬────────────┐
    │ name                            ┆ short_name ┆ status_type_description ┆ home_score ┆ away_score │
    │ ---                             ┆ ---        ┆ ---                     ┆ ---        ┆ ---        │
    │ str                             ┆ str        ┆ str                     ┆ str        ┆ str        │
    ╞═════════════════════════════════╪════════════╪═════════════════════════╪════════════╪════════════╡
    │ Denver Pioneers at Boston Coll… ┆ DEN @ BC   ┆ Final                   ┆ 0          ┆ 2          │
    └─────────────────────────────────┴────────────┴─────────────────────────┴────────────┴────────────┘



```python
# Men's college hockey teams
teams_mch = safe(
    'men\'s college hockey teams',
    lambda: mch.espn_mch_teams_site(),
)
if teams_mch is not None and teams_mch.height:
    keep = [c for c in teams_mch.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    print('Total D-I men\'s programs:', teams_mch.height)
    display(teams_mch.select(keep).head(10) if keep else teams_mch.head(10))
else:
    print('teams unavailable')
```

    ✅ men's college hockey teams
    Total D-I men's programs: 116



    shape: (10, 14)
    ┌────────────┬───────────┬─────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
    │ team_abbre ┆ team_disp ┆ team_id ┆ team_is_a ┆ … ┆ team_uid  ┆ team_logo ┆ team_alte ┆ team_colo │
    │ viation    ┆ lay_name  ┆ ---     ┆ ctive     ┆   ┆ ---       ┆ s         ┆ rnate_col ┆ r         │
    │ ---        ┆ ---       ┆ i64     ┆ ---       ┆   ┆ str       ┆ ---       ┆ or        ┆ ---       │
    │ str        ┆ str       ┆         ┆ bool      ┆   ┆           ┆ str       ┆ ---       ┆ str       │
    │            ┆           ┆         ┆           ┆   ┆           ┆           ┆ str       ┆           │
    ╞════════════╪═══════════╪═════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ ADR        ┆ Adrian    ┆ 110134  ┆ true      ┆ … ┆ s:70~l:91 ┆ null      ┆ null      ┆ null      │
    │            ┆ Bulldogs  ┆         ┆           ┆   ┆ ~t:110134 ┆           ┆           ┆           │
    │ AF         ┆ Air Force ┆ 2005    ┆ true      ┆ … ┆ s:70~l:91 ┆ [{'alt':  ┆ null      ┆ null      │
    │            ┆ Falcons   ┆         ┆           ┆   ┆ ~t:2005   ┆ '',       ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ 'height': ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ 500, 'h…  ┆           ┆           │
    │ AH         ┆ Alabama   ┆ 2008    ┆ true      ┆ … ┆ s:70~l:91 ┆ null      ┆ null      ┆ null      │
    │            ┆ Huntsvill ┆         ┆           ┆   ┆ ~t:2008   ┆           ┆           ┆           │
    │            ┆ e         ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
    │            ┆ Chargers  ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
    │ UAA        ┆ Alaska    ┆ 1       ┆ true      ┆ … ┆ s:70~l:91 ┆ [{'alt':  ┆ null      ┆ null      │
    │            ┆ Anchorage ┆         ┆           ┆   ┆ ~t:1      ┆ '',       ┆           ┆           │
    │            ┆ Seawolves ┆         ┆           ┆   ┆           ┆ 'height': ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ 500, 'h…  ┆           ┆           │
    │ AKFB       ┆ Alaska    ┆ 298     ┆ true      ┆ … ┆ s:70~l:91 ┆ [{'alt':  ┆ null      ┆ null      │
    │            ┆ Nanooks   ┆         ┆           ┆   ┆ ~t:298    ┆ '',       ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ 'height': ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ 500, 'h…  ┆           ┆           │
    │ ABCA       ┆ Alberta   ┆ 14196   ┆ true      ┆ … ┆ s:70~l:91 ┆ null      ┆ null      ┆ null      │
    │            ┆ Golden    ┆         ┆           ┆   ┆ ~t:14196  ┆           ┆           ┆           │
    │            ┆ Bears &   ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
    │            ┆ Pandas    ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
    │ AIC        ┆ American  ┆ 2022    ┆ true      ┆ … ┆ s:70~l:91 ┆ [{'alt':  ┆ null      ┆ null      │
    │            ┆ Internati ┆         ┆           ┆   ┆ ~t:2022   ┆ '',       ┆           ┆           │
    │            ┆ onal      ┆         ┆           ┆   ┆           ┆ 'height': ┆           ┆           │
    │            ┆ Yellow …  ┆         ┆           ┆   ┆           ┆ 500, 'h…  ┆           ┆           │
    │ ANMR       ┆ Anna      ┆ 126807  ┆ true      ┆ … ┆ s:70~l:91 ┆ null      ┆ null      ┆ null      │
    │            ┆ Maria     ┆         ┆           ┆   ┆ ~t:126807 ┆           ┆           ┆           │
    │            ┆ Amcats    ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
    │ ASU        ┆ Arizona   ┆ 9       ┆ true      ┆ … ┆ s:70~l:91 ┆ [{'alt':  ┆ null      ┆ null      │
    │            ┆ State Sun ┆         ┆           ┆   ┆ ~t:9      ┆ '',       ┆           ┆           │
    │            ┆ Devils    ┆         ┆           ┆   ┆           ┆ 'height': ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ 500, 'h…  ┆           ┆           │
    │ ARIZ       ┆ Arizona   ┆ 12      ┆ true      ┆ … ┆ s:70~l:91 ┆ [{'alt':  ┆ null      ┆ null      │
    │            ┆ Wildcats  ┆         ┆           ┆   ┆ ~t:12     ┆ '',       ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ 'height': ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ 500, 'h…  ┆           ┆           │
    └────────────┴───────────┴─────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘


### 📊 Men's college hockey: standings & rankings

Conference standings are the primary puck-nerd query —
which NCHC / Hockey East / Big Ten team leads their division?
The rankings endpoint returns the USCHO / USA Today poll.


```python
# Conference standings
standings_mch = safe(
    "men's college hockey standings",
    lambda: mch.espn_mch_standings(),
)
if standings_mch is not None and standings_mch.height:
    keep = [c for c in standings_mch.columns
            if c in ('team_id', 'team_name', 'team_abbreviation',
                     'wins', 'losses', 'ties', 'win_percent')]
    display(standings_mch.select(keep).head(10) if keep else standings_mch.head(10))
else:
    print('standings unavailable')
```

    ✅ men's college hockey standings



    shape: (10, 6)
    ┌─────────┬─────────────────┬───────────────────┬────────┬─────────────┬──────┐
    │ team_id ┆ team_name       ┆ team_abbreviation ┆ losses ┆ win_percent ┆ wins │
    │ ---     ┆ ---             ┆ ---               ┆ ---    ┆ ---         ┆ ---  │
    │ i64     ┆ str             ┆ str               ┆ f64    ┆ f64         ┆ f64  │
    ╞═════════╪═════════════════╪═══════════════════╪════════╪═════════════╪══════╡
    │ 349     ┆ Black Knights   ┆ ARMY              ┆ 1.0    ┆ 0.8333333   ┆ 5.0  │
    │ 2005    ┆ Falcons         ┆ AFA               ┆ 5.0    ┆ 0.375       ┆ 3.0  │
    │ 2060    ┆ Falcons         ┆ BENT              ┆ 4.0    ┆ 0.5         ┆ 4.0  │
    │ 2099    ┆ Golden Griffins ┆ CAN               ┆ 4.0    ┆ 0.428571    ┆ 3.0  │
    │ 2529    ┆ Pioneers        ┆ SHU               ┆ 4.0    ┆ 0.5         ┆ 4.0  │
    │ 178     ┆ Tigers          ┆ RIT               ┆ 6.0    ┆ 0.25        ┆ 2.0  │
    │ 2523    ┆ Colonials       ┆ RMU               ┆ 7.0    ┆ 0.222222    ┆ 2.0  │
    │ 107     ┆ Crusaders       ┆ HC                ┆ 6.0    ┆ 0.4         ┆ 4.0  │
    │ 315     ┆ Purple Eagles   ┆ NIA               ┆ 4.0    ┆ 0.5555556   ┆ 5.0  │
    │ 127     ┆ Spartans        ┆ MSU               ┆ 1.0    ┆ 0.8333333   ┆ 5.0  │
    └─────────┴─────────────────┴───────────────────┴────────┴─────────────┴──────┘



```python
# National rankings
rankings_mch = safe(
    "men's college hockey rankings",
    lambda: mch.espn_mch_rankings(),
)
if rankings_mch is not None and rankings_mch.height:
    keep = [c for c in rankings_mch.columns
            if c in ('rank', 'team_id', 'team_name', 'points',
                     'first_place_votes', 'wins', 'losses', 'ties')]
    display(rankings_mch.select(keep).head(10) if keep else rankings_mch.head(10))
else:
    print('rankings unavailable (only current during the season)')
```

    ✅ men's college hockey rankings



    shape: (10, 5)
    ┌─────────┬──────┬────────┬───────────────────┬────────────────┐
    │ team_id ┆ rank ┆ points ┆ first_place_votes ┆ team_name      │
    │ ---     ┆ ---  ┆ ---    ┆ ---               ┆ ---            │
    │ i64     ┆ i64  ┆ f64    ┆ i64               ┆ str            │
    ╞═════════╪══════╪════════╪═══════════════════╪════════════════╡
    │ 275     ┆ 1    ┆ 0.0    ┆ null              ┆ Badgers        │
    │ 130     ┆ 2    ┆ 0.0    ┆ null              ┆ Wolverines     │
    │ 127     ┆ 3    ┆ 0.0    ┆ null              ┆ Spartans       │
    │ 2172    ┆ 4    ┆ 0.0    ┆ null              ┆ Pioneers       │
    │ 2711    ┆ 5    ┆ 0.0    ┆ null              ┆ Broncos        │
    │ 134     ┆ 6    ┆ 0.0    ┆ null              ┆ Bulldogs       │
    │ 2514    ┆ 6    ┆ 0.0    ┆ null              ┆ Bobcats        │
    │ 2043    ┆ 8    ┆ 0.0    ┆ null              ┆ Vikings        │
    │ 2507    ┆ 9    ┆ 0.0    ┆ null              ┆ Friars         │
    │ 155     ┆ 10   ┆ 0.0    ┆ null              ┆ Fighting Hawks │
    └─────────┴──────┴────────┴───────────────────┴────────────────┘


### 📅 Men's college hockey: team schedule & roster

Pull a program's full schedule with
[`espn_mch_team_schedule(team_id=, season=)`](../mch/reference/site.md#espn_mch_team_schedule)
and their roster with
[`espn_mch_team_roster(team_id=, season=)`](../mch/reference/site.md#espn_mch_team_roster).
Boston University (team_id=103) won the 2024 national title — a good
reference point.


```python
# BU Terriers schedule 2023-24
schedule_mch = safe(
    "men's college hockey team schedule",
    lambda: mch.espn_mch_team_schedule(team_id=104, season=SAMPLE_SEASON,
                                       ),
)
if schedule_mch is not None and schedule_mch.height:
    keep = [c for c in schedule_mch.columns
            if c in ('id', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    display(schedule_mch.select(keep).head(8) if keep else schedule_mch.head(8))
else:
    print('schedule unavailable')
```

    ✅ men's college hockey team schedule



    shape: (8, 3)
    ┌───────────┬─────────────────────────────────┬────────────┐
    │ id        ┆ name                            ┆ short_name │
    │ ---       ┆ ---                             ┆ ---        │
    │ i64       ┆ str                             ┆ str        │
    ╞═══════════╪═════════════════════════════════╪════════════╡
    │ 401596244 ┆ Boston University Terriers at … ┆ BU @ BENT  │
    │ 401596384 ┆ Boston University Terriers at … ┆ BU @ UNH   │
    │ 401596385 ┆ Boston University Terriers at … ┆ BU @ ND    │
    │ 401596386 ┆ Boston University Terriers at … ┆ BU @ ND    │
    │ 401596387 ┆ Massachusetts Minutemen at Bos… ┆ MASS @ BU  │
    │ 401596388 ┆ Boston University Terriers at … ┆ BU @ MASS  │
    │ 401596389 ┆ North Dakota Fighting Hawks at… ┆ UND @ BU   │
    │ 401596390 ┆ North Dakota Fighting Hawks at… ┆ UND @ BU   │
    └───────────┴─────────────────────────────────┴────────────┘



```python
# BU Terriers roster (current)
roster_mch = safe(
    "men's college hockey roster",
    lambda: mch.espn_mch_team_roster(team_id=104),  # ESPN's roster is always the current one
)
if roster_mch is not None and roster_mch.height:
    keep = [c for c in roster_mch.columns
            if c in ('id', 'first_name', 'last_name', 'display_name',
                     'position_abbreviation', 'jersey', 'birth_country')]
    display(roster_mch.select(keep).head(10) if keep else roster_mch.head(10))
else:
    print('roster unavailable')
```

    ✅ men's college hockey roster



    shape: (10, 6)
    ┌─────────┬────────────┬───────────┬─────────────────┬────────┬───────────────────────┐
    │ id      ┆ first_name ┆ last_name ┆ display_name    ┆ jersey ┆ position_abbreviation │
    │ ---     ┆ ---        ┆ ---       ┆ ---             ┆ ---    ┆ ---                   │
    │ i64     ┆ str        ┆ str       ┆ str             ┆ str    ┆ str                   │
    ╞═════════╪════════════╪═══════════╪═════════════════╪════════╪═══════════════════════╡
    │ 5136656 ┆ Tristan    ┆ Amonte    ┆ Tristan Amonte  ┆ 3      ┆ F                     │
    │ 5216936 ┆ Kamil      ┆ Bednarik  ┆ Kamil Bednarik  ┆ 11     ┆ F                     │
    │ 5206627 ┆ Mathieu    ┆ Caron     ┆ Mathieu Caron   ┆ 62     ┆ G                     │
    │ 5188559 ┆ Aiden      ┆ Celebrini ┆ Aiden Celebrini ┆ 22     ┆ D                     │
    │ 4997117 ┆ Brehdan    ┆ Engum     ┆ Brehdan Engum   ┆ 10     ┆ D                     │
    │ 5206622 ┆ Mick       ┆ Frechette ┆ Mick Frechette  ┆ 14     ┆ D                     │
    │ 5275393 ┆ Billy      ┆ Girard IV ┆ Billy Girard IV ┆ 1      ┆ G                     │
    │ 5206624 ┆ Jack       ┆ Gorton    ┆ Jack Gorton     ┆ 26     ┆ F                     │
    │ 5206623 ┆ Doug       ┆ Grimes    ┆ Doug Grimes     ┆ 23     ┆ F                     │
    │ 5188526 ┆ Jack       ┆ Harvey    ┆ Jack Harvey     ┆ 12     ┆ F                     │
    └─────────┴────────────┴───────────┴─────────────────┴────────┴───────────────────────┘


---

### 👩 Women's college hockey: scoreboard, teams & schedule

The `wch` module is the women's mirror. The NCAA Women's Frozen Four runs
in late March. Wisconsin is the perennial power — 12 national championships.


```python
# NCAA Women's Frozen Four 2024
board_wch = safe(
    "women's college hockey scoreboard (parsed)",
    lambda: wch.espn_wch_scoreboard(dates=WCH_DATE),
)
if board_wch is not None and getattr(board_wch, 'height', 0):
    keep = [c for c in board_wch.columns
            if c in ('id', 'name', 'short_name', 'status_type_description',
                     'home_team_abbreviation', 'home_score',
                     'away_team_abbreviation', 'away_score')]
    display(board_wch.select(keep).head() if keep else board_wch.head())
else:
    print("women's college hockey scoreboard unavailable")
```

    ✅ women's college hockey scoreboard (parsed)



    shape: (2, 5)
    ┌─────────────────────────────────┬────────────┬─────────────────────────┬────────────┬────────────┐
    │ name                            ┆ short_name ┆ status_type_description ┆ home_score ┆ away_score │
    │ ---                             ┆ ---        ┆ ---                     ┆ ---        ┆ ---        │
    │ str                             ┆ str        ┆ str                     ┆ str        ┆ str        │
    ╞═════════════════════════════════╪════════════╪═════════════════════════╪════════════╪════════════╡
    │ Clarkson Golden Knights at Ohi… ┆ CLAR @ OSU ┆ Final                   ┆ 4          ┆ 1          │
    │ Colgate Raiders at Wisconsin B… ┆ COLG @ WIS ┆ Final                   ┆ 3          ┆ 1          │
    └─────────────────────────────────┴────────────┴─────────────────────────┴────────────┴────────────┘



```python
# Women's college hockey teams
teams_wch = safe(
    "women's college hockey teams",
    lambda: wch.espn_wch_teams_site(),
)
if teams_wch is not None and teams_wch.height:
    keep = [c for c in teams_wch.columns
            if c in ('id', 'abbreviation', 'display_name', 'location', 'color')]
    print("Total women's D-I programs:", teams_wch.height)
    display(teams_wch.select(keep).head(10) if keep else teams_wch.head(10))
else:
    print('teams unavailable')
```

    ✅ women's college hockey teams
    Total women's D-I programs: 47



    shape: (10, 12)
    ┌────────────┬───────────┬─────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
    │ team_abbre ┆ team_disp ┆ team_id ┆ team_is_a ┆ … ┆ team_nick ┆ team_shor ┆ team_slug ┆ team_uid  │
    │ viation    ┆ lay_name  ┆ ---     ┆ ctive     ┆   ┆ name      ┆ t_display ┆ ---       ┆ ---       │
    │ ---        ┆ ---       ┆ i64     ┆ ---       ┆   ┆ ---       ┆ _name     ┆ str       ┆ str       │
    │ str        ┆ str       ┆         ┆ bool      ┆   ┆ str       ┆ ---       ┆           ┆           │
    │            ┆           ┆         ┆           ┆   ┆           ┆ str       ┆           ┆           │
    ╞════════════╪═══════════╪═════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ ASP        ┆ Assumptio ┆ 127962  ┆ true      ┆ … ┆ Assumptio ┆ Assumptio ┆ assumptio ┆ s:70~l:92 │
    │            ┆ n Greyhou ┆         ┆           ┆   ┆ n         ┆ n         ┆ n-greyhou ┆ ~t:127962 │
    │            ┆ nds       ┆         ┆           ┆   ┆           ┆           ┆ nds       ┆           │
    │ BST        ┆ Bemidji   ┆ 132     ┆ true      ┆ … ┆ Bemidji   ┆ Bemidji   ┆ bemidji-s ┆ s:70~l:92 │
    │            ┆ State     ┆         ┆           ┆   ┆ State     ┆ State     ┆ tate-beav ┆ ~t:132    │
    │            ┆ Beavers   ┆         ┆           ┆   ┆           ┆           ┆ ers       ┆           │
    │ BC         ┆ Boston    ┆ 103     ┆ true      ┆ … ┆ Boston    ┆ Boston    ┆ boston-co ┆ s:70~l:92 │
    │            ┆ College   ┆         ┆           ┆   ┆ College   ┆ College   ┆ llege-eag ┆ ~t:103    │
    │            ┆ Eagles    ┆         ┆           ┆   ┆           ┆           ┆ les       ┆           │
    │ BU         ┆ Boston    ┆ 104     ┆ true      ┆ … ┆ Boston    ┆ Boston    ┆ boston-un ┆ s:70~l:92 │
    │            ┆ Universit ┆         ┆           ┆   ┆ Universit ┆ Universit ┆ iversity- ┆ ~t:104    │
    │            ┆ y         ┆         ┆           ┆   ┆ y         ┆ y         ┆ terriers  ┆           │
    │            ┆ Terriers  ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
    │ BRWN       ┆ Brown     ┆ 225     ┆ true      ┆ … ┆ Brown     ┆ Brown     ┆ brown-bea ┆ s:70~l:92 │
    │            ┆ Bears     ┆         ┆           ┆   ┆           ┆           ┆ rs        ┆ ~t:225    │
    │ CLAR       ┆ Clarkson  ┆ 2137    ┆ true      ┆ … ┆ Clarkson  ┆ Clarkson  ┆ clarkson- ┆ s:70~l:92 │
    │            ┆ Golden    ┆         ┆           ┆   ┆           ┆           ┆ golden-kn ┆ ~t:2137   │
    │            ┆ Knights   ┆         ┆           ┆   ┆           ┆           ┆ ights     ┆           │
    │ COLG       ┆ Colgate   ┆ 2142    ┆ true      ┆ … ┆ Colgate   ┆ Colgate   ┆ colgate-r ┆ s:70~l:92 │
    │            ┆ Raiders   ┆         ┆           ┆   ┆           ┆           ┆ aiders    ┆ ~t:2142   │
    │ COR        ┆ Cornell   ┆ 172     ┆ true      ┆ … ┆ Cornell   ┆ Cornell   ┆ cornell-b ┆ s:70~l:92 │
    │            ┆ Big Red   ┆         ┆           ┆   ┆           ┆           ┆ ig-red    ┆ ~t:172    │
    │ DART       ┆ Dartmouth ┆ 159     ┆ true      ┆ … ┆ Dartmouth ┆ Dartmouth ┆ dartmouth ┆ s:70~l:92 │
    │            ┆ Big Green ┆         ┆           ┆   ┆           ┆           ┆ -big-gree ┆ ~t:159    │
    │            ┆           ┆         ┆           ┆   ┆           ┆           ┆ n         ┆           │
    │ FP         ┆ Franklin  ┆ 121934  ┆ true      ┆ … ┆ Franklin  ┆ Franklin  ┆ franklin- ┆ s:70~l:92 │
    │            ┆ Pierce    ┆         ┆           ┆   ┆ Pierce    ┆ Pierce    ┆ pierce-ra ┆ ~t:121934 │
    │            ┆ Ravens    ┆         ┆           ┆   ┆           ┆           ┆ vens      ┆           │
    └────────────┴───────────┴─────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘



```python
# Wisconsin Badgers women's hockey schedule 2023-24 (team_id=275)
schedule_wch = safe(
    "women's college hockey team schedule",
    lambda: wch.espn_wch_team_schedule(team_id=275, season=SAMPLE_SEASON,
                                       ),
)
if schedule_wch is not None and schedule_wch.height:
    keep = [c for c in schedule_wch.columns
            if c in ('id', 'name', 'short_name',
                     'home_team_abbreviation', 'away_team_abbreviation',
                     'home_score', 'away_score', 'status_type_description')]
    display(schedule_wch.select(keep).head(8) if keep else schedule_wch.head(8))
else:
    print('schedule unavailable')
```

    ✅ women's college hockey team schedule



    shape: (3, 3)
    ┌───────────┬─────────────────────────────────┬────────────┐
    │ id        ┆ name                            ┆ short_name │
    │ ---       ┆ ---                             ┆ ---        │
    │ i64       ┆ str                             ┆ str        │
    ╞═══════════╪═════════════════════════════════╪════════════╡
    │ 401647743 ┆ St. Lawrence Saints at Wiscons… ┆ USL @ WIS  │
    │ 401647740 ┆ Colgate Raiders at Wisconsin B… ┆ COLG @ WIS │
    │ 401647741 ┆ Wisconsin Badgers at Ohio Stat… ┆ WIS @ OSU  │
    └───────────┴─────────────────────────────────┴────────────┘



```python
# Women's college hockey rankings
rankings_wch = safe(
    "women's college hockey rankings",
    lambda: wch.espn_wch_rankings(),
)
if rankings_wch is not None and rankings_wch.height:
    keep = [c for c in rankings_wch.columns
            if c in ('rank', 'team_id', 'team_name', 'points',
                     'first_place_votes', 'wins', 'losses', 'ties')]
    display(rankings_wch.select(keep).head(10) if keep else rankings_wch.head(10))
else:
    print("rankings unavailable (only current during the season)")
```

    ✅ women's college hockey rankings



    shape: (10, 5)
    ┌─────────┬──────┬────────┬───────────────────┬────────────────┐
    │ team_id ┆ rank ┆ points ┆ first_place_votes ┆ team_name      │
    │ ---     ┆ ---  ┆ ---    ┆ ---               ┆ ---            │
    │ i64     ┆ i64  ┆ f64    ┆ i64               ┆ str            │
    ╞═════════╪══════╪════════╪═══════════════════╪════════════════╡
    │ 194     ┆ 1    ┆ 0.0    ┆ null              ┆ Buckeyes       │
    │ 135     ┆ 2    ┆ 0.0    ┆ null              ┆ Golden Gophers │
    │ 275     ┆ 3    ┆ 0.0    ┆ null              ┆ Badgers        │
    │ 111     ┆ 4    ┆ 0.0    ┆ null              ┆ Huskies        │
    │ 163     ┆ 5    ┆ 0.0    ┆ null              ┆ Tigers         │
    │ 2514    ┆ 6    ┆ 0.0    ┆ null              ┆ Bobcats        │
    │ 213     ┆ 7    ┆ 0.0    ┆ null              ┆ Nittany Lions  │
    │ 2142    ┆ 8    ┆ 0.0    ┆ null              ┆ Raiders        │
    │ 2137    ┆ 9    ┆ 0.0    ┆ null              ┆ Golden Knights │
    │ 2594    ┆ 10   ┆ 0.0    ┆ null              ┆ Huskies        │
    └─────────┴──────┴────────┴───────────────────┴────────────────┘


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
    df = safe(f'{league_name} teams', lambda fn=fn: fn())
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
    display(all_teams.group_by('league').len().sort('len', descending=True))
else:
    print('no team data available right now')
```

    ✅ ufl teams
    ✅ xfl teams
    ✅ cfl teams
    ✅ college_baseball teams


    ✅ college_softball teams
    ✅ mch teams
    ✅ wch teams
    Total teams across all 7 leagues: 1067



    shape: (7, 2)
    ┌──────────────────┬─────┐
    │ league           ┆ len │
    │ ---              ┆ --- │
    │ str              ┆ u32 │
    ╞══════════════════╪═════╡
    │ college_softball ┆ 446 │
    │ college_baseball ┆ 437 │
    │ mch              ┆ 116 │
    │ wch              ┆ 47  │
    │ cfl              ┆ 9   │
    │ ufl              ┆ 8   │
    │ xfl              ┆ 4   │
    └──────────────────┴─────┘


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
    lambda: cfl.espn_cfl_scoreboard(dates=today_str),
)
if todays_cfl is not None and getattr(todays_cfl, 'height', 0):
    print(f"{todays_cfl.height} game(s) today")
    display(todays_cfl.head())
else:
    print('no CFL games today (or offseason)')
```

    ✅ today's CFL slate (20261102)
    no CFL games today (or offseason)


### Recipe 3 — Player info for any athlete 🧑‍💻

Every league exposes `espn_{prefix}_player_info(athlete_id=)`. Find an
`athlete_id` via the roster endpoint, then pull richer bio / career-stats
detail with `espn_{prefix}_player_career_stats(athlete_id=)`. When the site page is missing,
as it is for many college players, `espn_{prefix}_player_core(athlete_id=)` has the core record.


```python
# ESPN's site v2 athlete page is missing for many college players, so try a few roster ids
roster_ids = (roster_mch['id'].head(5).to_list()
              if roster_mch is not None and 'id' in roster_mch.columns else [])
player_info = None
for athlete_id in roster_ids:
    try:
        player_info = mch.espn_mch_player_info(athlete_id=athlete_id)
        print(f'✅ MCH player info (id={athlete_id})')
        break
    except NoDataError:
        continue
if roster_ids and player_info is None:
    print(f'No site v2 athlete page for the first {len(roster_ids)} roster players; showing the core v2 record')
    player_info = safe(f'MCH player core (id={roster_ids[0]})',
                       lambda: mch.espn_mch_player_core(athlete_id=roster_ids[0]))
elif not roster_ids:
    print('roster not loaded — skipping player info example')
player_info

```

    No site v2 athlete page for the first 5 roster players; showing the core v2 record
    ✅ MCH player core (id=5136656)





    shape: (1, 39)
    ┌────────────┬─────────┬────────────┬────────┬───┬────────────┬────────────┬───────────┬───────────┐
    │ $ref       ┆ id      ┆ uid        ┆ type   ┆ … ┆ status_nam ┆ status_typ ┆ status_ab ┆ statistic │
    │ ---        ┆ ---     ┆ ---        ┆ ---    ┆   ┆ e          ┆ e          ┆ breviatio ┆ slog_$ref │
    │ str        ┆ i64     ┆ str        ┆ str    ┆   ┆ ---        ┆ ---        ┆ n         ┆ ---       │
    │            ┆         ┆            ┆        ┆   ┆ str        ┆ str        ┆ ---       ┆ str       │
    │            ┆         ┆            ┆        ┆   ┆            ┆            ┆ str       ┆           │
    ╞════════════╪═════════╪════════════╪════════╪═══╪════════════╪════════════╪═══════════╪═══════════╡
    │ http://spo ┆ 5136656 ┆ s:70~l:91~ ┆ hockey ┆ … ┆ Active     ┆ active     ┆ Active    ┆ http://sp │
    │ rts.core.a ┆         ┆ a:5136656  ┆        ┆   ┆            ┆            ┆           ┆ orts.core │
    │ pi.espn.co ┆         ┆            ┆        ┆   ┆            ┆            ┆           ┆ .api.espn │
    │ …          ┆         ┆            ┆        ┆   ┆            ┆            ┆           ┆ .co…      │
    └────────────┴─────────┴────────────┴────────┴───┴────────────┴────────────┴───────────┴───────────┘



### Recipe 4 — News & injuries for any league 📰

[`espn_{prefix}_news()`](../ufl/reference/site.md#espn_ufl_news) and
[`espn_{prefix}_injuries()`](../ufl/reference/site.md#espn_ufl_injuries)
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

All seven leagues come out of **one** code generator. The architecture is worth
understanding because it makes every league behave identically:

```
tools/codegen/endpoints/espn_site_v2.yaml  # + web_v3 / core_v2 / fitt_v3 / cdn: one entry per short name
tools/codegen/endpoints/leagues.yaml       # one entry per league: sport, league, prefix, scopes
        └── tools/codegen/generate.py  →  espn_{prefix}_{short}

sportsdataverse/football/ufl/ufl_espn_ext.py  # generated, never hand-edited
sportsdataverse/football/xfl/xfl_espn_ext.py
sportsdataverse/football/cfl/cfl_espn_ext.py
sportsdataverse/baseball/college_baseball/college_baseball_espn_ext.py
sportsdataverse/baseball/college_softball/college_softball_espn_ext.py
sportsdataverse/hockey/mch/mch_espn_ext.py
sportsdataverse/hockey/wch/wch_espn_ext.py
```

This means:

- Any fix to an endpoint definition or the template reaches all 7 leagues at once.
- Every league has an identical `dir()` surface — the only difference
  is the prefix string (`ufl`, `xfl`, `cfl`, `college_baseball`, …).
- The `` path routes through `ENDPOINT_PARSERS` — a
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

    113 functions on sportsdataverse.football.ufl
    sample: ['espn_ufl_award', 'espn_ufl_awards', 'espn_ufl_calendar', 'espn_ufl_cdn_schedule', 'espn_ufl_coach', 'espn_ufl_coach_record', 'espn_ufl_coach_season', 'espn_ufl_conferences'] ...



```python
# Quick cross-league count
for name, mod in [('ufl', ufl), ('xfl', xfl), ('cfl', cfl),
                  ('college_baseball', cbb), ('college_softball', cbs),
                  ('mch', mch), ('wch', wch)]:
    n = len([f for f in dir(mod) if f.startswith('espn_')])
    print(f'  {name:20s}: {n} wrappers')
```

      ufl                 : 113 wrappers
      xfl                 : 112 wrappers
      cfl                 : 112 wrappers
      college_baseball    : 122 wrappers
      college_softball    : 121 wrappers
      mch                 : 118 wrappers
      wch                 : 118 wrappers


---

## 🔗 See Also

- **Spring / pro football reference docs:**
  [UFL](../ufl/reference/site.md) ·
  [XFL](../xfl/reference/site.md) ·
  [CFL](../cfl/reference/site.md)
- **College baseball / softball reference docs:**
  [College Baseball](../college_baseball/reference/site.md) ·
  [College Softball](../college_softball/reference/site.md)
- **NCAA hockey reference docs:**
  [Men's College Hockey](../mch/reference/site.md) ·
  [Women's College Hockey](../wch/reference/site.md)
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
