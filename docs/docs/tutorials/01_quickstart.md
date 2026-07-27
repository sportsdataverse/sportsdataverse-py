---
title: Quickstart tutorial
sidebar_label: Quickstart
sidebar_position: 1
---

# 🏟️ Welcome to `sportsdataverse-py` — the cross-sport quickstart

One `pip install`, **every** major league. `sportsdataverse` is a single Python
package that speaks to the official, *premium* native data feeds across the
sporting world — the same endpoints the leagues use to power their own apps —
plus the **ESPN** mirror and pre-built **parquet release** loaders. Everything
comes back as a tidy **polars** DataFrame, ready to model. 🚀

This page is your **map** to the whole package. By the end you'll be able to:

1. 🗺️ **see every datasource** available for every league, with links straight
   to its tutorial and its reference index;
2. 🧭 **predict function names you've never seen** — sportsdataverse uses one
   consistent naming contract, so *knowing one function tells you the others*;
3. 🍳 cook through **~20 cross-sport recipes** that show the breadth in action.

If you've used the R sisters — **hoopR, wehoop, cfbfastR, baseballr,
fastRhockey, oddsapiR** — the names here will feel like home. Let's take the
tour! 😊

## 🗺️ 1 · The master index — every datasource, every league

Here's the whole package on one page. Each row is a league (or the betting-odds
module); each cell tells you which **datasource families** are wired up. 💳 marks
the **premium** native feeds (the leagues' own APIs / tracking systems / Statcast).
Click a league's **tutorial** for the deep dive, or its **reference** for the full
function index.

| League | Tutorial · Reference | ESPN (`espn_<lg>_*`) | Native premium API | Tracking / analytics | Release loaders (`load_*`) |
|---|---|:---:|---|---|---|
| 🏀 **NBA** | [tutorial](../tutorials/04_nba_intro.md) · [ref](../nba/index.md) | ✅ | — | — | `load_nba_pbp`, `load_nba_team_boxscore` |
| 🏀 **WNBA** | [tutorial](../tutorials/08_wnba_intro.md) · [ref](../wnba/index.md) | ✅ | — | — | `load_wnba_pbp`, `load_wnba_player_boxscore` |
| 🏀 **MBB** (NCAA M) | [tutorial](../tutorials/06_mbb_intro.md) · [ref](../mbb/index.md) | ✅ | — | — | `load_mbb_pbp`, `load_mbb_team_boxscore` |
| 🏀 **WBB** (NCAA W) | [tutorial](../tutorials/05_wbb_intro.md) · [ref](../wbb/index.md) | ✅ | — | — | `load_wbb_pbp`, `load_wbb_team_boxscore` |
| 🏈 **NFL** | [tutorial](../tutorials/03_nfl_intro.md) · [ref](../nfl/index.md) | ✅ | 💳 `nfl_*` (`api.nfl.com`) | 💳 Next Gen Stats `nfl_ngs_*` | `load_nfl_pbp`, `load_nfl_player_stats`, `load_injuries` |
| 🏈 **CFB** (College) | [tutorial](../tutorials/02_cfb_intro.md) · [ref](../cfb/index.md) | ✅ | `yahoo_cfb_*`, `fox_cfb_*` | — | `load_cfb_pbp` |
| ⚾ **MLB** | [tutorial](../tutorials/09_mlb_intro.md) · [ref](../mlb/index.md) | ✅ | 💳 `mlb_*` (MLB Stats API) | 💳 Statcast `mlb_statcast_*` | `load_mlb_pbp`, `load_mlb_team_boxscore` |
| 🏒 **NHL** | [tutorial](../tutorials/07_nhl_intro.md) · [ref](../nhl/index.md) | ✅ | 💳 `nhl_*` (`api-web`) | 💳 NHL EDGE `nhl_edge_*` | `load_nhl_pbp`, `load_nhl_team_boxscore` |
| 🏒 **PWHL** (Women's pro) | [tutorial](../tutorials/10_pwhl_intro.md) · [ref](../pwhl/index.md) | — | 💳 `pwhl_*` (HockeyTech) | corsi / shifts / TOI | `load_pwhl_schedules` |
| 🏒 **AHL** (Minor pro) | [tutorial](../tutorials/11_junior_hockey_intro.md) · [ref](../ahl/index.md) | — | 💳 `ahl_*` (HockeyTech) | corsi / shifts / TOI | — |
| 🏒 **OHL** (CHL junior) | [tutorial](../tutorials/11_junior_hockey_intro.md) · [ref](../ohl/index.md) | — | 💳 `ohl_*` (HockeyTech) | corsi / shifts / TOI | — |
| 🏒 **WHL** (CHL junior) | [tutorial](../tutorials/11_junior_hockey_intro.md) · [ref](../whl/index.md) | — | 💳 `whl_*` (HockeyTech) | corsi / shifts / TOI | — |
| 🏒 **QMJHL** (CHL junior) | [tutorial](../tutorials/11_junior_hockey_intro.md) · [ref](../qmjhl/index.md) | — | 💳 `qmjhl_*` (HockeyTech) | corsi / shifts / TOI | — |
| 🎲 **Betting odds** | [tutorial](../tutorials/12_odds_intro.md) · [ref](../odds/index.md) | — | 💳 `toa_*` (The Odds API) | line history / props | — |

> 💡 *HockeyTech leagues* (AHL/OHL/WHL/QMJHL/PWHL) ship public client keys — **no
> setup needed**. Only the betting-odds module wants a free `ODDS_API_KEY`.

### 🧩 The five function styles

Across all those rows, only **five families** exist. Learn the shape of each
once and you can read any function name in the package:

1. **Live ESPN wrappers** — `espn_<lg>_*` (e.g. `espn_nba_teams`,
   `espn_wbb_scoreboard`). The *same* set exists for every ESPN league: teams,
   rosters, scoreboards, standings, schedules, play-by-play, box scores. 🪞
2. **Native premium API wrappers** — the league's own feed: `nfl_*` (`api.nfl.com`),
   `mlb_*` (MLB Stats API), `nhl_*` (`api-web`), `pwhl_*`/`ahl_*`/`ohl_*`/`whl_*`/`qmjhl_*`
   (HockeyTech), `toa_*` (The Odds API). 💳
3. **Tracking / analytics feeds** — the *really* premium stuff: `mlb_statcast_*`
   (Baseball Savant), `nhl_edge_*` (player tracking), `nfl_ngs_*` (Next Gen Stats).
4. **Release / parquet loaders** — `load_<sport>_*()` reads a pre-built parquet
   release (fast, reliable, whole-season-at-once): `load_nba_pbp`,
   `load_mlb_team_boxscore`, `load_pwhl_schedules`, …
5. **Parser layer** — `parse_*` turns a raw native payload into a tidy frame
   (e.g. `parse_mlb_api_standings`). Most wrappers parse for you; the parsers are
   there when you fetch the raw `Dict` yourself.

**The return contract never changes.** Every wrapper gives you **polars by
default**; pass `return_as_pandas=True` for a pandas frame, and on the native
APIs pass `return_parsed=False` for the raw JSON `Dict`. One contract, every
sport. 🎛️

## 🔌 Setup

```sh
pip install sportsdataverse
# or
uv add sportsdataverse
```

Every league is a submodule of the umbrella package, and the headline cross-league
wrappers + discovery helpers are re-exported at the top level. Let's import it.


```python
import os
import polars as pl
import sportsdataverse as sdv
import sportsdataverse.odds as odds

# Every league hangs off the top-level package:
[m for m in dir(sdv) if m in
 ("cfb", "nfl", "nba", "wnba", "mbb", "wbb", "nhl", "mlb", "pwhl",
  "ahl", "ohl", "whl", "qmjhl", "odds")]
```




    ['cfb', 'mbb', 'mlb', 'nba', 'nfl', 'nhl', 'odds', 'pwhl', 'wbb', 'wnba']



Live endpoints are seasonal and occasionally rate-limited, and the
naming-convention loops below fan out **many** live calls at once — so a tiny
`safe()` helper runs every network call defensively. You get the frame when the
feed is up, and a friendly one-liner when it isn't — never a scary traceback.
That keeps this whole page runnable offline or in the off-season. 🛟


```python
def safe(label, thunk):
    '''Run a live call; return its result, or print a one-liner and return None.'''
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None


# Odds is the only module that wants a (free) key — guard those cells:
HAS_KEY = bool(os.environ.get("ODDS_API_KEY"))
print("ODDS_API_KEY set:", HAS_KEY,
      "— odds cells will" + ("" if HAS_KEY else " NOT") + " run live")
```

    ODDS_API_KEY set: True — odds cells will run live


## 🧭 2 · The naming-convention superpower

Here's the centerpiece. **sportsdataverse names things so predictably that
knowing one function name tells you the others.** The *same style* of data is
exactly one rename away across every sport — swap the league slug and the call
just works. Let's prove it. 🪄

### 🪞 The ESPN families are identical across every league

`espn_<lg>_teams`, `espn_<lg>_team_roster`, `espn_<lg>_scoreboard`,
`espn_<lg>_standings` exist for **every** ESPN league. A one-line helper +
`getattr` tours them all and returns the **same shape** each time.


```python
def teams(league):
    '''Knowing one name (espn_<lg>_teams) gives you all of them.'''
    return getattr(sdv, f"espn_{league}_teams")()

rows = []
for lg in ["nba", "wnba", "nhl", "mlb"]:
    df = safe(f"espn_{lg}_teams", lambda lg=lg: teams(lg))
    rows.append({"league": lg.upper(),
                 "fn": f"espn_{lg}_teams()",
                 "n_teams": None if df is None else df.height,
                 "n_cols": None if df is None else df.width})

pl.DataFrame(rows)  # same columns, same shape — one contract, four leagues
```

    ✅ espn_nba_teams
    ✅ espn_wnba_teams


    ✅ espn_nhl_teams


    ✅ espn_mlb_teams





    shape: (4, 4)
    ┌────────┬───────────────────┬─────────┬────────┐
    │ league ┆ fn                ┆ n_teams ┆ n_cols │
    │ ---    ┆ ---               ┆ ---     ┆ ---    │
    │ str    ┆ str               ┆ i64     ┆ i64    │
    ╞════════╪═══════════════════╪═════════╪════════╡
    │ NBA    ┆ espn_nba_teams()  ┆ 30      ┆ 14     │
    │ WNBA   ┆ espn_wnba_teams() ┆ 15      ┆ 14     │
    │ NHL    ┆ espn_nhl_teams()  ┆ 32      ┆ 14     │
    │ MLB    ┆ espn_mlb_teams()  ┆ 30      ┆ 14     │
    └────────┴───────────────────┴─────────┴────────┘



Same trick for the **scoreboard** and **standings** families — the call is
identical, only the slug changes.


```python
def call(family, league, **kw):
    '''Generic dispatcher: call("scoreboard", "nhl") -> espn_nhl_scoreboard().'''
    return getattr(sdv, f"espn_{league}_{family}")(**kw)

board = safe("espn_nfl_scoreboard", lambda: call("scoreboard", "nfl"))
stand = safe("espn_nba_standings", lambda: call("standings", "nba"))
print("NFL scoreboard rows:", None if board is None else board.height,
      "| NBA standings rows:", None if stand is None else getattr(stand, "height", None))
```

    ✅ espn_nfl_scoreboard
    ✅ espn_nba_standings
    NFL scoreboard rows: 1 | NBA standings rows: 30


### 📦 The loaders follow one pattern too

`load_<sport>_pbp` and `load_<sport>_team_boxscore` read pre-built parquet for
**every** sport — same signature (`seasons=[...]`), same return type. Knowing
`load_nba_pbp` means you already know `load_nhl_pbp` and `load_mlb_pbp`.


```python
# A single getattr loop loads play-by-play for four different sports:
season = 2024
for sport in ["nba", "wnba", "nhl"]:
    fn = getattr(sdv, f"load_{sport}_pbp")
    print(f"load_{sport}_pbp(seasons=[{season}])  ->  signature is identical for every sport")
# (we don't pull all of them here — that's a lot of parquet; Recipe 3 runs one.)
```

    load_nba_pbp(seasons=[2024])  ->  signature is identical for every sport
    load_wnba_pbp(seasons=[2024])  ->  signature is identical for every sport
    load_nhl_pbp(seasons=[2024])  ->  signature is identical for every sport


### 🏒 The HockeyTech leagues share one surface

AHL / OHL / WHL / QMJHL / PWHL all expose `<lg>_schedule`, `<lg>_standings`,
`<lg>_teams`, `<lg>_team_roster`, and `most_recent_<lg>_season`. Learn one, you
learned all five.


```python
import sportsdataverse.hockey.ahl as ahl
import sportsdataverse.hockey.ohl as ohl
import sportsdataverse.hockey.whl as whl
import sportsdataverse.hockey.qmjhl as qmjhl
import sportsdataverse.pwhl as pwhl

HOCKEYTECH = {"ahl": ahl, "ohl": ohl, "whl": whl, "qmjhl": qmjhl, "pwhl": pwhl}

rows = []
for lg, mod in HOCKEYTECH.items():
    season = safe(f"most_recent_{lg}_season", getattr(mod, f"most_recent_{lg}_season"))
    rows.append({"league": lg.upper(),
                 "schedule_fn": f"{lg}_schedule()",
                 "standings_fn": f"{lg}_standings()",
                 "season": season})
pl.DataFrame(rows)
```

    ✅ most_recent_ahl_season


    ✅ most_recent_ohl_season


    ✅ most_recent_whl_season


    ✅ most_recent_qmjhl_season


    ✅ most_recent_pwhl_season





    shape: (5, 4)
    ┌────────┬──────────────────┬───────────────────┬────────┐
    │ league ┆ schedule_fn      ┆ standings_fn      ┆ season │
    │ ---    ┆ ---              ┆ ---               ┆ ---    │
    │ str    ┆ str              ┆ str               ┆ i64    │
    ╞════════╪══════════════════╪═══════════════════╪════════╡
    │ AHL    ┆ ahl_schedule()   ┆ ahl_standings()   ┆ 2027   │
    │ OHL    ┆ ohl_schedule()   ┆ ohl_standings()   ┆ 2027   │
    │ WHL    ┆ whl_schedule()   ┆ whl_standings()   ┆ 2026   │
    │ QMJHL  ┆ qmjhl_schedule() ┆ qmjhl_standings() ┆ 2027   │
    │ PWHL   ┆ pwhl_schedule()  ┆ pwhl_standings()  ┆ 2027   │
    └────────┴──────────────────┴───────────────────┴────────┘



### 🔎 Discovery helpers — when you don't know the name yet

Four top-level helpers let you *search* the surface instead of guessing:

- `list_functions(league=None, search=..., parsers_only=..., wrappers_only=...)` — list/search every wrapper.
- `function_count(league=None)` — how many functions each league exposes.
- `find_team(name, league)` — fuzzy team lookup (returns the ESPN team dict + `id`).
- `find_athlete(name, league)` — fuzzy player lookup.


```python
# What does the package know about "scoreboard"? (grouped by league)
hits = sdv.list_functions(search="scoreboard")
for lg, fns in hits.items():
    print(f"{lg:>4}: {', '.join(fns)}")
```

     cfb: espn_cfb_scoreboard, scoreboard_event_parsing, yahoo_cfb_scoreboard
     mbb: espn_mbb_scoreboard, parse_ncaa_bb_scoreboard, scoreboard_event_parsing
     mlb: espn_mlb_scoreboard
     nba: espn_nba_scoreboard, scoreboard_event_parsing
     nfl: espn_nfl_scoreboard, scoreboard_event_parsing
     nhl: espn_nhl_scoreboard, nhl_scoreboard, parse_nhl_web_scoreboard, scoreboard_event_parsing
     wbb: espn_wbb_scoreboard, scoreboard_event_parsing
    wnba: espn_wnba_scoreboard, scoreboard_event_parsing
    soccer: espn_soccer_scoreboard
    cricket: espn_cricket_scoreboard
     epl: espn_epl_scoreboard
    laliga: espn_laliga_scoreboard
    bundesliga: espn_bundesliga_scoreboard
    seriea: espn_seriea_scoreboard
    ligue1: espn_ligue1_scoreboard
     mls: espn_mls_scoreboard
    ligamx: espn_ligamx_scoreboard
     ucl: espn_ucl_scoreboard
     uel: espn_uel_scoreboard
    nwsl: espn_nwsl_scoreboard
     wwc: espn_wwc_scoreboard
      wc: espn_wc_scoreboard
     mch: espn_mch_scoreboard
     wch: espn_wch_scoreboard
     ufl: espn_ufl_scoreboard
     xfl: espn_xfl_scoreboard
     cfl: espn_cfl_scoreboard
    college_baseball: espn_college_baseball_scoreboard
    college_softball: espn_college_softball_scoreboard



```python
# How big is each league's surface?
counts = sdv.function_count()
pl.DataFrame({"league": list(counts.keys()), "n_functions": list(counts.values())}) \
  .sort("n_functions", descending=True)
```




    shape: (34, 2)
    ┌────────┬─────────────┐
    │ league ┆ n_functions │
    │ ---    ┆ ---         │
    │ str    ┆ i64         │
    ╞════════╪═════════════╡
    │ mbb    ┆ 547         │
    │ wbb    ┆ 508         │
    │ nhl    ┆ 383         │
    │ cfb    ┆ 377         │
    │ mlb    ┆ 330         │
    │ …      ┆ …           │
    │ pwhl   ┆ 68          │
    │ ahl    ┆ 14          │
    │ ohl    ┆ 14          │
    │ qmjhl  ┆ 14          │
    │ whl    ┆ 14          │
    └────────┴─────────────┘




```python
# Fuzzy lookups — no IDs to memorize:
team = sdv.find_team("Lakers", "nba")
ath = sdv.find_athlete("LeBron", "nba")
print("team  ->", None if team is None else f"{team['displayName']} (id={team['id']})")
print("athlete ->", None if ath is None else f"{ath['displayName']} (id={ath['id']})")
```

    team  -> Los Angeles Lakers (id=13)
    athlete -> LeBron James (id=1966)


## 🍳 3 · Twenty cross-sport recipes

Now the fun part — **20 runnable recipes** that show the breadth *and* the
overlap. Every recipe is defensively guarded, so a flaky network or off-season
just prints a friendly note instead of erroring. Mix, match, and remix. 👇

### Recipe 1 — Any league's teams 🪞

`teams("<lg>")` (our helper from above) hits `espn_<lg>_teams` for any ESPN
league. Here's the WBB team list.


```python
wbb_teams = safe("espn_wbb_teams", lambda: teams("wbb"))
cols = ["team_id", "team_abbreviation", "team_display_name", "team_location"]
(wbb_teams.select([c for c in cols if c in wbb_teams.columns]).head()
 if wbb_teams is not None and wbb_teams.height else "WBB teams unavailable right now")
```

    ✅ espn_wbb_teams





    shape: (5, 4)
    ┌─────────┬───────────────────┬────────────────────────────┬───────────────────┐
    │ team_id ┆ team_abbreviation ┆ team_display_name          ┆ team_location     │
    │ ---     ┆ ---               ┆ ---                        ┆ ---               │
    │ str     ┆ str               ┆ str                        ┆ str               │
    ╞═════════╪═══════════════════╪════════════════════════════╪═══════════════════╡
    │ 2000    ┆ ACU               ┆ Abilene Christian Wildcats ┆ Abilene Christian │
    │ 2005    ┆ AF                ┆ Air Force Falcons          ┆ Air Force         │
    │ 2006    ┆ AKR               ┆ Akron Zips                 ┆ Akron             │
    │ 2010    ┆ AAMU              ┆ Alabama A&M Bulldogs       ┆ Alabama A&M       │
    │ 333     ┆ ALA               ┆ Alabama Crimson Tide       ┆ Alabama           │
    └─────────┴───────────────────┴────────────────────────────┴───────────────────┘



### Recipe 2 — Any league's scoreboard 📋

`espn_<lg>_scoreboard()` returns today's slate as a tidy frame. Same call for
MLB, NBA, NHL — just change the slug.


```python
sb = safe("espn_mlb_scoreboard", lambda: sdv.espn_mlb_scoreboard())
(sb.head() if sb is not None and getattr(sb, "height", 0)
 else "no MLB games on the board right now")
```

    ✅ espn_mlb_scoreboard





    shape: (5, 50)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ game_id   ┆ uid       ┆ date      ┆ name      ┆ … ┆ away_logo ┆ away_scor ┆ away_winn ┆ away_ran │
    │ ---       ┆ ---       ┆ ---       ┆ ---       ┆   ┆ ---       ┆ e         ┆ er        ┆ k        │
    │ str       ┆ str       ┆ str       ┆ str       ┆   ┆ str       ┆ ---       ┆ ---       ┆ ---      │
    │           ┆           ┆           ┆           ┆   ┆           ┆ str       ┆ str       ┆ str      │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ 401816288 ┆ s:1~l:10~ ┆ 2026-07-2 ┆ Seattle   ┆ … ┆ https://a ┆ 0         ┆ null      ┆ null     │
    │           ┆ e:4018162 ┆ 7T18:35Z  ┆ Mariners  ┆   ┆ .espncdn. ┆           ┆           ┆          │
    │           ┆ 88        ┆           ┆ at Texas  ┆   ┆ com/i/tea ┆           ┆           ┆          │
    │           ┆           ┆           ┆ Rang…     ┆   ┆ mlo…      ┆           ┆           ┆          │
    │ 401816280 ┆ s:1~l:10~ ┆ 2026-07-2 ┆ Arizona   ┆ … ┆ https://a ┆ 0         ┆ null      ┆ null     │
    │           ┆ e:4018162 ┆ 7T22:40Z  ┆ Diamondba ┆   ┆ .espncdn. ┆           ┆           ┆          │
    │           ┆ 80        ┆           ┆ cks at    ┆   ┆ com/i/tea ┆           ┆           ┆          │
    │           ┆           ┆           ┆ Pittsb…   ┆   ┆ mlo…      ┆           ┆           ┆          │
    │ 401816282 ┆ s:1~l:10~ ┆ 2026-07-2 ┆ Baltimore ┆ … ┆ https://a ┆ 0         ┆ null      ┆ null     │
    │           ┆ e:4018162 ┆ 7T22:40Z  ┆ Orioles   ┆   ┆ .espncdn. ┆           ┆           ┆          │
    │           ┆ 82        ┆           ┆ at        ┆   ┆ com/i/tea ┆           ┆           ┆          │
    │           ┆           ┆           ┆ Detroit   ┆   ┆ mlo…      ┆           ┆           ┆          │
    │           ┆           ┆           ┆ T…        ┆   ┆           ┆           ┆           ┆          │
    │ 401816284 ┆ s:1~l:10~ ┆ 2026-07-2 ┆ Philadelp ┆ … ┆ https://a ┆ 0         ┆ null      ┆ null     │
    │           ┆ e:4018162 ┆ 7T22:40Z  ┆ hia       ┆   ┆ .espncdn. ┆           ┆           ┆          │
    │           ┆ 84        ┆           ┆ Phillies  ┆   ┆ com/i/tea ┆           ┆           ┆          │
    │           ┆           ┆           ┆ at Miami… ┆   ┆ mlo…      ┆           ┆           ┆          │
    │ 401816285 ┆ s:1~l:10~ ┆ 2026-07-2 ┆ Toronto   ┆ … ┆ https://a ┆ 0         ┆ null      ┆ null     │
    │           ┆ e:4018162 ┆ 7T22:45Z  ┆ Blue Jays ┆   ┆ .espncdn. ┆           ┆           ┆          │
    │           ┆ 85        ┆           ┆ at Washin ┆   ┆ com/i/tea ┆           ┆           ┆          │
    │           ┆           ┆           ┆ gto…      ┆   ┆ mlo…      ┆           ┆           ┆          │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘



### Recipe 3 — Load any sport's season play-by-play 📦

`load_<sport>_pbp(seasons=[...])` reads the parquet release. One sport here
(WNBA, a smaller season) to keep the download light.


```python
wnba_pbp = safe("load_wnba_pbp([2024])", lambda: sdv.load_wnba_pbp(seasons=[2024]))
print("WNBA 2024 pbp rows:", None if wnba_pbp is None else wnba_pbp.height)
(wnba_pbp.select([c for c in ["game_id", "period_number", "clock_display_value", "text"]
                  if c in wnba_pbp.columns]).head()
 if wnba_pbp is not None and wnba_pbp.height else "pbp unavailable right now")
```

    ✅ load_wnba_pbp([2024])
    WNBA 2024 pbp rows: 101501





    shape: (5, 4)
    ┌───────────┬───────────────┬─────────────────────┬─────────────────────────────────┐
    │ game_id   ┆ period_number ┆ clock_display_value ┆ text                            │
    │ ---       ┆ ---           ┆ ---                 ┆ ---                             │
    │ i32       ┆ i32           ┆ str                 ┆ str                             │
    ╞═══════════╪═══════════════╪═════════════════════╪═════════════════════════════════╡
    │ 401726992 ┆ 1             ┆ 10:00               ┆ Napheesa Collier vs. Jonquel J… │
    │ 401726992 ┆ 1             ┆ 9:35                ┆ Napheesa Collier makes 3-foot … │
    │ 401726992 ┆ 1             ┆ 9:12                ┆ Sabrina Ionescu misses 24-foot… │
    │ 401726992 ┆ 1             ┆ 9:09                ┆ Bridget Carleton defensive reb… │
    │ 401726992 ┆ 1             ┆ 8:55                ┆ Betnijah Laney-Hamilton person… │
    └───────────┴───────────────┴─────────────────────┴─────────────────────────────────┘



### Recipe 4 — The same box-score shape for two different sports 🪞

`load_<sport>_team_boxscore` returns the same *kind* of frame for basketball and
hockey. Load one season of each and compare the shapes.


```python
nba_box = safe("load_nba_team_boxscore([2024])", lambda: sdv.load_nba_team_boxscore(seasons=[2024]))
nhl_box = safe("load_nhl_team_boxscore([2024])", lambda: sdv.load_nhl_team_boxscore(seasons=[2024]))
print("NBA team-box shape:", None if nba_box is None else nba_box.shape)
print("NHL team-box shape:", None if nhl_box is None else nhl_box.shape)
```

    ✅ load_nba_team_boxscore([2024])


    ✅ load_nhl_team_boxscore([2024])
    NBA team-box shape: (2640, 57)
    NHL team-box shape: (2800, 19)


### Recipe 5 — Standings for several leagues at once 🔁

One loop over `espn_<lg>_standings` tours basketball, hockey, and baseball.


```python
rows = []
for lg in ["nba", "nhl", "mlb"]:
    df = safe(f"espn_{lg}_standings", lambda lg=lg: getattr(sdv, f"espn_{lg}_standings")())
    rows.append({"league": lg.upper(),
                 "rows": None if df is None else getattr(df, "height", None),
                 "cols": None if df is None else getattr(df, "width", None)})
pl.DataFrame(rows)
```

    ✅ espn_nba_standings


    ✅ espn_nhl_standings
    ✅ espn_mlb_standings





    shape: (3, 3)
    ┌────────┬──────┬──────┐
    │ league ┆ rows ┆ cols │
    │ ---    ┆ ---  ┆ ---  │
    │ str    ┆ i64  ┆ i64  │
    ╞════════╪══════╪══════╡
    │ NBA    ┆ 30   ┆ 31   │
    │ NHL    ┆ 32   ┆ 35   │
    │ MLB    ┆ 30   ┆ 46   │
    └────────┴──────┴──────┘



### Recipe 6 — Find a team by name 🔎

`find_team` fuzzy-matches across the ESPN leagues and hands back the team dict
(with its `id`, ready to feed into a roster call).


```python
for nm, lg in [("Patriots", "nfl"), ("Yankees", "mlb"), ("Bruins", "nhl"), ("Crimson Tide", "cfb")]:
    t = sdv.find_team(nm, lg)
    print(f"{lg:>3}  {nm:<14} -> {None if t is None else t['displayName']} (id={None if t is None else t['id']})")
```

    nfl  Patriots       -> New England Patriots (id=17)


    mlb  Yankees        -> New York Yankees (id=10)


    nhl  Bruins         -> Boston Bruins (id=1)
    cfb  Crimson Tide   -> Alabama Crimson Tide (id=333)


### Recipe 7 — Find an athlete by name 🏃

`find_athlete` does the same for players — great for grabbing an ESPN athlete
`id` without leaving Python.


```python
for nm, lg in [("Caitlin Clark", "wnba"), ("Patrick Mahomes", "nfl"), ("Connor McDavid", "nhl")]:
    a = sdv.find_athlete(nm, lg)
    print(f"{lg:>4}  {nm:<16} -> {None if a is None else a['displayName']} (id={None if a is None else a['id']})")
```

    wnba  Caitlin Clark    -> None (id=None)


     nfl  Patrick Mahomes  -> Patrick Mahomes (id=3139477)


     nhl  Connor McDavid   -> Connor McDavid (id=3895074)


### Recipe 8 — A team and its roster, end to end 👥

Chain `find_team` → `espn_<lg>_team_roster`: look up an ID by name, then pull the
roster. The roster wrapper is parsed to polars by default.


```python
lal = sdv.find_team("Lakers", "nba")
roster = None
if lal is not None:
    roster = safe(f"espn_nba_team_roster(team_id={lal['id']})",
                  lambda: sdv.espn_nba_team_roster(team_id=lal["id"], return_as_pandas=False))
(roster.head() if roster is not None and getattr(roster, "height", 0)
 else "roster unavailable right now")
```

    ✅ espn_nba_team_roster(team_id=13)





    shape: (5, 67)
    ┌─────────┬────────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
    │ id      ┆ uid        ┆ guid      ┆ first_nam ┆ … ┆ citizensh ┆ hand_type ┆ hand_abbr ┆ hand_disp │
    │ ---     ┆ ---        ┆ ---       ┆ e         ┆   ┆ ip        ┆ ---       ┆ eviation  ┆ lay_value │
    │ str     ┆ str        ┆ str       ┆ ---       ┆   ┆ ---       ┆ str       ┆ ---       ┆ ---       │
    │         ┆            ┆           ┆ str       ┆   ┆ str       ┆           ┆ str       ┆ str       │
    ╞═════════╪════════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ 5113969 ┆ s:40~l:46~ ┆ a24923a3- ┆ Cameron   ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
    │         ┆ a:5113969  ┆ f2e0-334d ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ -942f-3d3 ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ 689…      ┆           ┆   ┆           ┆           ┆           ┆           │
    │ 3945274 ┆ s:40~l:46~ ┆ 583794eb- ┆ Luka      ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
    │         ┆ a:3945274  ┆ 0f38-9bbd ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ -3e25-9dd ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ 33b…      ┆           ┆   ┆           ┆           ┆           ┆           │
    │ 4397014 ┆ s:40~l:46~ ┆ dbe4d07d- ┆ Quentin   ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
    │         ┆ a:4397014  ┆ 9166-07d7 ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ -19f0-52c ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ c77…      ┆           ┆   ┆           ┆           ┆           ┆           │
    │ 4868423 ┆ s:40~l:46~ ┆ d4c656b3- ┆ Jaden     ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
    │         ┆ a:4868423  ┆ e2b5-33c4 ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ -b4e7-7ac ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ a3e…      ┆           ┆   ┆           ┆           ┆           ┆           │
    │ 4683774 ┆ s:40~l:46~ ┆ 456f71fd- ┆ Bronny    ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
    │         ┆ a:4683774  ┆ 2ce5-3f50 ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ -8d0d-f30 ┆           ┆   ┆           ┆           ┆           ┆           │
    │         ┆            ┆ c01…      ┆           ┆   ┆           ┆           ┆           ┆           │
    └─────────┴────────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘



### Recipe 9 — polars → pandas in one keyword 🐼

Every wrapper honors `return_as_pandas=True`. Same data, different frame — handy
when the next step (sklearn, statsmodels, seaborn) wants pandas.


```python
teams_pl = safe("espn_wnba_teams (polars)", lambda: sdv.espn_wnba_teams())
teams_pd = safe("espn_wnba_teams (pandas)", lambda: sdv.espn_wnba_teams(return_as_pandas=True))
print("polars:", type(teams_pl).__name__, None if teams_pl is None else teams_pl.shape)
print("pandas:", type(teams_pd).__name__, None if teams_pd is None else teams_pd.shape)
```

    ✅ espn_wnba_teams (polars)
    ✅ espn_wnba_teams (pandas)
    polars: DataFrame (15, 14)
    pandas: DataFrame (15, 14)


### Recipe 10 — The `return_parsed` toggle on a native API 🎛️

Native API wrappers parse to polars by default; `return_parsed=False` hands back
the raw JSON `Dict` straight from the league feed.


```python
parsed = safe("nhl_standings (parsed)", lambda: sdv.nhl.nhl_standings())
raw = safe("nhl_standings (raw dict)", lambda: sdv.nhl.nhl_standings(return_parsed=False))
print("parsed ->", type(parsed).__name__, None if parsed is None else getattr(parsed, "shape", None))
print("raw    ->", type(raw).__name__, "(top-level keys:", None if not isinstance(raw, dict) else list(raw.keys())[:4], ")")
```

    ✅ nhl_standings (parsed)


    ✅ nhl_standings (raw dict)
    parsed -> DataFrame (32, 85)
    raw    -> dict (top-level keys: ['wildCardIndicator', 'standingsDateTimeUtc', 'standings'] )


### Recipe 11 — 🏈 Premium NFL pull (`api.nfl.com`)

`nfl_standings()` hits the league's own API and returns one tidy row per team.


```python
nfl_st = safe("nfl_standings (api.nfl.com)", lambda: sdv.nfl.nfl_standings(season=2024, week=18))
cols = ["team_abbr", "team_full_name", "overall_wins", "overall_losses",
        "division_name", "conference_name"]
(nfl_st.select([c for c in cols if c in nfl_st.columns]).head(8)
 if nfl_st is not None and getattr(nfl_st, "height", 0) else "NFL standings unavailable right now")
```

    ✅ nfl_standings (api.nfl.com)





    shape: (8, 3)
    ┌────────────────────┬──────────────┬────────────────┐
    │ team_full_name     ┆ overall_wins ┆ overall_losses │
    │ ---                ┆ ---          ┆ ---            │
    │ str                ┆ i64          ┆ i64            │
    ╞════════════════════╪══════════════╪════════════════╡
    │ Arizona Cardinals  ┆ 8            ┆ 9              │
    │ Atlanta Falcons    ┆ 8            ┆ 9              │
    │ Baltimore Ravens   ┆ 12           ┆ 5              │
    │ Buffalo Bills      ┆ 13           ┆ 4              │
    │ Carolina Panthers  ┆ 5            ┆ 12             │
    │ Chicago Bears      ┆ 5            ┆ 12             │
    │ Cincinnati Bengals ┆ 9            ┆ 8              │
    │ Cleveland Browns   ┆ 3            ┆ 14             │
    └────────────────────┴──────────────┴────────────────┘



### Recipe 12 — ⚾ Premium MLB pull (MLB Stats API + parser)

`mlb_*` wrappers return the raw `Dict`; pair them with the matching
`parse_mlb_api_*` for a tidy frame. Here's division standings, parsed.


```python
def mlb_standings():
    raw = sdv.mlb.mlb_standings(league_id="103,104", season=2024)
    return sdv.mlb.parse_mlb_api_standings(raw)

mlb_st = safe("MLB standings (Stats API + parser)", mlb_standings)
keep = ["standings_division_name", "team_name", "wins", "losses", "winning_percentage", "games_back"]
(mlb_st.select([c for c in keep if c in mlb_st.columns]).head(10)
 if mlb_st is not None and getattr(mlb_st, "height", 0) else "MLB standings unavailable right now")
```

    ✅ MLB standings (Stats API + parser)





    shape: (10, 6)
    ┌─────────────────────────┬───────────┬──────┬────────┬────────────────────┬────────────┐
    │ standings_division_name ┆ team_name ┆ wins ┆ losses ┆ winning_percentage ┆ games_back │
    │ ---                     ┆ ---       ┆ ---  ┆ ---    ┆ ---                ┆ ---        │
    │ str                     ┆ str       ┆ i64  ┆ i64    ┆ str                ┆ str        │
    ╞═════════════════════════╪═══════════╪══════╪════════╪════════════════════╪════════════╡
    │ null                    ┆ Yankees   ┆ 94   ┆ 68     ┆ .580               ┆ -          │
    │ null                    ┆ Orioles   ┆ 91   ┆ 71     ┆ .562               ┆ 3.0        │
    │ null                    ┆ Red Sox   ┆ 81   ┆ 81     ┆ .500               ┆ 13.0       │
    │ null                    ┆ Rays      ┆ 80   ┆ 82     ┆ .494               ┆ 14.0       │
    │ null                    ┆ Blue Jays ┆ 74   ┆ 88     ┆ .457               ┆ 20.0       │
    │ null                    ┆ Guardians ┆ 92   ┆ 69     ┆ .571               ┆ -          │
    │ null                    ┆ Royals    ┆ 86   ┆ 76     ┆ .531               ┆ 6.5        │
    │ null                    ┆ Tigers    ┆ 86   ┆ 76     ┆ .531               ┆ 6.5        │
    │ null                    ┆ Twins     ┆ 82   ┆ 80     ┆ .506               ┆ 10.5       │
    │ null                    ┆ White Sox ┆ 41   ┆ 121    ┆ .253               ┆ 51.5       │
    └─────────────────────────┴───────────┴──────┴────────┴────────────────────┴────────────┘



### Recipe 13 — ⚾ MLB Statcast — the premium tracking firehose

`mlb_statcast_search()` returns one row per pitch — the raw Baseball Savant tracking
data. Grab a single day and pull a few of the most useful columns.


```python
pitches = safe("mlb_statcast_search (1 day)",
               lambda: sdv.mlb.mlb_statcast_search(start_dt="2024-07-01", end_dt="2024-07-01"))
show = [c for c in ["game_date", "player_name", "pitch_type", "release_speed",
                    "launch_speed", "launch_angle", "events"]
        if pitches is not None and c in pitches.columns]
(pitches.select(show).head(10)
 if pitches is not None and getattr(pitches, "height", 0) else "no Statcast rows for that day right now")
```

    ✅ mlb_statcast_search (1 day)





    shape: (10, 7)
    ┌────────────┬───────────────┬────────────┬──────────────┬──────────────┬──────────────┬───────────┐
    │ game_date  ┆ player_name   ┆ pitch_type ┆ release_spee ┆ launch_speed ┆ launch_angle ┆ events    │
    │ ---        ┆ ---           ┆ ---        ┆ d            ┆ ---          ┆ ---          ┆ ---       │
    │ str        ┆ str           ┆ str        ┆ ---          ┆ f64          ┆ f64          ┆ str       │
    │            ┆               ┆            ┆ f64          ┆              ┆              ┆           │
    ╞════════════╪═══════════════╪════════════╪══════════════╪══════════════╪══════════════╪═══════════╡
    │ 2024-07-01 ┆ Alonso, Pete  ┆ FF         ┆ 94.8         ┆ 78.0         ┆ 46.0         ┆ field_out │
    │ 2024-07-01 ┆ Alonso, Pete  ┆ FF         ┆ 96.6         ┆ null         ┆ null         ┆ null      │
    │ 2024-07-01 ┆ Alonso, Pete  ┆ FF         ┆ 96.3         ┆ 73.3         ┆ 20.0         ┆ null      │
    │ 2024-07-01 ┆ Alonso, Pete  ┆ FF         ┆ 97.2         ┆ null         ┆ null         ┆ null      │
    │ 2024-07-01 ┆ Alonso, Pete  ┆ FF         ┆ 95.6         ┆ null         ┆ null         ┆ null      │
    │ 2024-07-01 ┆ Alonso, Pete  ┆ FF         ┆ 95.8         ┆ null         ┆ null         ┆ null      │
    │ 2024-07-01 ┆ Varsho,       ┆ FF         ┆ 97.4         ┆ null         ┆ null         ┆ strikeout │
    │            ┆ Daulton       ┆            ┆              ┆              ┆              ┆           │
    │ 2024-07-01 ┆ Martinez,     ┆ FF         ┆ 97.5         ┆ null         ┆ null         ┆ strikeout │
    │            ┆ J.D.          ┆            ┆              ┆              ┆              ┆           │
    │ 2024-07-01 ┆ Varsho,       ┆ KC         ┆ 84.0         ┆ 94.3         ┆ -12.0        ┆ null      │
    │            ┆ Daulton       ┆            ┆              ┆              ┆              ┆           │
    │ 2024-07-01 ┆ Varsho,       ┆ FF         ┆ 96.2         ┆ null         ┆ null         ┆ null      │
    │            ┆ Daulton       ┆            ┆              ┆              ┆              ┆           │
    └────────────┴───────────────┴────────────┴──────────────┴──────────────┴──────────────┴───────────┘



### Recipe 14 — 🏒 Premium NHL pull (`api-web`)

`nhl_standings()` reads the modern NHL `api-web` feed — one row per team, parsed
to polars.


```python
nhl_st = safe("nhl_standings (api-web)", lambda: sdv.nhl.nhl_standings())
keep = ["team_abbrev", "team_name", "wins", "losses", "ot_losses", "points",
        "conference_name", "division_name"]
(nhl_st.select([c for c in keep if c in nhl_st.columns]).head(8)
 if nhl_st is not None and getattr(nhl_st, "height", 0) else "NHL standings unavailable right now")
```

    ✅ nhl_standings (api-web)





    shape: (8, 6)
    ┌──────┬────────┬───────────┬────────┬─────────────────┬───────────────┐
    │ wins ┆ losses ┆ ot_losses ┆ points ┆ conference_name ┆ division_name │
    │ ---  ┆ ---    ┆ ---       ┆ ---    ┆ ---             ┆ ---           │
    │ i64  ┆ i64    ┆ i64       ┆ i64    ┆ str             ┆ str           │
    ╞══════╪════════╪═══════════╪════════╪═════════════════╪═══════════════╡
    │ 55   ┆ 16     ┆ 11        ┆ 121    ┆ Western         ┆ Central       │
    │ 53   ┆ 22     ┆ 7         ┆ 113    ┆ Eastern         ┆ Metropolitan  │
    │ 50   ┆ 20     ┆ 12        ┆ 112    ┆ Western         ┆ Central       │
    │ 50   ┆ 23     ┆ 9         ┆ 109    ┆ Eastern         ┆ Atlantic      │
    │ 50   ┆ 26     ┆ 6         ┆ 106    ┆ Eastern         ┆ Atlantic      │
    │ 48   ┆ 24     ┆ 10        ┆ 106    ┆ Eastern         ┆ Atlantic      │
    │ 46   ┆ 24     ┆ 12        ┆ 104    ┆ Western         ┆ Central       │
    │ 45   ┆ 27     ┆ 10        ┆ 100    ┆ Eastern         ┆ Atlantic      │
    └──────┴────────┴───────────┴────────┴─────────────────┴───────────────┘



### Recipe 15 — 🏒 NHL EDGE tracking leaderboard

NHL EDGE is the league's player- and puck-tracking system. The
`nhl_edge_skater_speed_top_10` board surfaces the fastest skating bursts.


```python
edge = safe("nhl_edge_skater_speed_top_10",
            lambda: sdv.nhl.nhl_edge_skater_speed_top_10(positions="forwards",
                                                         sort_by="maxskatingspeed"))
(edge.head(10) if edge is not None and getattr(edge, "height", 0)
 else "NHL EDGE leaderboard unavailable right now")
```

    ⏭️  nhl_edge_skater_speed_top_10: unavailable right now (NoESPNDataError)





    'NHL EDGE leaderboard unavailable right now'



### Recipe 16 — 🏒 Premium PWHL pull (HockeyTech)

The women's pro league rides the HockeyTech feed. `pwhl_standings()` returns the
table; `load_pwhl_schedules()` reads the parquet release for a whole season.


```python
pwhl_st = safe("pwhl_standings", lambda: sdv.pwhl.pwhl_standings(season=sdv.pwhl.most_recent_pwhl_season()))
pwhl_sched = safe("load_pwhl_schedules([2024])", lambda: sdv.pwhl.load_pwhl_schedules(seasons=[2024]))
print("standings rows:", None if pwhl_st is None else getattr(pwhl_st, "height", None),
      "| schedule rows:", None if pwhl_sched is None else getattr(pwhl_sched, "height", None))
(pwhl_st.head() if pwhl_st is not None and getattr(pwhl_st, "height", 0)
 else "PWHL standings unavailable right now")
```

    ⏭️  pwhl_standings: unavailable right now (ValueError)
    ✅ load_pwhl_schedules([2024])
    standings rows: None | schedule rows: 85





    'PWHL standings unavailable right now'



### Recipe 17 — 🏒 Junior hockey: schedule for all four CHL/AHL loops 🔁

Because AHL/OHL/WHL/QMJHL share one surface, a single loop tours every league's
schedule.


```python
rows = []
for lg, mod in {"ahl": ahl, "ohl": ohl, "whl": whl, "qmjhl": qmjhl}.items():
    season = safe(f"{lg} season", getattr(mod, f"most_recent_{lg}_season"))
    sch = (safe(f"{lg}_schedule", lambda mod=mod, lg=lg: getattr(mod, f"{lg}_schedule")())
           if season else None)
    rows.append({"league": lg.upper(), "season": season,
                 "games": None if sch is None else getattr(sch, "height", None)})
pl.DataFrame(rows)
```

    ✅ ahl season


    ✅ ahl_schedule


    ✅ ohl season


    ✅ ohl_schedule


    ✅ whl season


    ✅ whl_schedule


    ✅ qmjhl season


    ✅ qmjhl_schedule





    shape: (4, 3)
    ┌────────┬────────┬───────┐
    │ league ┆ season ┆ games │
    │ ---    ┆ ---    ┆ ---   │
    │ str    ┆ i64    ┆ i64   │
    ╞════════╪════════╪═══════╡
    │ AHL    ┆ 2027   ┆ 10000 │
    │ OHL    ┆ 2027   ┆ 10000 │
    │ WHL    ┆ 2026   ┆ 10000 │
    │ QMJHL  ┆ 2027   ┆ 10000 │
    └────────┴────────┴───────┘



### Recipe 18 — 🎲 A quick odds peek (key-guarded)

`odds.toa_sports()` lists every in-season sport/league key — it's **free**
(doesn't touch your quota). Set a free `ODDS_API_KEY` to light it up.


```python
if HAS_KEY:
    sports = safe("odds.toa_sports", lambda: odds.toa_sports(all_sports=False))
    out = (sports.select([c for c in ["key", "group", "title"] if c in sports.columns]).head(10)
           if sports is not None and getattr(sports, "height", 0) else "no in-season sports returned")
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports()  (free, doesn't touch quota)"
out
```

    ✅ odds.toa_sports





    shape: (10, 3)
    ┌─────────────────────────────────┬───────────────────┬───────────────────────────┐
    │ key                             ┆ group             ┆ title                     │
    │ ---                             ┆ ---               ┆ ---                       │
    │ str                             ┆ str               ┆ str                       │
    ╞═════════════════════════════════╪═══════════════════╪═══════════════════════════╡
    │ americanfootball_cfl            ┆ American Football ┆ CFL                       │
    │ americanfootball_ncaaf          ┆ American Football ┆ NCAAF                     │
    │ americanfootball_ncaaf_champio… ┆ American Football ┆ NCAAF Championship Winner │
    │ americanfootball_nfl            ┆ American Football ┆ NFL                       │
    │ americanfootball_nfl_preseason  ┆ American Football ┆ NFL Preseason             │
    │ americanfootball_nfl_super_bow… ┆ American Football ┆ NFL Super Bowl Winner     │
    │ aussierules_afl                 ┆ Aussie Rules      ┆ AFL                       │
    │ baseball_kbo                    ┆ Baseball          ┆ KBO                       │
    │ baseball_mlb                    ┆ Baseball          ┆ MLB                       │
    │ baseball_mlb_world_series_winn… ┆ Baseball          ┆ MLB World Series Winner   │
    └─────────────────────────────────┴───────────────────┴───────────────────────────┘



### Recipe 19 — 🎲 Live odds for a league (key-guarded)

`odds.toa_sports_odds()` returns **long-format** odds — one row per
event × book × market × outcome — exactly the shape you want for modelling.


```python
if HAS_KEY:
    board = safe("odds.toa_sports_odds (NFL h2h)",
                 lambda: odds.toa_sports_odds(sport="americanfootball_nfl", regions="us", markets="h2h"))
    keep = ["home_team", "away_team", "bookmaker_key", "market_key", "outcome_name", "outcome_price"]
    out = (board.select([c for c in keep if c in board.columns]).head(10)
           if board is not None and getattr(board, "height", 0) else "no NFL odds on the board right now")
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports_odds(sport='americanfootball_nfl')"
out
```

    ✅ odds.toa_sports_odds (NFL h2h)





    shape: (10, 6)
    ┌───────────┬─────────────┬───────────────┬────────────┬──────────────────────┬───────────────┐
    │ home_team ┆ away_team   ┆ bookmaker_key ┆ market_key ┆ outcome_name         ┆ outcome_price │
    │ ---       ┆ ---         ┆ ---           ┆ ---        ┆ ---                  ┆ ---           │
    │ str       ┆ str         ┆ str           ┆ str        ┆ str                  ┆ i64           │
    ╞═══════════╪═════════════╪═══════════════╪════════════╪══════════════════════╪═══════════════╡
    │ Seattle   ┆ New England ┆ draftkings    ┆ h2h        ┆ New England Patriots ┆ 160           │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ draftkings    ┆ h2h        ┆ Seattle Seahawks     ┆ -192          │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ betus         ┆ h2h        ┆ New England Patriots ┆ 163           │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ betus         ┆ h2h        ┆ Seattle Seahawks     ┆ -190          │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ fanduel       ┆ h2h        ┆ New England Patriots ┆ 184           │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ fanduel       ┆ h2h        ┆ Seattle Seahawks     ┆ -220          │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ fanatics      ┆ h2h        ┆ New England Patriots ┆ 175           │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ fanatics      ┆ h2h        ┆ Seattle Seahawks     ┆ -210          │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ lowvig        ┆ h2h        ┆ New England Patriots ┆ 165           │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    │ Seattle   ┆ New England ┆ lowvig        ┆ h2h        ┆ Seattle Seahawks     ┆ -190          │
    │ Seahawks  ┆ Patriots    ┆               ┆            ┆                      ┆               │
    └───────────┴─────────────┴───────────────┴────────────┴──────────────────────┴───────────────┘



### Recipe 20 — Count the whole surface, per league 🔢

`function_count()` returns the exposed-function tally for every league — a quick
sense of how much each sport gives you. (HockeyTech + odds modules are counted in
their own submodules.)


```python
counts = sdv.function_count()
df = (pl.DataFrame({"league": list(counts.keys()), "n_functions": list(counts.values())})
      .sort("n_functions", descending=True))
print("Total wrappers across the counted leagues:", sum(counts.values()))
df
```

    Total wrappers across the counted leagues: 5421





    shape: (34, 2)
    ┌────────┬─────────────┐
    │ league ┆ n_functions │
    │ ---    ┆ ---         │
    │ str    ┆ i64         │
    ╞════════╪═════════════╡
    │ mbb    ┆ 547         │
    │ wbb    ┆ 508         │
    │ nhl    ┆ 383         │
    │ cfb    ┆ 377         │
    │ mlb    ┆ 330         │
    │ …      ┆ …           │
    │ pwhl   ┆ 68          │
    │ ahl    ┆ 14          │
    │ ohl    ┆ 14          │
    │ qmjhl  ┆ 14          │
    │ whl    ┆ 14          │
    └────────┴─────────────┘



## 🎉 Where to next

You've now seen the **whole map** — every datasource, the naming contract that
makes the package guessable, and 20 recipes spanning ten-plus leagues. Each
sport has a dedicated tutorial that leads with its premium endpoints:

- [`02_cfb_intro`](../tutorials/02_cfb_intro.md) — 🏈 college football
- [`03_nfl_intro`](../tutorials/03_nfl_intro.md) — 🏈 NFL (`api.nfl.com` + nflverse)
- [`04_nba_intro`](../tutorials/04_nba_intro.md) — 🏀 NBA
- [`05_wbb_intro`](../tutorials/05_wbb_intro.md) — 🏀 NCAA women's basketball
- [`06_mbb_intro`](../tutorials/06_mbb_intro.md) — 🏀 NCAA men's basketball
- [`07_nhl_intro`](../tutorials/07_nhl_intro.md) — 🏒 NHL (`api-web` + EDGE + ESPN)
- [`08_wnba_intro`](../tutorials/08_wnba_intro.md) — 🏀 WNBA
- [`09_mlb_intro`](../tutorials/09_mlb_intro.md) — ⚾ MLB (Stats API + Statcast + ESPN)
- [`10_pwhl_intro`](../tutorials/10_pwhl_intro.md) — 🏒 PWHL
- [`11_junior_hockey_intro`](../tutorials/11_junior_hockey_intro.md) — 🏒 AHL / OHL / WHL / QMJHL
- [`12_odds_intro`](../tutorials/12_odds_intro.md) — 🎲 Betting odds (The Odds API)

**Reference indexes:**
[NBA](../nba/index.md) · [WNBA](../wnba/index.md) · [MBB](../mbb/index.md) ·
[WBB](../wbb/index.md) · [NFL](../nfl/index.md) · [CFB](../cfb/index.md) ·
[MLB](../mlb/index.md) · [NHL](../nhl/index.md) · [PWHL](../pwhl/index.md) ·
[AHL](../ahl/index.md) · [OHL](../ohl/index.md) · [WHL](../whl/index.md) ·
[QMJHL](../qmjhl/index.md) · [Odds](../odds/index.md).

Part of the **[SportsDataverse](https://www.sportsdataverse.org)** — the names
here mirror the R sisters (hoopR, wehoop, cfbfastR, baseballr, fastRhockey,
oddsapiR). Now go build something great! 🏆
