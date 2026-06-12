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
| ⚾ **MLB** | [tutorial](../tutorials/09_mlb_intro.md) · [ref](../mlb/index.md) | ✅ | 💳 `mlb_api_*` (MLB Stats API) | 💳 Statcast `statcast_*` | `load_mlb_pbp`, `load_mlb_team_boxscore` |
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
   `mlb_api_*` (MLB Stats API), `nhl_*` (`api-web`), `pwhl_*`/`ahl_*`/`ohl_*`/`whl_*`/`qmjhl_*`
   (HockeyTech), `toa_*` (The Odds API). 💳
3. **Tracking / analytics feeds** — the *really* premium stuff: `statcast_*`
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

### 🏒 The HockeyTech leagues share one surface

AHL / OHL / WHL / QMJHL / PWHL all expose `<lg>_schedule`, `<lg>_standings`,
`<lg>_teams`, `<lg>_team_roster`, and `most_recent_<lg>_season`. Learn one, you
learned all five.


```python
import sportsdataverse.ahl as ahl
import sportsdataverse.ohl as ohl
import sportsdataverse.whl as whl
import sportsdataverse.qmjhl as qmjhl
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


```python
# How big is each league's surface?
counts = sdv.function_count()
pl.DataFrame({"league": list(counts.keys()), "n_functions": list(counts.values())}) \
  .sort("n_functions", descending=True)
```


```python
# Fuzzy lookups — no IDs to memorize:
team = sdv.find_team("Lakers", "nba")
ath = sdv.find_athlete("LeBron", "nba")
print("team  ->", None if team is None else f"{team['displayName']} (id={team['id']})")
print("athlete ->", None if ath is None else f"{ath['displayName']} (id={ath['id']})")
```

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

### Recipe 2 — Any league's scoreboard 📋

`espn_<lg>_scoreboard()` returns today's slate as a tidy frame. Same call for
MLB, NBA, NHL — just change the slug.


```python
sb = safe("espn_mlb_scoreboard", lambda: sdv.espn_mlb_scoreboard())
(sb.head() if sb is not None and getattr(sb, "height", 0)
 else "no MLB games on the board right now")
```

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

### Recipe 4 — The same box-score shape for two different sports 🪞

`load_<sport>_team_boxscore` returns the same *kind* of frame for basketball and
hockey. Load one season of each and compare the shapes.


```python
nba_box = safe("load_nba_team_boxscore([2024])", lambda: sdv.load_nba_team_boxscore(seasons=[2024]))
nhl_box = safe("load_nhl_team_boxscore([2024])", lambda: sdv.load_nhl_team_boxscore(seasons=[2024]))
print("NBA team-box shape:", None if nba_box is None else nba_box.shape)
print("NHL team-box shape:", None if nhl_box is None else nhl_box.shape)
```

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

### Recipe 6 — Find a team by name 🔎

`find_team` fuzzy-matches across the ESPN leagues and hands back the team dict
(with its `id`, ready to feed into a roster call).


```python
for nm, lg in [("Patriots", "nfl"), ("Yankees", "mlb"), ("Bruins", "nhl"), ("Crimson Tide", "cfb")]:
    t = sdv.find_team(nm, lg)
    print(f"{lg:>3}  {nm:<14} -> {None if t is None else t['displayName']} (id={None if t is None else t['id']})")
```

### Recipe 7 — Find an athlete by name 🏃

`find_athlete` does the same for players — great for grabbing an ESPN athlete
`id` without leaving Python.


```python
for nm, lg in [("Caitlin Clark", "wnba"), ("Patrick Mahomes", "nfl"), ("Connor McDavid", "nhl")]:
    a = sdv.find_athlete(nm, lg)
    print(f"{lg:>4}  {nm:<16} -> {None if a is None else a['displayName']} (id={None if a is None else a['id']})")
```

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

### Recipe 9 — polars → pandas in one keyword 🐼

Every wrapper honors `return_as_pandas=True`. Same data, different frame — handy
when the next step (sklearn, statsmodels, seaborn) wants pandas.


```python
teams_pl = safe("espn_wnba_teams (polars)", lambda: sdv.espn_wnba_teams())
teams_pd = safe("espn_wnba_teams (pandas)", lambda: sdv.espn_wnba_teams(return_as_pandas=True))
print("polars:", type(teams_pl).__name__, None if teams_pl is None else teams_pl.shape)
print("pandas:", type(teams_pd).__name__, None if teams_pd is None else teams_pd.shape)
```

### Recipe 10 — The `return_parsed` toggle on a native API 🎛️

Native API wrappers parse to polars by default; `return_parsed=False` hands back
the raw JSON `Dict` straight from the league feed.


```python
parsed = safe("nhl_standings (parsed)", lambda: sdv.nhl.nhl_standings())
raw = safe("nhl_standings (raw dict)", lambda: sdv.nhl.nhl_standings(return_parsed=False))
print("parsed ->", type(parsed).__name__, None if parsed is None else getattr(parsed, "shape", None))
print("raw    ->", type(raw).__name__, "(top-level keys:", None if not isinstance(raw, dict) else list(raw.keys())[:4], ")")
```

### Recipe 11 — 🏈 Premium NFL pull (`api.nfl.com`)

`nfl_standings()` hits the league's own API and returns one tidy row per team.


```python
nfl_st = safe("nfl_standings (api.nfl.com)", lambda: sdv.nfl.nfl_standings(season=2024, week=18))
cols = ["team_abbr", "team_full_name", "overall_wins", "overall_losses",
        "division_name", "conference_name"]
(nfl_st.select([c for c in cols if c in nfl_st.columns]).head(8)
 if nfl_st is not None and getattr(nfl_st, "height", 0) else "NFL standings unavailable right now")
```

### Recipe 12 — ⚾ Premium MLB pull (MLB Stats API + parser)

`mlb_api_*` wrappers return the raw `Dict`; pair them with the matching
`parse_mlb_api_*` for a tidy frame. Here's division standings, parsed.


```python
def mlb_standings():
    raw = sdv.mlb.mlb_api_standings(league_id="103,104", season=2024)
    return sdv.mlb.parse_mlb_api_standings(raw)

mlb_st = safe("MLB standings (Stats API + parser)", mlb_standings)
keep = ["standings_division_name", "team_name", "wins", "losses", "winning_percentage", "games_back"]
(mlb_st.select([c for c in keep if c in mlb_st.columns]).head(10)
 if mlb_st is not None and getattr(mlb_st, "height", 0) else "MLB standings unavailable right now")
```

### Recipe 13 — ⚾ MLB Statcast — the premium tracking firehose

`statcast_search()` returns one row per pitch — the raw Baseball Savant tracking
data. Grab a single day and pull a few of the most useful columns.


```python
pitches = safe("statcast_search (1 day)",
               lambda: sdv.mlb.statcast_search(start_date="2024-07-01", end_date="2024-07-01"))
show = [c for c in ["game_date", "player_name", "pitch_type", "release_speed",
                    "launch_speed", "launch_angle", "events"]
        if pitches is not None and c in pitches.columns]
(pitches.select(show).head(10)
 if pitches is not None and getattr(pitches, "height", 0) else "no Statcast rows for that day right now")
```

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
