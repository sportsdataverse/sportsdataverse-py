---
title: Quickstart tutorial
sidebar_label: Quickstart
sidebar_position: 1
---

# 🏟️ Welcome to `sportsdataverse-py` — the cross-sport quickstart

One `pip install`, **every** major league. `sportsdataverse` is a single
Python package that speaks to the *premium native* data feeds across the
sporting world — the same official endpoints the leagues use to power their
own apps — and hands you back tidy **polars** DataFrames ready to model. 🚀

- 🏈 **NFL** via `api.nfl.com` (`nfl_*`) + the nflverse parquet releases (`load_*`)
- ⚾ **MLB** via the MLB Stats API (`mlb_api_*`) **and** Baseball Savant / Statcast (`statcast_*`)
- 🏒 **NHL** via the new `api-web` feed (`nhl_*`) **and** NHL EDGE tracking (`nhl_edge_*`)
- 🏒 **PWHL** women's pro hockey via the HockeyTech feed (`pwhl_*`, `load_pwhl_*`)
- 🎲 **Betting odds** across a whole market of sportsbooks (`sportsdataverse.odds`, `toa_*`)
- 🏀⚾🏈 **ESPN** cross-league families (`espn_<league>_*`) for the sports ESPN covers best

If you've used the R sisters — **hoopR, wehoop, cfbfastR, baseballr,
fastRhockey, oddsapiR** — the names here will feel like home. Let's take the
tour! 😊

## 🧰 The toolbox

Here's a hand-picked slice of the **premium** surface — one or two headline
functions per sport. Everything returns a tidy **polars** `DataFrame` by
default; pass `return_as_pandas=True` for pandas, or `return_parsed=False`
(native APIs) for the raw JSON `Dict`. Click any name for its full reference.

| Function | Source | What it gives you |
|---|---|---|
| [`nfl.nfl_standings`](../nfl/reference/nfl_api.md#nfl_standings) | 🏈 **api.nfl.com** | League standings, one row per team |
| [`nfl.nfl_ngs_leaders`](../nfl/reference/additional.md#nfl_ngs_leaders) | 🏈 **NFL Next Gen Stats** | Tracking-stat leaderboards (speed, etc.) |
| [`nfl.load_injuries`](../nfl/reference/loaders.md#load_nfl_injuries) | 🏈 nflverse release | Weekly injury reports (parquet) |
| [`nfl.load_nextgen_stats`](../nfl/reference/loaders.md#load_nfl_nextgen_stats) | 🏈 nflverse release | Season NGS passing/rushing/receiving |
| [`mlb.mlb_api_standings`](../mlb/reference/additional.md#mlb_api_standings) | ⚾ **MLB Stats API** | Division standings (parse with `parse_mlb_api_standings`) |
| [`mlb.mlb_api_team_roster`](../mlb/reference/additional.md#mlb_api_team_roster) | ⚾ **MLB Stats API** | A team's roster |
| [`mlb.statcast_leaderboard_expected_statistics`](../mlb/reference/additional.md#statcast_leaderboard_expected_statistics) | ⚾ **Statcast** | xBA / xSLG / xwOBA expected-stats board |
| [`mlb.statcast_search`](../mlb/reference/additional.md#statcast_search) | ⚾ **Statcast** | Pitch-level search (the raw tracking data) |
| [`nhl.nhl_standings`](../nhl/reference/nhl_api_web.md#nhl_standings) | 🏒 **NHL api-web** | League standings |
| [`nhl.nhl_skater_leaders`](../nhl/reference/nhl_api_web.md#nhl_skater_leaders) | 🏒 **NHL api-web** | Skater statistical leaders |
| [`nhl.nhl_edge_skater_speed_top_10`](../nhl/reference/nhl_edge.md#nhl_edge_skater_speed_top_10) | 🏒 **NHL EDGE** | Player-tracking top-speed leaderboard |
| [`pwhl.load_pwhl_schedules`](../pwhl/reference/loaders.md#load_pwhl_schedules) | 🏒 PWHL release | Women's pro schedule + results |
| [`pwhl.pwhl_standings`](../pwhl/reference/additional.md#pwhl_standings) | 🏒 **PWHL HockeyTech** | PWHL standings |
| [`odds.toa_sports`](../odds/reference/additional.md#toa_sports) | 🎲 **The Odds API** | Every in-season sport/league key |
| [`odds.toa_sports_odds`](../odds/reference/additional.md#toa_sports_odds) | 🎲 **The Odds API** | Live moneylines / spreads / totals |

Many more live in each sport's reference pages and its dedicated tutorial
notebook (linked at the very bottom). 👇

## 🔌 Setup

```sh
pip install sportsdataverse
# or
uv add sportsdataverse
```

Every sport is a submodule of the umbrella package. No API key is needed for
the league feeds (NFL / MLB / NHL / PWHL / ESPN); only the betting-odds module
wants a (free) `ODDS_API_KEY`. Let's import the umbrella and the odds module.


```python
import polars as pl
import sportsdataverse as sdv
import sportsdataverse.odds as odds

# Every league hangs off the top-level package:
[m for m in dir(sdv) if m in
 ("cfb", "nfl", "nba", "wnba", "mbb", "wbb", "nhl", "mlb", "pwhl",
  "ahl", "ohl", "whl", "qmjhl", "odds")]
```

Live league endpoints are seasonal and occasionally rate-limited, so a tiny
`safe()` helper runs each network call defensively. You get the frame when the
feed is up, and a friendly one-liner when it isn't — never a scary traceback.
That keeps this whole page runnable offline or in the off-season. 🛟


```python
def safe(label, thunk):
    """Run a live call; return its result, or print a one-liner and return None."""
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None
```

## 🧭 The naming contract

Once you learn the pattern, you can *guess* function names across every sport:
`<league>_*` is the native premium API (`nhl_standings()`, `mlb_api_schedule()`);
the tracking feeds are `<league>_edge_*` / `statcast_*` / `<league>_ngs_*`;
`espn_<league>_*` is the ESPN mirror; and `load_<league>_*()` reads a pre-built
parquet release (fast and reliable). The return-type knobs never change:
**polars by default**, `return_as_pandas=True` for pandas, and
`return_parsed=False` (native APIs) for the raw JSON `Dict`.

## 🏈 NFL — straight from `api.nfl.com`

`nfl_standings()` hits the league's own API and returns one tidy row per team.
It's parsed to polars by default; the same `return_parsed=False` knob hands
you the raw payload.


```python
standings = safe("NFL standings (api.nfl.com)",
                 lambda: sdv.nfl.nfl_standings(season=2024, week=18))
cols = ["team_abbr", "team_full_name", "overall_wins", "overall_losses",
        "overall_ties", "division_name", "conference_name"]
(standings.select([c for c in cols if c in standings.columns]).head(8)
 if standings is not None else "NFL standings unavailable right now")
```

## ⚾ MLB — the Stats API + Statcast

The MLB Stats API wrappers (`mlb_api_*`) return the raw `Dict`; pair them with
the matching `parse_mlb_api_*` to get a tidy frame. Here's the division
standings, parsed.


```python
def mlb_standings():
    raw = sdv.mlb.mlb_api_standings(league_id="103,104", season=2024)
    return sdv.mlb.parse_mlb_api_standings(raw)

mlb_st = safe("MLB standings (MLB Stats API)", mlb_standings)
keep = ["standings_division_name", "team_name", "wins", "losses",
        "winning_percentage", "games_back"]
(mlb_st.select([c for c in keep if c in mlb_st.columns]).head(10)
 if mlb_st is not None else "MLB standings unavailable right now")
```

And the *premium* part of baseball — **Statcast**. The expected-statistics
leaderboard gives you xBA / xSLG / xwOBA (the tracking-derived "deserved"
numbers) for a season, straight from Baseball Savant.


```python
xstats = safe("Statcast expected-stats leaderboard",
              lambda: sdv.mlb.statcast_leaderboard_expected_statistics(year=2024, type_="batter"))
show = [c for c in ["last_name, first_name", "player_id", "pa", "ba", "est_ba",
                    "slg", "est_slg", "woba", "est_woba"]
        if xstats is not None and c in xstats.columns]
(xstats.select(show).head(8) if xstats is not None else "Statcast unavailable right now")
```

## 🏒 NHL — `api-web` + EDGE tracking

`nhl_standings()` reads the modern NHL `api-web` feed. The headline *premium*
extra is **NHL EDGE** — the league's player- and puck-tracking system — exposed
through the `nhl_edge_*` family.


```python
nhl_st = safe("NHL standings (api-web)", lambda: sdv.nhl.nhl_standings())
keep = ["team_abbrev", "team_name", "wins", "losses", "ot_losses", "points",
        "conference_name", "division_name"]
(nhl_st.select([c for c in keep if c in nhl_st.columns]).head(8)
 if nhl_st is not None else "NHL standings unavailable right now")
```

## 🏒 PWHL — women's pro hockey

The Professional Women's Hockey League rides the same HockeyTech feed as the
junior/minor loops. The `load_pwhl_*` **release loaders** read pre-built
parquet and are the most reliable way to grab a whole season at once.


```python
pwhl_sched = safe("PWHL schedule (release parquet)",
                  lambda: sdv.pwhl.load_pwhl_schedules(seasons=[2024]))
cols = ["game_id", "game_date", "home_team", "away_team",
        "home_goal_count", "visiting_goal_count"]
(pwhl_sched.select([c for c in cols if c in pwhl_sched.columns]).head()
 if pwhl_sched is not None else "PWHL schedule unavailable right now")
```

## 🎲 Betting odds — a whole market in one call

`sportsdataverse.odds` wraps [The Odds API](https://the-odds-api.com) and
returns **long-format** odds (one row per event × book × market × outcome).
Set a free `ODDS_API_KEY` env var to light up the live cells; without one,
this section just prints a friendly note.


```python
import os
HAS_ODDS_KEY = bool(os.environ.get("ODDS_API_KEY"))
print("ODDS_API_KEY set:", HAS_ODDS_KEY,
      "— odds cells will" + ("" if HAS_ODDS_KEY else " NOT") + " run live")

if HAS_ODDS_KEY:
    sports = safe("Odds: in-season sports", lambda: odds.toa_sports(all_sports=False))
    out = (sports.select([c for c in ["key", "group", "title"] if c in sports.columns]).head(10)
           if sports is not None else "no sports returned")
else:
    out = "set ODDS_API_KEY to run: odds.toa_sports()  (free, doesn't touch quota)"
out
```

## 🍳 Cookbook — one premium task per sport

These four recipes are the kind of thing you'll reach for constantly. Each one
calls a **premium** feed and is fully runnable (and defensively guarded, so a
flaky network or off-season just prints a note).

### Recipe 1 — 🏈 NFL weekly injury report

`load_injuries()` pulls the nflverse injury-report parquet. Filter to one week
and you've got the practice-status board for the whole league.


```python
inj = safe("NFL injuries (nflverse release)",
           lambda: sdv.nfl.load_injuries(seasons=[2024]))
if inj is not None and inj.height:
    wk = (inj.filter((pl.col("week") == 1) & (pl.col("report_status").is_not_null()))
          if "week" in inj.columns and "report_status" in inj.columns else inj)
    cols = ["season", "week", "team", "position", "full_name", "report_status"]
    out = wk.select([c for c in cols if c in wk.columns]).head(10)
else:
    out = "injuries unavailable right now"
out
```

### Recipe 2 — ⚾ MLB Statcast pitch-level slice

`statcast_search()` is the raw tracking firehose — one row per pitch. Grab a
single day and pull a few of the most useful tracking columns.


```python
pitches = safe("Statcast pitch search (1 day)",
               lambda: sdv.mlb.statcast_search(start_date="2024-07-01", end_date="2024-07-01"))
show = [c for c in ["game_date", "player_name", "pitch_type", "release_speed",
                    "launch_speed", "launch_angle", "events", "description"]
        if pitches is not None and c in pitches.columns]
(pitches.select(show).head(10)
 if pitches is not None and pitches.height else "no Statcast rows for that day right now")
```

### Recipe 3 — 🏒 NHL EDGE top-speed leaderboard

NHL EDGE tracking, distilled to a leaderboard: the fastest skating bursts of
the season. `positions=` filters by position group and `sort_by=` picks the
metric.


```python
edge = safe("NHL EDGE skater top-speed top-10",
            lambda: sdv.nhl.nhl_edge_skater_speed_top_10(positions="forwards",
                                                         sort_by="maxskatingspeed"))
(edge.head(10) if edge is not None and getattr(edge, "height", 0)
 else "NHL EDGE leaderboard unavailable right now")
```

### Recipe 4 — 🏒 PWHL standings + a team roster

Stack two PWHL native calls: the live `pwhl_standings()` for the table, then
`pwhl_teams()` → `pwhl_team_roster()` to pull a roster for the first team.


```python
pwhl_st = safe("PWHL standings (HockeyTech)",
               lambda: sdv.pwhl.pwhl_standings(season=sdv.pwhl.most_recent_pwhl_season()))
teams = safe("PWHL teams", lambda: sdv.pwhl.pwhl_teams(season=sdv.pwhl.most_recent_pwhl_season()))

roster = None
if teams is not None and getattr(teams, "height", 0):
    tid_col = next((c for c in ("team_id", "id") if c in teams.columns), None)
    if tid_col is not None:
        tid = int(teams[tid_col][0])
        roster = safe(f"PWHL roster (team {tid})", lambda: sdv.pwhl.pwhl_team_roster(team_id=tid))

print("standings rows:", None if pwhl_st is None else getattr(pwhl_st, "height", None),
      "| roster rows:", None if roster is None else getattr(roster, "height", None))
(pwhl_st.head() if pwhl_st is not None and getattr(pwhl_st, "height", 0)
 else "PWHL standings unavailable right now")
```

## 🐼 polars ↔ pandas in one keyword

Every wrapper honors `return_as_pandas=True`. Same data, different frame —
handy when the next step in your pipeline (sklearn, statsmodels, seaborn)
expects pandas. Here's the round-trip on an ESPN call that always works.


```python
teams_pl = safe("ESPN WNBA teams (polars)", lambda: sdv.wnba.espn_wnba_teams())
teams_pd = safe("ESPN WNBA teams (pandas)",
                lambda: sdv.wnba.espn_wnba_teams(return_as_pandas=True))
print("polars:", type(teams_pl).__name__, None if teams_pl is None else teams_pl.shape)
print("pandas:", type(teams_pd).__name__, None if teams_pd is None else teams_pd.shape)
```

## 🎉 Where to next

You've now touched the premium native feed for every major sport in the
package. Dive deeper in the per-sport tutorials — each leads with that sport's
premium endpoints:

- `02_cfb_intro.ipynb` — 🏈 college football
- `03_nfl_intro.ipynb` — 🏈 NFL (`api.nfl.com` + nflverse parity)
- `04_nba_intro.ipynb` — 🏀 NBA
- `05_wbb_intro.ipynb` — 🏀 NCAA women's basketball
- `06_mbb_intro.ipynb` — 🏀 NCAA men's basketball
- `07_nhl_intro.ipynb` — 🏒 NHL (`api-web` + EDGE + ESPN)
- `08_wnba_intro.ipynb` — 🏀 WNBA
- `09_mlb_intro.ipynb` — ⚾ MLB (Stats API + Statcast + ESPN)
- `10_pwhl_intro.ipynb` — 🏒 PWHL
- `11_junior_hockey_intro.ipynb` — 🏒 AHL / OHL / WHL / QMJHL
- `12_odds_intro.ipynb` — 🎲 Betting odds (The Odds API)

**Reference pages** for everything you saw:
[NFL](../nfl/reference/nfl_api.md) ·
[MLB](../mlb/reference/additional.md) ·
[NHL](../nhl/reference/nhl_api_web.md) ·
[NHL EDGE](../nhl/reference/nhl_edge.md) ·
[PWHL](../pwhl/reference/additional.md) ·
[Odds](../odds/reference/additional.md).

Part of the **[SportsDataverse](https://www.sportsdataverse.org)** — the names
here mirror the R sisters. Now go build something great! 🏆
