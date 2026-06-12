---
title: NFL tutorial
sidebar_label: NFL
sidebar_position: 6
---

# 🏈 The NFL with `sportsdataverse-py`

Welcome to gridiron data! 🎉 In a handful of lines you're about to pull
**standings, rosters, weekly injury reports, NextGen Stats tracking
leaderboards, and full play-by-play** — straight from the source.

`sportsdataverse.nfl` leads with the **premium [`api.nfl.com`](https://api.nfl.com)
native endpoints** (`nfl_standings`, `nfl_rosters`, `nfl_injuries`, …) and the
**[NextGen Stats](https://nextgenstats.nfl.com) tracking API** (`nfl_ngs_*`),
backed by the battle-tested **[nflverse](https://nflverse.nflverse.com) release
loaders** (`load_nfl_pbp`, `load_nfl_player_stats`, …). ESPN
(`espn_nfl_*`) rides shotgun as a quick, no-auth secondary path.

Every accessor hands you a tidy **polars** `DataFrame` by default — pass
`return_as_pandas=True` for pandas. If you've used the R packages
[nflfastR](https://www.nflfastr.com) / [nflreadr](https://nflreadr.nflverse.com),
or the Python [nflreadpy](https://github.com/nflverse/nflreadpy), you're already
home: the `load_*` names line up. Let's hike it! 🏈

## 🧰 The toolbox

Three data families, one module. The 🟢 **premium** rows lead with native
`api.nfl.com` / NextGen Stats endpoints; the 📦 rows read versioned nflverse
release parquets; ESPN is the 🔵 quick secondary path. Click any name for the
full reference.

| Function | What it gives you | Source |
|---|---|---|
| [`nfl_standings`](../nfl/reference/nfl_api.md#nfl_standings) | Team standings for a season/week — one row per team | 🟢 premium (NFL.com) |
| [`nfl_rosters`](../nfl/reference/nfl_api.md#nfl_rosters) | Season rosters, one row per team (players nested) | 🟢 premium (NFL.com) |
| [`nfl_injuries`](../nfl/reference/nfl_api.md#nfl_injuries) | Weekly injury report, one row per player | 🟢 premium (NFL.com) |
| [`nfl_weeks`](../nfl/reference/nfl_api.md#nfl_weeks) | The week calendar (bye weeks, date ranges) | 🟢 premium (NFL.com) |
| [`nfl_weekly_game_details`](../nfl/reference/nfl_api.md#nfl_weekly_game_details) | Rich per-game details for a week (drive charts, standings) | 🟢 premium (NFL.com) |
| [`nfl_game_summaries`](../nfl/reference/nfl_api.md#nfl_game_summaries) | Live game state, one row per game | 🟢 premium (NFL.com) |
| [`nfl_team`](../nfl/reference/nfl_api.md#nfl_team) | Single-team detail by `team_id` | 🟢 premium (NFL.com) |
| [`nfl_ngs_statboard`](../nfl/reference/additional.md#nfl_ngs_statboard) | NextGen Stats season leaderboard (passing/rushing/receiving) | 🟢 premium (NextGen Stats) |
| [`nfl_ngs_leaders`](../nfl/reference/additional.md#nfl_ngs_leaders) | NextGen top-N highlight boards (speed, YAC over expected, …) | 🟢 premium (NextGen Stats) |
| [`nfl_ngs_league_schedule`](../nfl/reference/additional.md#nfl_ngs_league_schedule) | NextGen schedule — source of NGS `gameId`s | 🟢 premium (NextGen Stats) |
| [`nfl_ngs_gamecenter_overview`](../nfl/reference/additional.md#nfl_ngs_gamecenter_overview) | Per-game NextGen player splits (passers/rushers/…) | 🟢 premium (NextGen Stats) |
| [`load_nfl_pbp`](../nfl/reference/loaders.md#load_nfl_pbp) | Full nflfastR play-by-play (370+ columns) | 📦 nflverse release |
| [`load_nfl_player_stats`](../nfl/reference/additional.md#load_nfl_player_stats) | Weekly player box-score stats | 📦 nflverse release |
| [`load_nfl_nextgen_stats`](../nfl/reference/additional.md#load_nfl_nextgen_stats) | NextGen Stats back to 2016 (release parquet) | 📦 nflverse release |
| [`load_nfl_rosters`](../nfl/reference/loaders.md#load_nfl_rosters) | Season rosters with IDs & bios | 📦 nflverse release |
| [`espn_nfl_schedule`](../nfl/reference/additional.md#espn_nfl_schedule) · [`espn_nfl_scoreboard`](../nfl/reference/site.md#espn_nfl_scoreboard) | ESPN scoreboard/schedule (no auth) | 🔵 ESPN (secondary) |
| [`load_nfl_snap_counts`](../nfl/reference/loaders.md#load_nfl_snap_counts) | Weekly snap counts & snap-share % per player | 📦 nflverse release |
| [`load_nfl_depth_charts`](../nfl/reference/loaders.md#load_nfl_depth_charts) | Weekly depth charts, one row per slotted player | 📦 nflverse release |
| [`load_nfl_schedule`](../nfl/reference/additional.md#load_nfl_schedule) | Game results + lines, one row per game | 📦 nflverse release |
| [`load_nfl_draft_picks`](../nfl/reference/additional.md#load_nfl_draft_picks) | Every draft pick + career value, one row per pick | 📦 nflverse release |
| `get_current_nfl_season` · `most_recent_nfl_season` | Season helpers | 🟢 helper |


## 🔌 Setup

```sh
pip install sportsdataverse
```

**No API key needed** — the native `api.nfl.com` wrappers mint a fresh
anonymous token for you, and the NextGen Stats client warms its own browser
cookies. The nflverse loaders just read public release parquets. 😊


```python
import polars as pl
import sportsdataverse.nfl as nfl

pl.Config.set_tbl_cols(8)  # keep wide frames readable in the notebook

```




    polars.config.Config



Native NFL.com / NextGen endpoints and ESPN are *live* services — great
in-season, occasionally grumpy in the offseason or behind a flaky network. A
tiny `safe()` helper runs each live call defensively: you get the frame when
the feed is up, and a friendly one-liner when it isn't (never a scary
traceback). 🛟 The `load_*` release parquets are reliable, so we call those
directly.


```python
def safe(label, thunk):
    """Run a live call; return its result, or print a one-liner and return None."""
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience over network blips
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None


# 2024 is a complete season with full data everywhere — a safe default to demo.
SEASON = 2024

```

## 🟢 Premium first: NFL.com native standings

The headliner. [`nfl_standings`](../nfl/reference/nfl_api.md#nfl_standings)
returns **one row per team** with conference/division records, streaks,
clinch flags, point differentials — the works. Pass `season`, `season_type`
(`"REG"`/`"POST"`/`"PRE"` — *strings*, not ESPN's numeric codes) and `week`.


```python
standings = safe(
    "NFL.com standings",
    lambda: nfl.nfl_standings(season=SEASON, season_type="REG", week=18),
)
standings.shape if standings is not None else "standings unavailable"

```

    ✅ NFL.com standings





    (32, 53)




```python
cols = [
    "team_full_name", "conference_rank", "division_rank",
    "overall_wins", "overall_losses", "overall_ties",
    "division_wins", "division_losses",
]
(standings.select([c for c in cols if c in standings.columns])
          .sort("conference_rank")
          .head(10)
 if standings is not None else "standings unavailable")

```




    shape: (10, 8)
    ┌────────────┬────────────┬────────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
    │ team_full_ ┆ conference ┆ division_r ┆ overall_w ┆ overall_l ┆ overall_t ┆ division_ ┆ division_ │
    │ name       ┆ _rank      ┆ ank        ┆ ins       ┆ osses     ┆ ies       ┆ wins      ┆ losses    │
    │ ---        ┆ ---        ┆ ---        ┆ ---       ┆ ---       ┆ ---       ┆ ---       ┆ ---       │
    │ str        ┆ i64        ┆ i64        ┆ i64       ┆ i64       ┆ i64       ┆ i64       ┆ i64       │
    ╞════════════╪════════════╪════════════╪═══════════╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ Detroit    ┆ 1          ┆ 1          ┆ 15        ┆ 2         ┆ 0         ┆ 6         ┆ 0         │
    │ Lions      ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Kansas     ┆ 1          ┆ 1          ┆ 15        ┆ 2         ┆ 0         ┆ 5         ┆ 1         │
    │ City       ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Chiefs     ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Buffalo    ┆ 2          ┆ 1          ┆ 13        ┆ 4         ┆ 0         ┆ 5         ┆ 1         │
    │ Bills      ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Philadelph ┆ 2          ┆ 1          ┆ 14        ┆ 3         ┆ 0         ┆ 5         ┆ 1         │
    │ ia Eagles  ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Baltimore  ┆ 3          ┆ 1          ┆ 12        ┆ 5         ┆ 0         ┆ 4         ┆ 2         │
    │ Ravens     ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Tampa Bay  ┆ 3          ┆ 1          ┆ 10        ┆ 7         ┆ 0         ┆ 4         ┆ 2         │
    │ Buccaneers ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Houston    ┆ 4          ┆ 1          ┆ 10        ┆ 7         ┆ 0         ┆ 5         ┆ 1         │
    │ Texans     ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Los        ┆ 4          ┆ 1          ┆ 10        ┆ 7         ┆ 0         ┆ 4         ┆ 2         │
    │ Angeles    ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Rams       ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Los        ┆ 5          ┆ 2          ┆ 11        ┆ 6         ┆ 0         ┆ 4         ┆ 2         │
    │ Angeles    ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Chargers   ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    │ Minnesota  ┆ 5          ┆ 2          ┆ 14        ┆ 3         ┆ 0         ┆ 4         ┆ 2         │
    │ Vikings    ┆            ┆            ┆           ┆           ┆           ┆           ┆           │
    └────────────┴────────────┴────────────┴───────────┴───────────┴───────────┴───────────┴───────────┘



## 👥 Rosters & the week calendar

[`nfl_rosters`](../nfl/reference/nfl_api.md#nfl_rosters) gives one row per
team for a season, with the player list nested under `persons` (great for a
team directory). [`nfl_weeks`](../nfl/reference/nfl_api.md#nfl_weeks) is the
season's week calendar — handy for finding bye weeks and date ranges before
you loop over a slate.

| Function | One row per | Key columns |
|---|---|---|
| [`nfl_rosters`](../nfl/reference/nfl_api.md#nfl_rosters) | team | `team_abbreviation`, `team_conference_abbr`, `persons` |
| [`nfl_weeks`](../nfl/reference/nfl_api.md#nfl_weeks) | week | `week`, `week_type`, `bye_teams`, `date_begin` |


```python
rosters = safe("NFL.com rosters", lambda: nfl.nfl_rosters(season=SEASON))
cols = ["team_abbreviation", "team_full_name", "team_conference_abbr", "team_division_full_name"]
(rosters.select([c for c in cols if c in rosters.columns]).head(8)
 if rosters is not None else "rosters unavailable")

```

    ✅ NFL.com rosters





    shape: (8, 4)
    ┌───────────────────┬────────────────────┬──────────────────────┬─────────────────────────┐
    │ team_abbreviation ┆ team_full_name     ┆ team_conference_abbr ┆ team_division_full_name │
    │ ---               ┆ ---                ┆ ---                  ┆ ---                     │
    │ str               ┆ str                ┆ str                  ┆ str                     │
    ╞═══════════════════╪════════════════════╪══════════════════════╪═════════════════════════╡
    │ ARI               ┆ Arizona Cardinals  ┆ NFC                  ┆ NFC West                │
    │ ATL               ┆ Atlanta Falcons    ┆ NFC                  ┆ NFC South               │
    │ BAL               ┆ Baltimore Ravens   ┆ AFC                  ┆ AFC North               │
    │ BUF               ┆ Buffalo Bills      ┆ AFC                  ┆ AFC East                │
    │ CAR               ┆ Carolina Panthers  ┆ NFC                  ┆ NFC South               │
    │ CHI               ┆ Chicago Bears      ┆ NFC                  ┆ NFC North               │
    │ CIN               ┆ Cincinnati Bengals ┆ AFC                  ┆ AFC North               │
    │ CLE               ┆ Cleveland Browns   ┆ AFC                  ┆ AFC North               │
    └───────────────────┴────────────────────┴──────────────────────┴─────────────────────────┘




```python
weeks = safe("NFL.com weeks", lambda: nfl.nfl_weeks(season=SEASON, season_type="REG"))
cols = ["season", "week", "week_type", "date_begin", "date_end", "bye_teams"]
(weeks.select([c for c in cols if c in weeks.columns]).head(8)
 if weeks is not None else "weeks unavailable")

```

    ✅ NFL.com weeks





    shape: (8, 6)
    ┌────────┬──────┬───────────┬────────────┬────────────┬─────────────────────────────────┐
    │ season ┆ week ┆ week_type ┆ date_begin ┆ date_end   ┆ bye_teams                       │
    │ ---    ┆ ---  ┆ ---       ┆ ---        ┆ ---        ┆ ---                             │
    │ i64    ┆ i64  ┆ str       ┆ str        ┆ str        ┆ str                             │
    ╞════════╪══════╪═══════════╪════════════╪════════════╪═════════════════════════════════╡
    │ 2024   ┆ 18   ┆ REG       ┆ 2025-01-01 ┆ 2025-01-08 ┆ []                              │
    │ 2024   ┆ 17   ┆ REG       ┆ 2024-12-24 ┆ 2025-01-01 ┆ []                              │
    │ 2024   ┆ 16   ┆ REG       ┆ 2024-12-18 ┆ 2024-12-24 ┆ []                              │
    │ 2024   ┆ 15   ┆ REG       ┆ 2024-12-11 ┆ 2024-12-18 ┆ []                              │
    │ 2024   ┆ 14   ┆ REG       ┆ 2024-12-04 ┆ 2024-12-11 ┆ [{'id': '10400325-48de-3d6a-be… │
    │ 2024   ┆ 13   ┆ REG       ┆ 2024-11-27 ┆ 2024-12-04 ┆ []                              │
    │ 2024   ┆ 12   ┆ REG       ┆ 2024-11-20 ┆ 2024-11-27 ┆ [{'id': '10400200-f401-4e53-51… │
    │ 2024   ┆ 11   ┆ REG       ┆ 2024-11-13 ┆ 2024-11-20 ┆ [{'id': '10403800-517c-7b8c-65… │
    └────────┴──────┴───────────┴────────────┴────────────┴─────────────────────────────────┘



## 🏥 The weekly injury report

[`nfl_injuries`](../nfl/reference/nfl_api.md#nfl_injuries) is the official
weekly injury report — **one row per listed player** with their
`injury_status` (Out / Doubtful / Questionable), practice participation, and
team. This is the premium native feed, not a scrape.


```python
inj = safe(
    "NFL.com injuries",
    lambda: nfl.nfl_injuries(season=SEASON, season_type="REG", week=1),
)
cols = [
    "team_full_name", "person_display_name", "position",
    "injuries", "injury_status", "practice_status",
]
(inj.select([c for c in cols if c in inj.columns]).head(10)
 if inj is not None else "injuries unavailable")

```

    ✅ NFL.com injuries





    shape: (10, 6)
    ┌─────────────────┬─────────────────┬──────────┬─────────────────┬───────────────┬─────────────────┐
    │ team_full_name  ┆ person_display_ ┆ position ┆ injuries        ┆ injury_status ┆ practice_status │
    │ ---             ┆ name            ┆ ---      ┆ ---             ┆ ---           ┆ ---             │
    │ str             ┆ ---             ┆ str      ┆ str             ┆ str           ┆ str             │
    │                 ┆ str             ┆          ┆                 ┆               ┆                 │
    ╞═════════════════╪═════════════════╪══════════╪═════════════════╪═══════════════╪═════════════════╡
    │ Atlanta Falcons ┆ Ta'Quon Graham  ┆ DE       ┆ []              ┆ null          ┆ LIMITED         │
    │ Atlanta Falcons ┆ Antonio         ┆ CB       ┆ ['Groin']       ┆ OUT           ┆ DIDNOT          │
    │                 ┆ Hamilton        ┆          ┆                 ┆               ┆                 │
    │ Atlanta Falcons ┆ Grady Jarrett   ┆ DT       ┆ []              ┆ null          ┆ DIDNOT          │
    │ Atlanta Falcons ┆ Nate Landman    ┆ LB       ┆ []              ┆ null          ┆ FULL            │
    │ Atlanta Falcons ┆ Chris Lindstrom ┆ G        ┆ ['Evaluated for ┆ null          ┆ null            │
    │                 ┆                 ┆          ┆ a possible hea… ┆               ┆                 │
    │ Atlanta Falcons ┆ Jake Matthews   ┆ T        ┆ []              ┆ null          ┆ LIMITED         │
    │ Atlanta Falcons ┆ David Onyemata  ┆ DT       ┆ []              ┆ null          ┆ DIDNOT          │
    │ Atlanta Falcons ┆ Kyle Pitts      ┆ TE       ┆ []              ┆ null          ┆ FULL            │
    │ Baltimore       ┆ Rasheen Ali     ┆ RB       ┆ ['Neck']        ┆ DOUBTFUL      ┆ LIMITED         │
    │ Ravens          ┆                 ┆          ┆                 ┆               ┆                 │
    │ Baltimore       ┆ Adisa Isaac     ┆ LB       ┆ ['Hamstring']   ┆ OUT           ┆ DIDNOT          │
    │ Ravens          ┆                 ┆          ┆                 ┆               ┆                 │
    └─────────────────┴─────────────────┴──────────┴─────────────────┴───────────────┴─────────────────┘



## 📋 Per-game details for a week

Need the full slate with drive charts, broadcast info and embedded standings?
[`nfl_weekly_game_details`](../nfl/reference/nfl_api.md#nfl_weekly_game_details)
returns **one row per game** for a week (toggle the heavy blocks with the
`include_*` flags). For live in-game state (clock, down & distance, red-zone
flags), reach for
[`nfl_game_summaries`](../nfl/reference/nfl_api.md#nfl_game_summaries).


```python
wgd = safe(
    "NFL.com weekly game details",
    lambda: nfl.nfl_weekly_game_details(season=SEASON, season_type="REG", week=1),
)
cols = ["week", "date", "game_type", "away_team_full_name", "home_team_full_name", "status"]
(wgd.select([c for c in cols if c in wgd.columns]).head(8)
 if wgd is not None else "weekly game details unavailable")

```

    ✅ NFL.com weekly game details





    shape: (8, 6)
    ┌──────┬────────────┬─────────────┬──────────────────────┬─────────────────────┬───────────┐
    │ week ┆ date       ┆ game_type   ┆ away_team_full_name  ┆ home_team_full_name ┆ status    │
    │ ---  ┆ ---        ┆ ---         ┆ ---                  ┆ ---                 ┆ ---       │
    │ i64  ┆ str        ┆ str         ┆ str                  ┆ str                 ┆ str       │
    ╞══════╪════════════╪═════════════╪══════════════════════╪═════════════════════╪═══════════╡
    │ 1    ┆ 2024-09-06 ┆ UNSPECIFIED ┆ Baltimore Ravens     ┆ Kansas City Chiefs  ┆ SCHEDULED │
    │ 1    ┆ 2024-09-07 ┆ UNSPECIFIED ┆ Green Bay Packers    ┆ Philadelphia Eagles ┆ SCHEDULED │
    │ 1    ┆ 2024-09-08 ┆ UNSPECIFIED ┆ Pittsburgh Steelers  ┆ Atlanta Falcons     ┆ SCHEDULED │
    │ 1    ┆ 2024-09-08 ┆ UNSPECIFIED ┆ Arizona Cardinals    ┆ Buffalo Bills       ┆ SCHEDULED │
    │ 1    ┆ 2024-09-08 ┆ UNSPECIFIED ┆ Tennessee Titans     ┆ Chicago Bears       ┆ SCHEDULED │
    │ 1    ┆ 2024-09-08 ┆ UNSPECIFIED ┆ New England Patriots ┆ Cincinnati Bengals  ┆ SCHEDULED │
    │ 1    ┆ 2024-09-08 ┆ UNSPECIFIED ┆ Houston Texans       ┆ Indianapolis Colts  ┆ SCHEDULED │
    │ 1    ┆ 2024-09-08 ┆ UNSPECIFIED ┆ Jacksonville Jaguars ┆ Miami Dolphins      ┆ SCHEDULED │
    └──────┴────────────┴─────────────┴──────────────────────┴─────────────────────┴───────────┘



## ⚡ NextGen Stats: the tracking layer

This is where it gets *fun*. The NFL's **NextGen Stats** API exposes
player-tracking metrics you won't find in a box score — time to throw,
completion percentage over expectation (CPOE), separation, ball-carrier
top speed. All token-free.

[`nfl_ngs_statboard`](../nfl/reference/additional.md#nfl_ngs_statboard) is the
season leaderboard. Ask for `stat_type` `"passing"`, `"rushing"`, or
`"receiving"`.


```python
qb = safe(
    "NGS passing statboard",
    lambda: nfl.nfl_ngs_statboard(stat_type="passing", season=SEASON, season_type="REG"),
)
cols = [
    "playerName", "passerRating", "completionPercentageAboveExpectation",
    "avgTimeToThrow", "aggressiveness", "passYards", "passTouchdowns",
]
(qb.select([c for c in cols if c in qb.columns])
   .sort("passerRating", descending=True)
   .head(10)
 if qb is not None else "NGS statboard unavailable")

```

    ✅ NGS passing statboard





    shape: (10, 7)
    ┌──────────────┬──────────────┬──────────────┬─────────────┬─────────────┬───────────┬─────────────┐
    │ playerName   ┆ passerRating ┆ completionPe ┆ avgTimeToTh ┆ aggressiven ┆ passYards ┆ passTouchdo │
    │ ---          ┆ ---          ┆ rcentageAbov ┆ row         ┆ ess         ┆ ---       ┆ wns         │
    │ str          ┆ f64          ┆ eExpec…      ┆ ---         ┆ ---         ┆ i64       ┆ ---         │
    │              ┆              ┆ ---          ┆ f64         ┆ f64         ┆           ┆ i64         │
    │              ┆              ┆ f64          ┆             ┆             ┆           ┆             │
    ╞══════════════╪══════════════╪══════════════╪═════════════╪═════════════╪═══════════╪═════════════╡
    │ Lamar        ┆ 119.629044   ┆ -0.194641    ┆ 3.144269    ┆ 10.970464   ┆ 4172      ┆ 41          │
    │ Jackson      ┆              ┆              ┆             ┆             ┆           ┆             │
    │ Jared Goff   ┆ 111.769481   ┆ 5.054879     ┆ 2.787295    ┆ 11.502783   ┆ 4629      ┆ 37          │
    │ Joe Burrow   ┆ 108.537832   ┆ 4.521902     ┆ 2.710951    ┆ 15.797546   ┆ 4918      ┆ 43          │
    │ Baker        ┆ 106.761696   ┆ 2.17107      ┆ 2.6982      ┆ 10.877193   ┆ 4500      ┆ 41          │
    │ Mayfield     ┆              ┆              ┆             ┆             ┆           ┆             │
    │ Jalen Hurts  ┆ 103.687673   ┆ 6.305457     ┆ 3.1313      ┆ 16.34349    ┆ 2903      ┆ 18          │
    │ Sam Darnold  ┆ 102.534404   ┆ 2.799064     ┆ 3.083395    ┆ 13.944954   ┆ 4319      ┆ 35          │
    │ Justin       ┆ 101.703042   ┆ 2.391567     ┆ 2.910539    ┆ 15.873016   ┆ 3870      ┆ 23          │
    │ Herbert      ┆              ┆              ┆             ┆             ┆           ┆             │
    │ Josh Allen   ┆ 101.384576   ┆ 0.781139     ┆ 2.886884    ┆ 16.770186   ┆ 3731      ┆ 28          │
    │ Tua          ┆ 101.362782   ┆ 1.696667     ┆ 2.416476    ┆ 12.280702   ┆ 2867      ┆ 19          │
    │ Tagovailoa   ┆              ┆              ┆             ┆             ┆           ┆             │
    │ Derek Carr   ┆ 101.022999   ┆ 3.222724     ┆ 2.758918    ┆ 11.111111   ┆ 2145      ┆ 15          │
    └──────────────┴──────────────┴──────────────┴─────────────┴─────────────┴───────────┴─────────────┘



And [`nfl_ngs_leaders`](../nfl/reference/additional.md#nfl_ngs_leaders)
serves the highlight-reel top-N boards — each row is the *play* that earned
the leader their spot. Categories include `"speed"` (fastest ball carriers),
`"yac_season"` (yards-after-catch over expected), `"completion_season"`
(most-improbable completions) and more.


```python
fast = safe(
    "NGS fastest ball carriers",
    lambda: nfl.nfl_ngs_leaders(category="speed", season=SEASON, season_type="REG"),
)
cols = ["leader_playerName", "leader_teamAbbr", "leader_maxSpeed", "leader_yards", "play_playDescription"]
(fast.select([c for c in cols if c in fast.columns]).head(8)
 if fast is not None else "NGS leaders unavailable")

```

    ✅ NGS fastest ball carriers





    shape: (8, 5)
    ┌───────────────────┬─────────────────┬─────────────────┬──────────────┬───────────────────────────┐
    │ leader_playerName ┆ leader_teamAbbr ┆ leader_maxSpeed ┆ leader_yards ┆ play_playDescription      │
    │ ---               ┆ ---             ┆ ---             ┆ ---          ┆ ---                       │
    │ str               ┆ str             ┆ f64             ┆ i64          ┆ str                       │
    ╞═══════════════════╪═════════════════╪═════════════════╪══════════════╪═══════════════════════════╡
    │ KaVontae Turpin   ┆ DAL             ┆ 22.356818       ┆ 64           ┆ (15:00) (Shotgun) C.Rush  │
    │                   ┆                 ┆                 ┆              ┆ pass …                    │
    │ Brian Thomas Jr.  ┆ JAX             ┆ 22.152273       ┆ 85           ┆ (7:12) (Shotgun)          │
    │                   ┆                 ┆                 ┆              ┆ T.Lawrence pa…            │
    │ Jahmyr Gibbs      ┆ DET             ┆ 22.029546       ┆ 70           ┆ (4:07) (Shotgun) J.Gibbs  │
    │                   ┆                 ┆                 ┆              ┆ left …                    │
    │ Saquon Barkley    ┆ PHI             ┆ 21.927273       ┆ 55           ┆ (11:04) (No Huddle,       │
    │                   ┆                 ┆                 ┆              ┆ Shotgun) S…               │
    │ Saquon Barkley    ┆ PHI             ┆ 21.906818       ┆ 72           ┆ (2:54) (Shotgun)          │
    │                   ┆                 ┆                 ┆              ┆ S.Barkley lef…            │
    │ Nico Collins      ┆ HOU             ┆ 21.886364       ┆ 55           ┆ (12:56) C.Stroud pass     │
    │                   ┆                 ┆                 ┆              ┆ deep mid…                 │
    │ James Cook        ┆ BUF             ┆ 21.845455       ┆ 65           ┆ (8:48) A.Anderson         │
    │                   ┆                 ┆                 ┆              ┆ reported in …             │
    │ KaVontae Turpin   ┆ DAL             ┆ 21.845455       ┆ 18           ┆ J.Elliott kicks 60 yards  │
    │                   ┆                 ┆                 ┆              ┆ from …                    │
    └───────────────────┴─────────────────┴─────────────────┴──────────────┴───────────────────────────┘



## 📦 nflverse loaders: the bulk-data workhorses

For full-season modelling you want the **nflverse release parquets** — the
exact same assets that power nflfastR / nflreadr / nflreadpy. These are
versioned, cached releases (very reliable), so we call them directly.

| Function | Rows | Highlights |
|---|---|---|
| [`load_nfl_pbp`](../nfl/reference/loaders.md#load_nfl_pbp) | ~49k/season | EPA, WP, air yards, 370+ columns |
| [`load_nfl_player_stats`](../nfl/reference/additional.md#load_nfl_player_stats) | weekly | passing/rushing/receiving box lines |
| [`load_nfl_nextgen_stats`](../nfl/reference/additional.md#load_nfl_nextgen_stats) | weekly | NGS back to 2016 |
| [`load_nfl_rosters`](../nfl/reference/loaders.md#load_nfl_rosters) | per player | IDs, bios, draft info |


```python
pbp = nfl.load_nfl_pbp([SEASON])
pbp.shape

```




    (49492, 372)




```python
(pbp
    .filter(pl.col("play_type").is_not_null())
    .select(["game_id", "qtr", "down", "ydstogo", "posteam", "play_type", "yards_gained", "epa", "desc"])
    .head(8))

```




    shape: (8, 9)
    ┌────────────────┬─────┬──────┬─────────┬───┬───────────┬──────────────┬───────────┬───────────────┐
    │ game_id        ┆ qtr ┆ down ┆ ydstogo ┆ … ┆ play_type ┆ yards_gained ┆ epa       ┆ desc          │
    │ ---            ┆ --- ┆ ---  ┆ ---     ┆   ┆ ---       ┆ ---          ┆ ---       ┆ ---           │
    │ str            ┆ f64 ┆ f64  ┆ f64     ┆   ┆ str       ┆ f64          ┆ f64       ┆ str           │
    ╞════════════════╪═════╪══════╪═════════╪═══╪═══════════╪══════════════╪═══════════╪═══════════════╡
    │ 2024_01_ARI_BU ┆ 1.0 ┆ null ┆ 0.0     ┆ … ┆ kickoff   ┆ 0.0          ┆ 0.257819  ┆ 2-T.Bass      │
    │ F              ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ kicks 65      │
    │                ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ yards from B… │
    │ 2024_01_ARI_BU ┆ 1.0 ┆ 1.0  ┆ 10.0    ┆ … ┆ run       ┆ 3.0          ┆ -0.200602 ┆ (15:00)       │
    │ F              ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ 6-J.Conner up │
    │                ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ the midd…     │
    │ 2024_01_ARI_BU ┆ 1.0 ┆ 2.0  ┆ 7.0     ┆ … ┆ pass      ┆ 22.0         ┆ 2.028874  ┆ (14:27)       │
    │ F              ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ 1-K.Murray    │
    │                ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ pass short …  │
    │ 2024_01_ARI_BU ┆ 1.0 ┆ 1.0  ┆ 10.0    ┆ … ┆ pass      ┆ 9.0          ┆ 0.754242  ┆ (13:43)       │
    │ F              ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ (Shotgun)     │
    │                ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ 1-K.Murray p… │
    │ 2024_01_ARI_BU ┆ 1.0 ┆ 2.0  ┆ 1.0     ┆ … ┆ run       ┆ 2.0          ┆ -0.029602 ┆ (13:02)       │
    │ F              ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ 6-J.Conner up │
    │                ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ the midd…     │
    │ 2024_01_ARI_BU ┆ 1.0 ┆ 1.0  ┆ 10.0    ┆ … ┆ run       ┆ 2.0          ┆ -0.247749 ┆ (12:26)       │
    │ F              ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ (Shotgun)     │
    │                ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ 6-J.Conner l… │
    │ 2024_01_ARI_BU ┆ 1.0 ┆ 2.0  ┆ 8.0     ┆ … ┆ run       ┆ 2.0          ┆ -0.530139 ┆ (11:51)       │
    │ F              ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ 6-J.Conner    │
    │                ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ left end to…  │
    │ 2024_01_ARI_BU ┆ 1.0 ┆ 3.0  ┆ 6.0     ┆ … ┆ pass      ┆ 8.0          ┆ 1.6808    ┆ (11:08)       │
    │ F              ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ (Shotgun)     │
    │                ┆     ┆      ┆         ┆   ┆           ┆              ┆           ┆ 1-K.Murray p… │
    └────────────────┴─────┴──────┴─────────┴───┴───────────┴──────────────┴───────────┴───────────────┘




```python
ngs_release = nfl.load_nfl_nextgen_stats([SEASON], stat_type="passing")
(ngs_release
    .filter(pl.col("week") == 0)  # week 0 == season totals in this release
    .select(["player_display_name", "team_abbr", "attempts", "pass_yards",
             "completion_percentage_above_expectation", "passer_rating"])
    .sort("passer_rating", descending=True)
    .head(8))

```




    shape: (8, 6)
    ┌─────────────────────┬───────────┬──────────┬────────────┬────────────────────────┬───────────────┐
    │ player_display_name ┆ team_abbr ┆ attempts ┆ pass_yards ┆ completion_percentage_ ┆ passer_rating │
    │ ---                 ┆ ---       ┆ ---      ┆ ---        ┆ above_ex…              ┆ ---           │
    │ str                 ┆ str       ┆ i32      ┆ i32        ┆ ---                    ┆ f64           │
    │                     ┆           ┆          ┆            ┆ f64                    ┆               │
    ╞═════════════════════╪═══════════╪══════════╪════════════╪════════════════════════╪═══════════════╡
    │ Lamar Jackson       ┆ BAL       ┆ 474      ┆ 4172       ┆ -0.194641              ┆ 119.629044    │
    │ Jared Goff          ┆ DET       ┆ 539      ┆ 4629       ┆ 5.054879               ┆ 111.769481    │
    │ Joe Burrow          ┆ CIN       ┆ 652      ┆ 4918       ┆ 4.521902               ┆ 108.537832    │
    │ Baker Mayfield      ┆ TB        ┆ 570      ┆ 4500       ┆ 2.17107                ┆ 106.761696    │
    │ Jalen Hurts         ┆ PHI       ┆ 361      ┆ 2903       ┆ 6.305457               ┆ 103.687673    │
    │ Sam Darnold         ┆ MIN       ┆ 545      ┆ 4319       ┆ 2.799064               ┆ 102.534404    │
    │ Justin Herbert      ┆ LAC       ┆ 504      ┆ 3870       ┆ 2.391567               ┆ 101.703042    │
    │ Josh Allen          ┆ BUF       ┆ 483      ┆ 3731       ┆ 0.781139               ┆ 101.384576    │
    └─────────────────────┴───────────┴──────────┴────────────┴────────────────────────┴───────────────┘



## 🔵 Secondary path: ESPN (quick & no-auth)

When you just want a fast scoreboard without minting a token, ESPN is right
there. [`espn_nfl_schedule`](../nfl/reference/additional.md#espn_nfl_schedule)
returns a tidy schedule frame; pass `dates=YYYYMMDD` for a single day. (There's
also a raw [`espn_nfl_scoreboard`](../nfl/reference/site.md#espn_nfl_scoreboard)
if you want the unparsed JSON.)


```python
espn_sched = safe("ESPN schedule", lambda: nfl.espn_nfl_schedule(dates=20240908))
cols = ["id", "away_display_name", "home_display_name", "away_score", "home_score", "status_type_description"]
(espn_sched.select([c for c in cols if c in espn_sched.columns]).head(8)
 if espn_sched is not None else "ESPN schedule unavailable")

```

    ✅ ESPN schedule





    shape: (8, 6)
    ┌───────────┬────────────────────┬───────────────────┬────────────┬────────────┬───────────────────┐
    │ id        ┆ away_display_name  ┆ home_display_name ┆ away_score ┆ home_score ┆ status_type_descr │
    │ ---       ┆ ---                ┆ ---               ┆ ---        ┆ ---        ┆ iption            │
    │ str       ┆ str                ┆ str               ┆ str        ┆ str        ┆ ---               │
    │           ┆                    ┆                   ┆            ┆            ┆ str               │
    ╞═══════════╪════════════════════╪═══════════════════╪════════════╪════════════╪═══════════════════╡
    │ 401671744 ┆ Pittsburgh         ┆ Atlanta Falcons   ┆ 18         ┆ 10         ┆ Final             │
    │           ┆ Steelers           ┆                   ┆            ┆            ┆                   │
    │ 401671617 ┆ Arizona Cardinals  ┆ Buffalo Bills     ┆ 28         ┆ 34         ┆ Final             │
    │ 401671719 ┆ Tennessee Titans   ┆ Chicago Bears     ┆ 17         ┆ 24         ┆ Final             │
    │ 401671628 ┆ New England        ┆ Cincinnati        ┆ 16         ┆ 10         ┆ Final             │
    │           ┆ Patriots           ┆ Bengals           ┆            ┆            ┆                   │
    │ 401671861 ┆ Houston Texans     ┆ Indianapolis      ┆ 29         ┆ 27         ┆ Final             │
    │           ┆                    ┆ Colts             ┆            ┆            ┆                   │
    │ 401671849 ┆ Jacksonville       ┆ Miami Dolphins    ┆ 17         ┆ 20         ┆ Final             │
    │           ┆ Jaguars            ┆                   ┆            ┆            ┆                   │
    │ 401671734 ┆ Carolina Panthers  ┆ New Orleans       ┆ 10         ┆ 47         ┆ Final             │
    │           ┆                    ┆ Saints            ┆            ┆            ┆                   │
    │ 401671712 ┆ Minnesota Vikings  ┆ New York Giants   ┆ 28         ┆ 6          ┆ Final             │
    └───────────┴────────────────────┴───────────────────┴────────────┴────────────┴───────────────────┘



## 🍳 Cookbook: common NFL tasks

A full dozen recipes you'll reach for constantly — leaderboards, splits,
team-level efficiency, snap-share workhorses, play-by-play slices, a
schedule scan, and a quick hop into pandas. Each one leans on the
**premium** native/NextGen feeds or the rock-solid **nflverse** release
parquets, and every live call is wrapped so a network blip never breaks
your run. Recipes 1–4 use the live NFL.com / NextGen endpoints; 5–12 build
on the cached release parquets, so they run anywhere, anytime. 🏈

### Recipe 1 — This week's "Out" list 🚑

Filter the official injury report down to players ruled **Out** — exactly what
you'd check before setting a lineup.


```python
rep = safe(
    "injury report",
    lambda: nfl.nfl_injuries(season=SEASON, season_type="REG", week=1),
)
if rep is not None and rep.height and "injury_status" in rep.columns:
    out = (
        rep.filter(pl.col("injury_status").str.to_lowercase() == "out")
           .select([c for c in ["team_full_name", "person_display_name", "position", "injuries"]
                    if c in rep.columns])
           .head(15)
    )
else:
    out = "injury report unavailable"
out

```

    ✅ injury report





    shape: (5, 4)
    ┌──────────────────┬─────────────────────┬──────────┬─────────────────────────────────┐
    │ team_full_name   ┆ person_display_name ┆ position ┆ injuries                        │
    │ ---              ┆ ---                 ┆ ---      ┆ ---                             │
    │ str              ┆ str                 ┆ str      ┆ str                             │
    ╞══════════════════╪═════════════════════╪══════════╪═════════════════════════════════╡
    │ Atlanta Falcons  ┆ Antonio Hamilton    ┆ CB       ┆ ['Groin']                       │
    │ Baltimore Ravens ┆ Adisa Isaac         ┆ LB       ┆ ['Hamstring']                   │
    │ Baltimore Ravens ┆ Kyle Van Noy        ┆ LB       ┆ ['gameday concussion protocol … │
    │ Buffalo Bills    ┆ Taron Johnson       ┆ CB       ┆ ['Forearm']                     │
    │ Buffalo Bills    ┆ Javon Solomon       ┆ DE       ┆ ['Oblique']                     │
    └──────────────────┴─────────────────────┴──────────┴─────────────────────────────────┘



### Recipe 2 — CPOE leaderboard from NextGen Stats 🎯

Who's beating expectation as a passer? Rank qualified QBs by **completion
percentage above expectation** straight off the NextGen statboard.


```python
board = safe(
    "NGS passing board",
    lambda: nfl.nfl_ngs_statboard(stat_type="passing", season=SEASON, season_type="REG"),
)
if board is not None and board.height and "completionPercentageAboveExpectation" in board.columns:
    cpoe = (
        board.filter(pl.col("attempts") >= 200)
             .select(["playerName", "attempts", "completionPercentage",
                      "completionPercentageAboveExpectation", "passerRating"])
             .sort("completionPercentageAboveExpectation", descending=True)
             .head(10)
    )
else:
    cpoe = "NGS board unavailable"
cpoe

```

    ✅ NGS passing board





    shape: (10, 5)
    ┌────────────────┬──────────┬──────────────────────┬────────────────────────────────┬──────────────┐
    │ playerName     ┆ attempts ┆ completionPercentage ┆ completionPercentageAboveExpec ┆ passerRating │
    │ ---            ┆ ---      ┆ ---                  ┆ …                              ┆ ---          │
    │ str            ┆ i64      ┆ f64                  ┆ ---                            ┆ f64          │
    │                ┆          ┆                      ┆ f64                            ┆              │
    ╞════════════════╪══════════╪══════════════════════╪════════════════════════════════╪══════════════╡
    │ Jalen Hurts    ┆ 361      ┆ 68.698061            ┆ 6.305457                       ┆ 103.687673   │
    │ Jared Goff     ┆ 539      ┆ 72.356215            ┆ 5.054879                       ┆ 111.769481   │
    │ Joe Burrow     ┆ 652      ┆ 70.552147            ┆ 4.521902                       ┆ 108.537832   │
    │ Geno Smith     ┆ 578      ┆ 70.415225            ┆ 3.854464                       ┆ 93.202134    │
    │ Kirk Cousins   ┆ 453      ┆ 66.887417            ┆ 3.442009                       ┆ 88.61755     │
    │ Derek Carr     ┆ 279      ┆ 67.741935            ┆ 3.222724                       ┆ 101.022999   │
    │ Drake Maye     ┆ 338      ┆ 66.568047            ┆ 2.986893                       ┆ 88.079389    │
    │ Brock Purdy    ┆ 455      ┆ 65.934066            ┆ 2.983077                       ┆ 96.076007    │
    │ Sam Darnold    ┆ 545      ┆ 66.238532            ┆ 2.799064                       ┆ 102.534404   │
    │ Justin Herbert ┆ 504      ┆ 65.873016            ┆ 2.391567                       ┆ 101.703042   │
    └────────────────┴──────────┴──────────────────────┴────────────────────────────────┴──────────────┘



### Recipe 3 — Standings → division winners 🏆

Take the premium standings and pull the team that tops each division. One
group-by and you've got your playoff-seeding cheat sheet.


```python
st = safe(
    "standings",
    lambda: nfl.nfl_standings(season=SEASON, season_type="REG", week=18),
)
if st is not None and st.height and {"division_rank", "team_full_name"}.issubset(st.columns):
    div_col = next((c for c in ["team_division_full_name", "division_full_name", "division"] if c in st.columns), None)
    keep = [c for c in [div_col, "team_full_name", "overall_wins", "overall_losses"] if c]
    winners = (
        st.filter(pl.col("division_rank") == 1)
          .select(keep)
          .sort(div_col) if div_col else st.filter(pl.col("division_rank") == 1).select(keep)
    )
else:
    winners = "standings unavailable"
winners

```

    ✅ standings





    shape: (8, 3)
    ┌──────────────────────┬──────────────┬────────────────┐
    │ team_full_name       ┆ overall_wins ┆ overall_losses │
    │ ---                  ┆ ---          ┆ ---            │
    │ str                  ┆ i64          ┆ i64            │
    ╞══════════════════════╪══════════════╪════════════════╡
    │ Baltimore Ravens     ┆ 12           ┆ 5              │
    │ Buffalo Bills        ┆ 13           ┆ 4              │
    │ Detroit Lions        ┆ 15           ┆ 2              │
    │ Houston Texans       ┆ 10           ┆ 7              │
    │ Kansas City Chiefs   ┆ 15           ┆ 2              │
    │ Los Angeles Rams     ┆ 10           ┆ 7              │
    │ Philadelphia Eagles  ┆ 14           ┆ 3              │
    │ Tampa Bay Buccaneers ┆ 10           ┆ 7              │
    └──────────────────────┴──────────────┴────────────────┘



### Recipe 4 — A game's NextGen passer splits 🔬

Grab an NGS `gameId` from the schedule, then pull
[`nfl_ngs_gamecenter_overview`](../nfl/reference/additional.md#nfl_ngs_gamecenter_overview)
to see each side's primary passer with tracking-derived splits.


```python
sched = safe(
    "NGS schedule",
    lambda: nfl.nfl_ngs_league_schedule(season=SEASON, season_type="REG", week=1),
)
if sched is not None and sched.height and "gameId" in sched.columns:
    gid = sched["gameId"][0]
    ov = safe(f"NGS gamecenter {gid}",
              lambda: nfl.nfl_ngs_gamecenter_overview(game_id=gid, group="passers"))
    if ov is not None and ov.height:
        out = ov.select([c for c in ["side", "teamAbbr", "playerName", "position",
                                     "completions", "attempts", "passYards", "touchdowns"]
                         if c in ov.columns])
    else:
        out = "gamecenter unavailable"
else:
    out = "NGS schedule unavailable"
out

```

    ✅ NGS schedule


    ✅ NGS gamecenter 2024090500





    shape: (2, 8)
    ┌─────────┬──────────┬────────────────┬──────────┬─────────────┬──────────┬───────────┬────────────┐
    │ side    ┆ teamAbbr ┆ playerName     ┆ position ┆ completions ┆ attempts ┆ passYards ┆ touchdowns │
    │ ---     ┆ ---      ┆ ---            ┆ ---      ┆ ---         ┆ ---      ┆ ---       ┆ ---        │
    │ str     ┆ str      ┆ str            ┆ str      ┆ i64         ┆ i64      ┆ i64       ┆ i64        │
    ╞═════════╪══════════╪════════════════╪══════════╪═════════════╪══════════╪═══════════╪════════════╡
    │ home    ┆ KC       ┆ Patrick        ┆ QB       ┆ 20          ┆ 28       ┆ 291       ┆ 1          │
    │         ┆          ┆ Mahomes        ┆          ┆             ┆          ┆           ┆            │
    │ visitor ┆ BAL      ┆ Lamar Jackson  ┆ QB       ┆ 26          ┆ 41       ┆ 273       ┆ 1          │
    └─────────┴──────────┴────────────────┴──────────┴─────────────┴──────────┴───────────┴────────────┘



### Recipe 5 — Season rushing leaders 🏃

Roll the weekly box scores in [`load_nfl_player_stats`](../nfl/reference/additional.md#load_nfl_player_stats) up to season totals and crown the ground-game kings (≥150 carries).


```python
ps = nfl.load_nfl_player_stats()
rush_cols = {"season", "season_type", "carries", "rushing_yards"}
if rush_cols.issubset(ps.columns):
    rush_lb = (
        ps.filter((pl.col("season") == SEASON) & (pl.col("season_type") == "REG"))
          .group_by(["player_display_name", "recent_team"])
          .agg(
              pl.col("carries").sum().alias("carries"),
              pl.col("rushing_yards").sum().alias("rush_yds"),
              pl.col("rushing_tds").sum().alias("rush_td"),
              pl.col("rushing_epa").sum().round(1).alias("rush_epa"),
          )
          .filter(pl.col("carries") >= 150)
          .sort("rush_yds", descending=True)
          .head(10)
    )
else:
    rush_lb = "player_stats schema changed — rushing columns missing"
rush_lb
```




    shape: (10, 6)
    ┌─────────────────────┬─────────────┬─────────┬──────────┬─────────┬──────────┐
    │ player_display_name ┆ recent_team ┆ carries ┆ rush_yds ┆ rush_td ┆ rush_epa │
    │ ---                 ┆ ---         ┆ ---     ┆ ---      ┆ ---     ┆ ---      │
    │ str                 ┆ str         ┆ i32     ┆ f64      ┆ i32     ┆ f64      │
    ╞═════════════════════╪═════════════╪═════════╪══════════╪═════════╪══════════╡
    │ Saquon Barkley      ┆ PHI         ┆ 345     ┆ 2005.0   ┆ 13      ┆ 34.1     │
    │ Derrick Henry       ┆ BAL         ┆ 325     ┆ 1921.0   ┆ 16      ┆ 40.6     │
    │ Bijan Robinson      ┆ ATL         ┆ 304     ┆ 1456.0   ┆ 14      ┆ 16.9     │
    │ Jonathan Taylor     ┆ IND         ┆ 303     ┆ 1431.0   ┆ 11      ┆ -21.0    │
    │ Jahmyr Gibbs        ┆ DET         ┆ 250     ┆ 1412.0   ┆ 16      ┆ 35.1     │
    │ Josh Jacobs         ┆ GB          ┆ 301     ┆ 1329.0   ┆ 15      ┆ -18.3    │
    │ Kyren Williams      ┆ LA          ┆ 316     ┆ 1299.0   ┆ 14      ┆ -23.5    │
    │ Chuba Hubbard       ┆ CAR         ┆ 250     ┆ 1195.0   ┆ 10      ┆ 9.7      │
    │ Aaron Jones         ┆ MIN         ┆ 255     ┆ 1138.0   ┆ 5       ┆ -13.4    │
    │ Bucky Irving        ┆ TB          ┆ 207     ┆ 1122.0   ┆ 8       ┆ 20.9     │
    └─────────────────────┴─────────────┴─────────┴──────────┴─────────┴──────────┘



### Recipe 6 — The most efficient offenses (EPA/play) 📈

Expected points added is the modeller's favourite efficiency yardstick. Average `epa` over every run/pass in [`load_nfl_pbp`](../nfl/reference/loaders.md#load_nfl_pbp) to rank offenses.


```python
pbp = nfl.load_nfl_pbp([SEASON])
if {"epa", "posteam", "play_type"}.issubset(pbp.columns):
    epa_off = (
        pbp.filter(pl.col("play_type").is_in(["run", "pass"]))
           .group_by("posteam")
           .agg(
               pl.col("epa").mean().round(3).alias("epa_per_play"),
               pl.len().alias("plays"),
           )
           .filter(pl.col("posteam").is_not_null())
           .sort("epa_per_play", descending=True)
           .head(10)
    )
else:
    epa_off = "pbp schema changed — epa/posteam columns missing"
epa_off
```




    shape: (10, 3)
    ┌─────────┬──────────────┬───────┐
    │ posteam ┆ epa_per_play ┆ plays │
    │ ---     ┆ ---          ┆ ---   │
    │ str     ┆ f64          ┆ u32   │
    ╞═════════╪══════════════╪═══════╡
    │ BAL     ┆ 0.219        ┆ 1167  │
    │ BUF     ┆ 0.19         ┆ 1202  │
    │ DET     ┆ 0.165        ┆ 1164  │
    │ WAS     ┆ 0.132        ┆ 1308  │
    │ TB      ┆ 0.129        ┆ 1128  │
    │ PHI     ┆ 0.119        ┆ 1341  │
    │ CIN     ┆ 0.09         ┆ 1069  │
    │ GB      ┆ 0.071        ┆ 1081  │
    │ SF      ┆ 0.069        ┆ 1014  │
    │ ARI     ┆ 0.067        ┆ 1031  │
    └─────────┴──────────────┴───────┘



### Recipe 7 — Third-down conversion kings 🔑

Move-the-chains efficiency: keep only 3rd-down run/pass snaps and divide conversions by attempts per offense — a classic play-by-play split.


```python
if {"down", "third_down_converted", "posteam"}.issubset(pbp.columns):
    third = (
        pbp.filter((pl.col("down") == 3) & (pl.col("play_type").is_in(["run", "pass"])))
           .group_by("posteam")
           .agg(
               pl.col("third_down_converted").sum().alias("conversions"),
               pl.len().alias("attempts"),
           )
           .filter(pl.col("posteam").is_not_null())
           .with_columns((pl.col("conversions") / pl.col("attempts") * 100).round(1).alias("conv_pct"))
           .sort("conv_pct", descending=True)
           .head(10)
    )
else:
    third = "pbp schema changed — third-down columns missing"
third
```




    shape: (10, 4)
    ┌─────────┬─────────────┬──────────┬──────────┐
    │ posteam ┆ conversions ┆ attempts ┆ conv_pct │
    │ ---     ┆ ---         ┆ ---      ┆ ---      │
    │ str     ┆ f64         ┆ u32      ┆ f64      │
    ╞═════════╪═════════════╪══════════╪══════════╡
    │ TB      ┆ 115.0       ┆ 226      ┆ 50.9     │
    │ BAL     ┆ 109.0       ┆ 214      ┆ 50.9     │
    │ KC      ┆ 123.0       ┆ 255      ┆ 48.2     │
    │ DET     ┆ 101.0       ┆ 213      ┆ 47.4     │
    │ CIN     ┆ 100.0       ┆ 214      ┆ 46.7     │
    │ BUF     ┆ 107.0       ┆ 237      ┆ 45.1     │
    │ WAS     ┆ 118.0       ┆ 262      ┆ 45.0     │
    │ ARI     ┆ 83.0        ┆ 192      ┆ 43.2     │
    │ SF      ┆ 84.0        ┆ 196      ┆ 42.9     │
    │ PHI     ┆ 114.0       ┆ 278      ┆ 41.0     │
    └─────────┴─────────────┴──────────┴──────────┘



### Recipe 8 — Red-zone touchdown efficiency 🎯

Filter the play-by-play to snaps inside the opponent's 20 (`yardline_100 ≤ 20`) and see which offenses actually punch it in instead of settling for three.


```python
if {"yardline_100", "touchdown", "posteam"}.issubset(pbp.columns):
    redzone = (
        pbp.filter((pl.col("yardline_100") <= 20) & (pl.col("play_type").is_in(["run", "pass"])))
           .group_by("posteam")
           .agg(
               pl.col("touchdown").sum().alias("rz_tds"),
               pl.len().alias("rz_plays"),
           )
           .filter((pl.col("posteam").is_not_null()) & (pl.col("rz_plays") >= 80))
           .with_columns((pl.col("rz_tds") / pl.col("rz_plays") * 100).round(1).alias("td_pct"))
           .sort("td_pct", descending=True)
           .head(10)
    )
else:
    redzone = "pbp schema changed — red-zone columns missing"
redzone
```




    shape: (10, 4)
    ┌─────────┬────────┬──────────┬────────┐
    │ posteam ┆ rz_tds ┆ rz_plays ┆ td_pct │
    │ ---     ┆ ---    ┆ ---      ┆ ---    │
    │ str     ┆ f64    ┆ u32      ┆ f64    │
    ╞═════════╪════════╪══════════╪════════╡
    │ BAL     ┆ 55.0   ┆ 178      ┆ 30.9   │
    │ TB      ┆ 47.0   ┆ 175      ┆ 26.9   │
    │ BUF     ┆ 55.0   ┆ 232      ┆ 23.7   │
    │ DEN     ┆ 36.0   ┆ 157      ┆ 22.9   │
    │ DET     ┆ 54.0   ┆ 239      ┆ 22.6   │
    │ GB      ┆ 43.0   ┆ 195      ┆ 22.1   │
    │ SEA     ┆ 27.0   ┆ 125      ┆ 21.6   │
    │ NO      ┆ 25.0   ┆ 120      ┆ 20.8   │
    │ NYJ     ┆ 31.0   ┆ 150      ┆ 20.7   │
    │ WAS     ┆ 52.0   ┆ 255      ┆ 20.4   │
    └─────────┴────────┴──────────┴────────┘



### Recipe 9 — Snap-share workhorse running backs 🐴

[`load_nfl_snap_counts`](../nfl/reference/loaders.md#load_nfl_snap_counts) carries `offense_pct` per game — average it to find the backs their teams simply would not take off the field.


```python
snaps = nfl.load_nfl_snap_counts([SEASON])
if {"position", "offense_pct", "player"}.issubset(snaps.columns):
    workhorses = (
        snaps.filter(pl.col("position") == "RB")
             .group_by(["player", "team"])
             .agg(
                 (pl.col("offense_pct").mean() * 100).round(1).alias("avg_snap_pct"),
                 pl.len().alias("games"),
             )
             .filter(pl.col("games") >= 10)
             .sort("avg_snap_pct", descending=True)
             .head(10)
    )
else:
    workhorses = "snap_counts schema changed — offense_pct/position missing"
workhorses
```




    shape: (10, 4)
    ┌─────────────────┬──────┬──────────────┬───────┐
    │ player          ┆ team ┆ avg_snap_pct ┆ games │
    │ ---             ┆ ---  ┆ ---          ┆ ---   │
    │ str             ┆ str  ┆ f64          ┆ u32   │
    ╞═════════════════╪══════╪══════════════╪═══════╡
    │ Kyren Williams  ┆ LA   ┆ 87.2         ┆ 18    │
    │ Jonathan Taylor ┆ IND  ┆ 80.3         ┆ 14    │
    │ Chuba Hubbard   ┆ CAR  ┆ 77.3         ┆ 15    │
    │ Bijan Robinson  ┆ ATL  ┆ 75.4         ┆ 17    │
    │ Saquon Barkley  ┆ PHI  ┆ 75.1         ┆ 20    │
    │ Breece Hall     ┆ NYJ  ┆ 72.4         ┆ 16    │
    │ Alvin Kamara    ┆ NO   ┆ 70.9         ┆ 14    │
    │ Tony Pollard    ┆ TEN  ┆ 67.8         ┆ 16    │
    │ D'Andre Swift   ┆ CHI  ┆ 66.8         ┆ 17    │
    │ Aaron Jones     ┆ MIN  ┆ 63.7         ┆ 18    │
    └─────────────────┴──────┴──────────────┴───────┘



### Recipe 10 — Receiving leaders, then a hop into pandas 🐼

Aggregate the receiving box lines, `.to_pandas()`, and add derived columns (yards-per-catch, catch rate) with familiar pandas syntax — the polars→pandas handoff is one method call.


```python
rec_cols = {"receptions", "receiving_yards", "targets", "season", "season_type"}
if rec_cols.issubset(ps.columns):
    rec = (
        ps.filter((pl.col("season") == SEASON) & (pl.col("season_type") == "REG"))
          .group_by(["player_display_name", "recent_team"])
          .agg(
              pl.col("receptions").sum().alias("rec"),
              pl.col("receiving_yards").sum().alias("rec_yds"),
              pl.col("targets").sum().alias("tgt"),
          )
          .filter(pl.col("rec") >= 70)
    )
    pdf = rec.to_pandas()  # <-- polars -> pandas in one call
    pdf["yards_per_rec"] = (pdf["rec_yds"] / pdf["rec"]).round(1)
    pdf["catch_rate"] = (pdf["rec"] / pdf["tgt"] * 100).round(1)
    rec_out = pdf.sort_values("rec_yds", ascending=False).head(10)[
        ["player_display_name", "recent_team", "rec", "rec_yds", "yards_per_rec", "catch_rate"]
    ].reset_index(drop=True)
else:
    rec_out = "player_stats schema changed — receiving columns missing"
rec_out
```




      player_display_name recent_team  rec  rec_yds  yards_per_rec  catch_rate
    0       Ja'Marr Chase         CIN  127   1708.0           13.4        72.6
    1    Justin Jefferson         MIN  103   1533.0           14.9        66.9
    2        Brian Thomas         JAX   87   1282.0           14.7        65.4
    3        Drake London         ATL  100   1271.0           12.7        63.3
    4   Amon-Ra St. Brown         DET  115   1263.0           11.0        81.6
    5         Jerry Jeudy         CLE   90   1229.0           13.7        62.1
    6        Malik Nabers         NYG  109   1204.0           11.0        64.1
    7         CeeDee Lamb         DAL  101   1194.0           11.8        66.4
    8        Brock Bowers          LV  112   1194.0           10.7        73.2
    9       Ladd McConkey         LAC   82   1149.0           14.0        73.2



### Recipe 11 — Who gets open? NextGen separation 🛰️

The receiving statboard exposes a tracking-only metric box scores can't: **average separation** at the catch point. Rank qualified targets (≥80) to find the route-runners defenders can't shadow.


```python
sepboard = safe(
    "NGS receiving statboard",
    lambda: nfl.nfl_ngs_statboard(stat_type="receiving", season=SEASON, season_type="REG"),
)
if sepboard is not None and sepboard.height and "avgSeparation" in sepboard.columns:
    sep = (
        sepboard.filter(pl.col("targets") >= 80)
                .select([c for c in [
                    "player_displayName", "player_position", "avgSeparation",
                    "avgYACAboveExpectation", "catchPercentage", "yards",
                ] if c in sepboard.columns])
                .sort("avgSeparation", descending=True)
                .head(10)
    )
else:
    sep = "NGS receiving statboard unavailable"
sep
```

    ✅ NGS receiving statboard





    shape: (10, 6)
    ┌───────────────────┬─────────────────┬───────────────┬──────────────────┬─────────────────┬───────┐
    │ player_displayNam ┆ player_position ┆ avgSeparation ┆ avgYACAboveExpec ┆ catchPercentage ┆ yards │
    │ e                 ┆ ---             ┆ ---           ┆ tation           ┆ ---             ┆ ---   │
    │ ---               ┆ str             ┆ f64           ┆ ---              ┆ f64             ┆ i64   │
    │ str               ┆                 ┆               ┆ f64              ┆                 ┆       │
    ╞═══════════════════╪═════════════════╪═══════════════╪══════════════════╪═════════════════╪═══════╡
    │ Khalil Shakir     ┆ WR              ┆ 4.253744      ┆ 1.420359         ┆ 76.0            ┆ 821   │
    │ Demario Douglas   ┆ WR              ┆ 4.010062      ┆ 0.298947         ┆ 75.862069       ┆ 621   │
    │ Zay Flowers       ┆ WR              ┆ 3.9155        ┆ 1.636763         ┆ 63.793103       ┆ 1059  │
    │ Xavier Worthy     ┆ WR              ┆ 3.773458      ┆ 0.445979         ┆ 60.204082       ┆ 638   │
    │ Zach Ertz         ┆ TE              ┆ 3.631374      ┆ -0.207435        ┆ 72.527473       ┆ 654   │
    │ George Kittle     ┆ TE              ┆ 3.582076      ┆ 2.110873         ┆ 82.978723       ┆ 1106  │
    │ Sam LaPorta       ┆ TE              ┆ 3.529115      ┆ 1.433347         ┆ 72.289157       ┆ 726   │
    │ Brock Bowers      ┆ TE              ┆ 3.515527      ┆ 0.982977         ┆ 73.202614       ┆ 1194  │
    │ Trey McBride      ┆ TE              ┆ 3.511888      ┆ 0.637703         ┆ 75.510204       ┆ 1146  │
    │ Jonnu Smith       ┆ TE              ┆ 3.49632       ┆ 0.848143         ┆ 79.279279       ┆ 884   │
    └───────────────────┴─────────────────┴───────────────┴──────────────────┴─────────────────┴───────┘



### Recipe 12 — The nail-biters: closest games of the season 😬

[`load_nfl_schedule`](../nfl/reference/additional.md#load_nfl_schedule) carries the final `result` (home margin). Take its absolute value and sort ascending to surface the one-score thrillers — built-in betting lines ride along too.


```python
sched = nfl.load_nfl_schedule([SEASON])
if {"result", "game_type", "home_team", "away_team"}.issubset(sched.columns):
    scored = (
        sched.filter((pl.col("game_type") == "REG") & pl.col("result").is_not_null())
             .with_columns(pl.col("result").abs().alias("margin"))
    )
    want = [
        "week", "away_team", "away_score", "home_team", "home_score",
        "margin", "spread_line", "total_line",
    ]
    nailbiters = (
        scored.select([c for c in want if c in scored.columns])
              .sort("margin")
              .head(10)
    )
else:
    nailbiters = "schedule schema changed — result/team columns missing"
nailbiters
```




    shape: (10, 8)
    ┌──────┬───────────┬────────────┬───────────┬────────────┬────────┬─────────────┬────────────┐
    │ week ┆ away_team ┆ away_score ┆ home_team ┆ home_score ┆ margin ┆ spread_line ┆ total_line │
    │ ---  ┆ ---       ┆ ---        ┆ ---       ┆ ---        ┆ ---    ┆ ---         ┆ ---        │
    │ i32  ┆ str       ┆ i32        ┆ str       ┆ i32        ┆ i32    ┆ f64         ┆ f64        │
    ╞══════╪═══════════╪════════════╪═══════════╪════════════╪════════╪═════════════╪════════════╡
    │ 2    ┆ CIN       ┆ 25         ┆ KC        ┆ 26         ┆ 1      ┆ 6.5         ┆ 47.5       │
    │ 2    ┆ ATL       ┆ 22         ┆ PHI       ┆ 21         ┆ 1      ┆ 5.5         ┆ 46.5       │
    │ 4    ┆ DEN       ┆ 10         ┆ NYJ       ┆ 9          ┆ 1      ┆ 7.5         ┆ 39.5       │
    │ 5    ┆ ARI       ┆ 24         ┆ SF        ┆ 23         ┆ 1      ┆ 7.0         ┆ 48.5       │
    │ 8    ┆ ARI       ┆ 28         ┆ MIA       ┆ 27         ┆ 1      ┆ 4.0         ┆ 46.5       │
    │ 9    ┆ NO        ┆ 22         ┆ CAR       ┆ 23         ┆ 1      ┆ -7.0        ┆ 43.5       │
    │ 10   ┆ CIN       ┆ 34         ┆ BAL       ┆ 35         ┆ 1      ┆ 6.0         ┆ 53.0       │
    │ 10   ┆ PIT       ┆ 28         ┆ WAS       ┆ 27         ┆ 1      ┆ 1.5         ┆ 45.0       │
    │ 11   ┆ GB        ┆ 20         ┆ CHI       ┆ 19         ┆ 1      ┆ -6.0        ┆ 41.0       │
    │ 11   ┆ IND       ┆ 28         ┆ NYJ       ┆ 27         ┆ 1      ┆ 4.0         ┆ 43.0       │
    └──────┴───────────┴────────────┴───────────┴────────────┴────────┴─────────────┴────────────┘



## 🗓️ Season helpers

Handy when you want "the current/most-recent season" instead of hard-coding a
year. `get_current_nfl_season()` / `get_current_nfl_week()` track the live
calendar; `most_recent_nfl_season()` gives the latest season with data.


```python
{
    "current_season": nfl.get_current_nfl_season(),
    "current_week": nfl.get_current_nfl_week(),
    "most_recent_season": nfl.most_recent_nfl_season(),
}

```




    {'current_season': 2025, 'current_week': 22, 'most_recent_season': 2025}



## 🎉 Where to next

- **Premium native API** — the full `nfl_*` endpoint set:
  [`docs/docs/nfl/reference/nfl_api.md`](../nfl/reference/nfl_api.md)
- **NextGen Stats & loaders** — `nfl_ngs_*` and `load_nfl_*`:
  [`docs/docs/nfl/reference/additional.md`](../nfl/reference/additional.md)
  and [`docs/docs/nfl/reference/loaders.md`](../nfl/reference/loaders.md)
- **ESPN secondary path** — every `espn_nfl_*` wrapper:
  [`docs/docs/nfl/reference/site.md`](../nfl/reference/site.md)
- Pass `return_as_pandas=True` for a pandas frame, or `return_parsed=False`
  (native API) for raw JSON.
- R user? The same data lives in [nflfastR](https://www.nflfastr.com) /
  [nflreadr](https://nflreadr.nflverse.com); Python parity is
  [nflreadpy](https://github.com/nflverse/nflreadpy).

Now go build something great — may your EPA be ever positive! 📈🏈
