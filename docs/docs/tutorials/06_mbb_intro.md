---
title: MBB tutorial
sidebar_label: MBB
sidebar_position: 4
---

# 🏀 Men's college basketball with `sportsdataverse-py`

Welcome to **Selection-Sunday-grade** hoops data! 🎉 In a handful of lines of
Python you're about to pull NCAA Division I men's basketball — full schedules,
play-by-play, standings, rosters, statistical leaders and multi-season parquet
archives — and get it all back as tidy **polars** DataFrames ready to model.

`sportsdataverse.mbb` leads with two premium sources:

- 🟥 **ESPN** (`espn_mbb_*`) — the site + core APIs behind ESPN.com: live
  scoreboards, schedules, standings, rankings, box scores, win probability and
  play-by-play.
- 🦊 **FoxSports** (`fox_mbb_*`) — FoxSports' league-leader, standings, roster,
  boxscore and odds feeds.

Plus 📦 **release loaders** (`load_mbb_*`) that hand you whole seasons of
play-by-play, box scores, shots and schedules from the data repo in one call.

R user? The men's-basketball companion is
[hoopR](https://hoopR.sportsdataverse.org) (NBA + NCAA). Let's tip off! 🏀

## 🧰 The toolbox

Every accessor returns a tidy **polars** `DataFrame` by default — pass
`return_as_pandas=True` for pandas. The richest live surfaces are ESPN and Fox;
the `load_*` loaders read pre-built parquet from the data release (rock-solid,
no live API). Click any name for the full reference.

| Function | What it gives you | Source |
|---|---|---|
| [`espn_mbb_teams`](../mbb/reference/additional.md#espn_mbb_teams) | Every D-I team (grab `team_id`s) | 🟥 ESPN ⭐ |
| [`espn_mbb_schedule`](../mbb/reference/additional.md#espn_mbb_schedule) | Games + results for a date / window | 🟥 ESPN ⭐ |
| [`espn_mbb_scoreboard`](../mbb/reference/site.md#espn_mbb_scoreboard) | Rich scoreboard for a date (status, lines, odds) | 🟥 ESPN ⭐ |
| [`espn_mbb_standings`](../mbb/reference/site.md#espn_mbb_standings) | Conference standings, one row per team | 🟥 ESPN ⭐ |
| [`espn_mbb_rankings`](../mbb/reference/site.md#espn_mbb_rankings) | AP / Coaches poll (in-season) | 🟥 ESPN ⭐ |
| [`espn_mbb_summary`](../mbb/reference/site.md#espn_mbb_summary) | Full game summary: box, plays, win prob | 🟥 ESPN ⭐ |
| [`espn_mbb_team_roster`](../mbb/reference/site.md#espn_mbb_team_roster) | A team's roster | 🟥 ESPN ⭐ |
| [`espn_mbb_pbp`](../mbb/reference/additional.md#espn_mbb_pbp) | Event-level play-by-play for a game | 🟥 ESPN ⭐ |
| [`espn_mbb_game_rosters`](../mbb/reference/additional.md#espn_mbb_game_rosters) | Who dressed + started for one game | 🟥 ESPN ⭐ |
| [`espn_mbb_player_stats`](../mbb/reference/additional.md#espn_mbb_player_stats) | A player's season stat line | 🟥 ESPN ⭐ |
| [`fox_mbb_league_leaders`](../mbb/reference/additional.md#fox_mbb_league_leaders) | Stat leaders (scoring, rebounds, …) | 🦊 Fox ⭐ |
| [`fox_mbb_standings`](../mbb/reference/additional.md#fox_mbb_standings) | Fox conference standings for a team | 🦊 Fox ⭐ |
| [`fox_mbb_team_roster`](../mbb/reference/additional.md#fox_mbb_team_roster) | Fox roster for a team | 🦊 Fox |
| [`espn_mbb_team_schedule`](../mbb/reference/additional.md#espn_mbb_team_schedule) | One team's full season schedule | 🟥 ESPN ⭐ |
| [`espn_mbb_conferences`](../mbb/reference/additional.md#espn_mbb_conferences) | Conference / group catalog | 🟥 ESPN ⭐ |
| [`load_mbb_schedule`](../mbb/reference/loaders.md#load_mbb_schedule) | Whole-season schedule parquet | 📦 loader |
| [`load_mbb_player_boxscore`](../mbb/reference/loaders.md#load_mbb_player_boxscore) | Season player box scores | 📦 loader |
| [`load_mbb_team_boxscore`](../mbb/reference/loaders.md#load_mbb_team_boxscore) | Season team box scores | 📦 loader |
| [`load_mbb_pbp`](../mbb/reference/loaders.md#load_mbb_pbp) | Season play-by-play parquet | 📦 loader |
| [`most_recent_mbb_season`](../mbb/reference/additional.md#most_recent_mbb_season) | Current season-year helper | 🛠️ helper |

⭐ = premium live source.

## 🔌 Setup

```sh
pip install sportsdataverse
```

No API key needed — ESPN, Fox and the parquet loaders are all open. 😊


```python
import polars as pl
import sportsdataverse as sdv

pl.Config.set_tbl_rows(10)
print("most recent MBB season:", sdv.mbb.most_recent_mbb_season())
```

    most recent MBB season: 2026


ESPN's *live* endpoints (scoreboard, rankings, standings, a single game's
play-by-play) are seasonal — in the offseason a poll or scoreboard can come
back empty. So we use a tiny `safe()` helper: you get the frame when the feed
is up, and a friendly one-liner when it isn't — never a scary traceback. 🛟
The `load_*` parquet loaders are stable year-round, so we call those directly.


```python
def safe(label, thunk):
    """Run a live call defensively; return its result or None with a note."""
    try:
        out = thunk()
        ok = out is not None and (not hasattr(out, "height") or out.height)
        print(f"{'✅' if ok else 'ℹ️ '} {label}{'' if ok else ' — no rows right now'}")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None
```

## 🏟️ Every team in Division I

Start with [`espn_mbb_teams`](../mbb/reference/additional.md#espn_mbb_teams) —
one row per program, with the `team_id` you'll pass into roster, schedule and
summary calls. This is a plain catalog fetch, so it's reliable year-round.


```python
teams = sdv.mbb.espn_mbb_teams()
print("teams:", teams.shape)
teams.select(["team_id", "team_location", "team_name", "team_abbreviation", "team_is_active"]).head()
```

    teams: (362, 14)





    shape: (5, 5)
    ┌─────────┬───────────────────┬──────────────┬───────────────────┬────────────────┐
    │ team_id ┆ team_location     ┆ team_name    ┆ team_abbreviation ┆ team_is_active │
    │ ---     ┆ ---               ┆ ---          ┆ ---               ┆ ---            │
    │ str     ┆ str               ┆ str          ┆ str               ┆ bool           │
    ╞═════════╪═══════════════════╪══════════════╪═══════════════════╪════════════════╡
    │ 2000    ┆ Abilene Christian ┆ Wildcats     ┆ ACU               ┆ true           │
    │ 2005    ┆ Air Force         ┆ Falcons      ┆ AF                ┆ true           │
    │ 2006    ┆ Akron             ┆ Zips         ┆ AKR               ┆ true           │
    │ 2010    ┆ Alabama A&M       ┆ Bulldogs     ┆ AAMU              ┆ true           │
    │ 333     ┆ Alabama           ┆ Crimson Tide ┆ ALA               ┆ true           │
    └─────────┴───────────────────┴──────────────┴───────────────────┴────────────────┘



## 📅 Schedule & scores for a date window

[`espn_mbb_schedule`](../mbb/reference/additional.md#espn_mbb_schedule) takes a
single `dates=YYYYMMDD` or a `'YYYYMMDD-YYYYMMDD'` window and returns one row
per game with final scores. Here's championship day of the 2024 tournament.


```python
sched = safe(
    "schedule 2024-04-08",
    lambda: sdv.mbb.espn_mbb_schedule(dates=20240408),
)
(sched.select(["id", "home_display_name", "away_display_name", "home_score", "away_score"]).head()
 if sched is not None and sched.height else "schedule unavailable")
```

    ✅ schedule 2024-04-08





    shape: (1, 5)
    ┌───────────┬───────────────────┬─────────────────────┬────────────┬────────────┐
    │ id        ┆ home_display_name ┆ away_display_name   ┆ home_score ┆ away_score │
    │ ---       ┆ ---               ┆ ---                 ┆ ---        ┆ ---        │
    │ str       ┆ str               ┆ str                 ┆ str        ┆ str        │
    ╞═══════════╪═══════════════════╪═════════════════════╪════════════╪════════════╡
    │ 401638645 ┆ UConn Huskies     ┆ Purdue Boilermakers ┆ 75         ┆ 60         │
    └───────────┴───────────────────┴─────────────────────┴────────────┴────────────┘



## 📊 The rich scoreboard

[`espn_mbb_scoreboard`](../mbb/reference/site.md#espn_mbb_scoreboard) is the
deluxe version: for a given date it returns status, broadcast, betting lines and
team line scores — 50 columns wide. Defaults to polars; we peek at a tidy slice.


```python
sb = safe(
    "scoreboard 2024-04-08",
    lambda: sdv.mbb.espn_mbb_scoreboard(dates=20240408, return_as_pandas=False),
)
if sb is not None and getattr(sb, "height", 0):
    keep = ["game_id", "short_name", "status_type_description",
            "home_team_short_display_name", "away_team_short_display_name"]
    out = sb.select([c for c in keep if c in sb.columns]).head()
else:
    out = "scoreboard empty right now (offseason)"
out
```

    ✅ scoreboard 2024-04-08





    shape: (1, 3)
    ┌───────────┬─────────────┬─────────────────────────┐
    │ game_id   ┆ short_name  ┆ status_type_description │
    │ ---       ┆ ---         ┆ ---                     │
    │ str       ┆ str         ┆ str                     │
    ╞═══════════╪═════════════╪═════════════════════════╡
    │ 401638645 ┆ PUR VS CONN ┆ Final                   │
    └───────────┴─────────────┴─────────────────────────┘



## 🏆 Conference standings

[`espn_mbb_standings`](../mbb/reference/site.md#espn_mbb_standings) returns one
row per team for a season with wins, losses, win pct, point differential and
conference grouping. Great for a quick power look across the league.


```python
standings = safe(
    "standings 2024",
    lambda: sdv.mbb.espn_mbb_standings(season=2024, return_as_pandas=False),
)
if standings is not None and getattr(standings, "height", 0):
    keep = ["team_display_name", "group_name", "wins", "losses",
            "win_percent", "point_differential"]
    out = (standings.select([c for c in keep if c in standings.columns])
           .sort("win_percent", descending=True).head(10))
else:
    out = "standings unavailable"
out
```

    ✅ standings 2024





    shape: (10, 6)
    ┌───────────────────────┬───────────────────────┬──────┬────────┬─────────────┬────────────────────┐
    │ team_display_name     ┆ group_name            ┆ wins ┆ losses ┆ win_percent ┆ point_differential │
    │ ---                   ┆ ---                   ┆ ---  ┆ ---    ┆ ---         ┆ ---                │
    │ str                   ┆ str                   ┆ f64  ┆ f64    ┆ f64         ┆ f64                │
    ╞═══════════════════════╪═══════════════════════╪══════╪════════╪═════════════╪════════════════════╡
    │ McNeese Cowboys       ┆ Southland Conference  ┆ 17.0 ┆ 1.0    ┆ 0.9444444   ┆ 308.0              │
    │ Vermont Catamounts    ┆ America East          ┆ 15.0 ┆ 1.0    ┆ 0.9375      ┆ 179.0              │
    │                       ┆ Conference            ┆      ┆        ┆             ┆                    │
    │ Saint Mary's Gaels    ┆ West Coast Conference ┆ 15.0 ┆ 1.0    ┆ 0.9375      ┆ 312.0              │
    │ UConn Huskies         ┆ Big East Conference   ┆ 18.0 ┆ 2.0    ┆ 0.9         ┆ 277.0              │
    │ South Florida Bulls   ┆ American Conference   ┆ 16.0 ┆ 2.0    ┆ 0.8888889   ┆ 124.0              │
    │ Colgate Raiders       ┆ Patriot League        ┆ 16.0 ┆ 2.0    ┆ 0.8888889   ┆ 217.0              │
    │ App State             ┆ Sun Belt Conference   ┆ 16.0 ┆ 2.0    ┆ 0.8888889   ┆ 198.0              │
    │ Mountaineers          ┆                       ┆      ┆        ┆             ┆                    │
    │ Gonzaga Bulldogs      ┆ West Coast Conference ┆ 14.0 ┆ 2.0    ┆ 0.875       ┆ 305.0              │
    │ Princeton Tigers      ┆ Ivy League            ┆ 12.0 ┆ 2.0    ┆ 0.857143    ┆ 136.0              │
    │ North Carolina Tar    ┆ Atlantic Coast        ┆ 17.0 ┆ 3.0    ┆ 0.85        ┆ 210.0              │
    │ Heels                 ┆ Conference            ┆      ┆        ┆             ┆                    │
    └───────────────────────┴───────────────────────┴──────┴────────┴─────────────┴────────────────────┘



## 🍳 Cookbook: common MBB tasks

Now the fun part — real tasks you'll reach for constantly, each built on a
premium ESPN or Fox wrapper. Every recipe is guarded so a transient or
offseason hiccup prints a note instead of breaking the page.

### Recipe 1 — National scoring leaders 🥇 (FoxSports)

[`fox_mbb_league_leaders`](../mbb/reference/additional.md#fox_mbb_league_leaders)
serves the leaderboard direct from FoxSports — pick a `category` (`scoring`,
`rebounds`, `assists`, …) and `who` (`player` or `team`). No IDs needed.


```python
leaders = safe(
    "fox scoring leaders",
    lambda: sdv.mbb.fox_mbb_league_leaders(category="scoring", who="player"),
)
if leaders is not None and getattr(leaders, "height", 0):
    keep = ["players", "gp", "mpg", "ppg", "pts"]
    out = leaders.select([c for c in keep if c in leaders.columns]).head(10)
else:
    out = "Fox leaders unavailable right now"
out
```

    ✅ fox scoring leaders





    shape: (10, 5)
    ┌─────────┬─────┬──────┬──────┬──────┐
    │ players ┆ gp  ┆ mpg  ┆ ppg  ┆ pts  │
    │ ---     ┆ --- ┆ ---  ┆ ---  ┆ ---  │
    │ str     ┆ str ┆ str  ┆ str  ┆ str  │
    ╞═════════╪═════╪══════╪══════╪══════╡
    │ 1       ┆ 37  ┆ null ┆ null ┆ null │
    │ 2       ┆ 37  ┆ null ┆ null ┆ null │
    │ 3       ┆ 37  ┆ null ┆ null ┆ null │
    │ 4       ┆ 36  ┆ null ┆ null ┆ null │
    │ 5       ┆ 36  ┆ null ┆ null ┆ null │
    │ 6       ┆ 35  ┆ null ┆ null ┆ null │
    │ 7       ┆ 35  ┆ null ┆ null ┆ null │
    │ 8       ┆ 35  ┆ null ┆ null ┆ null │
    │ 9       ┆ 35  ┆ null ┆ null ┆ null │
    │ 10      ┆ 35  ┆ null ┆ null ┆ null │
    └─────────┴─────┴──────┴──────┴──────┘



### Recipe 2 — Look up a team's roster 👥 (ESPN)

Grab a `team_id` from `espn_mbb_teams`, then
[`espn_mbb_team_roster`](../mbb/reference/site.md#espn_mbb_team_roster) returns
the current roster. Here we resolve UConn (the 2024 champs) by abbreviation so
the recipe is self-contained.


```python
row = teams.filter(pl.col("team_abbreviation") == "CONN")
tid = int(row["team_id"][0]) if row.height else 41  # 41 = UConn fallback
roster = safe(
    f"roster team_id={tid}",
    lambda: sdv.mbb.espn_mbb_team_roster(team_id=tid, return_as_pandas=False),
)
if roster is not None and getattr(roster, "height", 0):
    keep = ["full_name", "jersey", "display_height", "display_weight"]
    out = roster.select([c for c in keep if c in roster.columns]).head(12)
else:
    out = "roster unavailable right now"
out
```

    ✅ roster team_id=41





    shape: (12, 4)
    ┌──────────────────┬────────┬────────────────┬────────────────┐
    │ full_name        ┆ jersey ┆ display_height ┆ display_weight │
    │ ---              ┆ ---    ┆ ---            ┆ ---            │
    │ str              ┆ str    ┆ str            ┆ str            │
    ╞══════════════════╪════════╪════════════════╪════════════════╡
    │ Solo Ball        ┆ 1      ┆ 6' 4"          ┆ 200 lbs        │
    │ Silas Demary Jr. ┆ 2      ┆ 6' 4"          ┆ 195 lbs        │
    │ Rrezon Elezaj    ┆ 10     ┆ 7' 1"          ┆ 225 lbs        │
    │ Jacob Furphy     ┆ 7      ┆ 6' 6"          ┆ 205 lbs        │
    │ Dwayne Koroma    ┆ 4      ┆ 6' 8"          ┆ 212 lbs        │
    │ …                ┆ …      ┆ …              ┆ …              │
    │ Uroš Paunovic    ┆ 77     ┆ 6' 3"          ┆ 190 lbs        │
    │ Eric Reibe       ┆ 12     ┆ 7' 1"          ┆ 260 lbs        │
    │ Jacob Ross       ┆ 13     ┆ 6' 5"          ┆ 195 lbs        │
    │ Jayden Ross      ┆ 23     ┆ 6' 7"          ┆ 205 lbs        │
    │ Malachi Smith    ┆ 0      ┆ 6' 1"          ┆ 180 lbs        │
    └──────────────────┴────────┴────────────────┴────────────────┘



### Recipe 3 — Season scoring leaderboard from parquet 📦

The `load_*` loaders pull whole seasons from the data release — perfect for
analysis that shouldn't depend on a live endpoint.
[`load_mbb_player_boxscore`](../mbb/reference/loaders.md#load_mbb_player_boxscore)
gives every player-game; we aggregate to a per-player points-per-game board.


```python
pbox = sdv.mbb.load_mbb_player_boxscore(seasons=[2024])
print("player box rows:", pbox.shape)
(pbox
    .filter(pl.col("points").is_not_null())
    .group_by(["athlete_display_name", "team_short_display_name"])
    .agg(
        pl.len().alias("g"),
        pl.col("points").cast(pl.Float64, strict=False).mean().round(1).alias("ppg"),
    )
    .filter(pl.col("g") >= 20)
    .sort("ppg", descending=True)
    .head(10))
```

    player box rows: (198586, 55)





    shape: (10, 4)
    ┌──────────────────────┬─────────────────────────┬─────┬──────┐
    │ athlete_display_name ┆ team_short_display_name ┆ g   ┆ ppg  │
    │ ---                  ┆ ---                     ┆ --- ┆ ---  │
    │ str                  ┆ str                     ┆ u32 ┆ f64  │
    ╞══════════════════════╪═════════════════════════╪═════╪══════╡
    │ Zach Edey            ┆ Purdue                  ┆ 39  ┆ 25.2 │
    │ Tommy Bruner         ┆ Denver                  ┆ 34  ┆ 24.0 │
    │ Terrence Shannon Jr. ┆ Illinois                ┆ 32  ┆ 23.0 │
    │ Tyler Thomas         ┆ Hofstra                 ┆ 33  ┆ 22.5 │
    │ Xavier Johnson       ┆ S Illinois              ┆ 32  ┆ 22.2 │
    │ David Jones          ┆ Memphis                 ┆ 32  ┆ 21.8 │
    │ Tyson Acuff          ┆ E Michigan              ┆ 27  ┆ 21.7 │
    │ Dalton Knecht        ┆ Tennessee               ┆ 36  ┆ 21.7 │
    │ Jordan Sears         ┆ UT Martin               ┆ 32  ┆ 21.6 │
    │ Tucker DeVries       ┆ Drake                   ┆ 34  ┆ 21.6 │
    └──────────────────────┴─────────────────────────┴─────┴──────┘



### Recipe 4 — Play-by-play slice for one game 🎬 (ESPN)

[`espn_mbb_pbp`](../mbb/reference/additional.md#espn_mbb_pbp) returns a dict;
its `plays` list is event-level. We frame it and pull just the scoring plays of
the 2024 national championship (UConn vs. Purdue, `game_id=401638636`).


```python
pbp = safe("pbp 401638636", lambda: sdv.mbb.espn_mbb_pbp(game_id=401638636))
if isinstance(pbp, dict) and pbp.get("plays"):
    plays = pl.DataFrame(pbp["plays"], infer_schema_length=None)
    keep = ["period.number", "clock.displayValue", "text", "scoringPlay",
            "homeScore", "awayScore"]
    out = (plays.select([c for c in keep if c in plays.columns])
           .filter(pl.col("scoringPlay") == True)  # noqa: E712
           .head(10))
else:
    out = "play-by-play unavailable right now"
out
```

    ✅ pbp 401638636





    shape: (10, 6)
    ┌───────────────┬────────────────────┬───────────────────────┬─────────────┬───────────┬───────────┐
    │ period.number ┆ clock.displayValue ┆ text                  ┆ scoringPlay ┆ homeScore ┆ awayScore │
    │ ---           ┆ ---                ┆ ---                   ┆ ---         ┆ ---       ┆ ---       │
    │ i64           ┆ str                ┆ str                   ┆ bool        ┆ i64       ┆ i64       │
    ╞═══════════════╪════════════════════╪═══════════════════════╪═════════════╪═══════════╪═══════════╡
    │ 1             ┆ 19:27              ┆ Ryan Kalkbrenner made ┆ true        ┆ 0         ┆ 2         │
    │               ┆                    ┆ Layup. A…             ┆             ┆           ┆           │
    │ 1             ┆ 18:24              ┆ Dalton Knecht made    ┆ true        ┆ 2         ┆ 2         │
    │               ┆                    ┆ Jumper.               ┆             ┆           ┆           │
    │ 1             ┆ 17:04              ┆ Mason Miller made     ┆ true        ┆ 2         ┆ 5         │
    │               ┆                    ┆ Three Point …         ┆             ┆           ┆           │
    │ 1             ┆ 16:48              ┆ Dalton Knecht made    ┆ true        ┆ 3         ┆ 5         │
    │               ┆                    ┆ Free Throw.           ┆             ┆           ┆           │
    │ 1             ┆ 16:34              ┆ Baylor Scheierman     ┆ true        ┆ 3         ┆ 7         │
    │               ┆                    ┆ made Jumper.          ┆             ┆           ┆           │
    │ 1             ┆ 15:34              ┆ Josiah-Jordan James   ┆ true        ┆ 6         ┆ 7         │
    │               ┆                    ┆ made Three…           ┆             ┆           ┆           │
    │ 1             ┆ 14:45              ┆ Josiah-Jordan James   ┆ true        ┆ 9         ┆ 7         │
    │               ┆                    ┆ made Three…           ┆             ┆           ┆           │
    │ 1             ┆ 14:19              ┆ Baylor Scheierman     ┆ true        ┆ 9         ┆ 9         │
    │               ┆                    ┆ made Jumper.          ┆             ┆           ┆           │
    │ 1             ┆ 14:07              ┆ Jordan Gainey made    ┆ true        ┆ 11        ┆ 9         │
    │               ┆                    ┆ Jumper. Ass…          ┆             ┆           ┆           │
    │ 1             ┆ 13:39              ┆ Baylor Scheierman     ┆ true        ┆ 11        ┆ 12        │
    │               ┆                    ┆ made Three P…         ┆             ┆           ┆           │
    └───────────────┴────────────────────┴───────────────────────┴─────────────┴───────────┴───────────┘



### Recipe 5 — Best net scoring margin 📊 (parquet)

[`load_mbb_team_boxscore`](../mbb/reference/loaders.md#load_mbb_team_boxscore) gives one row per team-game with the opponent's score attached, so a single group-by ranks every program by points scored minus points allowed — the cleanest one-number power proxy. Pure parquet, no live endpoint.


```python
tbox = sdv.mbb.load_mbb_team_boxscore(seasons=[2024])
print("team box rows:", tbox.shape)
(tbox
    .group_by("team_display_name")
    .agg(
        pl.len().alias("g"),
        pl.col("team_score").cast(pl.Float64, strict=False).mean().round(1).alias("ppg"),
        pl.col("opponent_team_score").cast(pl.Float64, strict=False).mean().round(1).alias("opp_ppg"),
    )
    .with_columns((pl.col("ppg") - pl.col("opp_ppg")).round(1).alias("net_margin"))
    .filter(pl.col("g") >= 25)
    .sort("net_margin", descending=True)
    .head(10))
```

    team box rows: (12480, 57)





    shape: (10, 5)
    ┌─────────────────────┬─────┬──────┬─────────┬────────────┐
    │ team_display_name   ┆ g   ┆ ppg  ┆ opp_ppg ┆ net_margin │
    │ ---                 ┆ --- ┆ ---  ┆ ---     ┆ ---        │
    │ str                 ┆ u32 ┆ f64  ┆ f64     ┆ f64        │
    ╞═════════════════════╪═════╪══════╪═════════╪════════════╡
    │ UConn Huskies       ┆ 40  ┆ 81.4 ┆ 63.4    ┆ 18.0       │
    │ McNeese Cowboys     ┆ 34  ┆ 80.0 ┆ 62.2    ┆ 17.8       │
    │ Houston Cougars     ┆ 37  ┆ 73.5 ┆ 57.6    ┆ 15.9       │
    │ Gonzaga Bulldogs    ┆ 35  ┆ 84.5 ┆ 69.1    ┆ 15.4       │
    │ Arizona Wildcats    ┆ 36  ┆ 87.1 ┆ 72.1    ┆ 15.0       │
    │ Auburn Tigers       ┆ 35  ┆ 83.1 ┆ 68.3    ┆ 14.8       │
    │ Saint Mary's Gaels  ┆ 34  ┆ 74.0 ┆ 59.2    ┆ 14.8       │
    │ Iowa State Cyclones ┆ 37  ┆ 75.3 ┆ 61.5    ┆ 13.8       │
    │ James Madison Dukes ┆ 36  ┆ 83.2 ┆ 69.6    ┆ 13.6       │
    │ Purdue Boilermakers ┆ 39  ┆ 82.3 ┆ 69.0    ┆ 13.3       │
    └─────────────────────┴─────┴──────┴─────────┴────────────┘



### Recipe 6 — Best 3-point shooting teams 🎯 (parquet)

Same team-box parquet, different question: sum makes and attempts across the season, then divide. A `min attempts` filter keeps small-sample flukes off the board so the leaders are real volume shooters.


```python
(tbox
    .group_by("team_display_name")
    .agg(
        pl.col("three_point_field_goals_made")
          .cast(pl.Float64, strict=False).sum().alias("tpm"),
        pl.col("three_point_field_goals_attempted")
          .cast(pl.Float64, strict=False).sum().alias("tpa"),
    )
    .with_columns((pl.col("tpm") / pl.col("tpa") * 100).round(1).alias("three_pct"))
    .filter(pl.col("tpa") >= 500)
    .sort("three_pct", descending=True)
    .select(["team_display_name", "tpm", "tpa", "three_pct"])
    .head(10))
```




    shape: (10, 4)
    ┌─────────────────────────┬───────┬───────┬───────────┐
    │ team_display_name       ┆ tpm   ┆ tpa   ┆ three_pct │
    │ ---                     ┆ ---   ┆ ---   ┆ ---       │
    │ str                     ┆ f64   ┆ f64   ┆ f64       │
    ╞═════════════════════════╪═══════╪═══════╪═══════════╡
    │ Kentucky Wildcats       ┆ 327.0 ┆ 800.0 ┆ 40.9      │
    │ Purdue Boilermakers     ┆ 318.0 ┆ 788.0 ┆ 40.4      │
    │ Dayton Flyers           ┆ 310.0 ┆ 777.0 ┆ 39.9      │
    │ UNC Greensboro Spartans ┆ 322.0 ┆ 812.0 ┆ 39.7      │
    │ Samford Bulldogs        ┆ 351.0 ┆ 889.0 ┆ 39.5      │
    │ Colorado Buffaloes      ┆ 254.0 ┆ 649.0 ┆ 39.1      │
    │ Northwestern Wildcats   ┆ 278.0 ┆ 713.0 ┆ 39.0      │
    │ Baylor Bears            ┆ 301.0 ┆ 773.0 ┆ 38.9      │
    │ McNeese Cowboys         ┆ 257.0 ┆ 671.0 ┆ 38.3      │
    │ Wright State Raiders    ┆ 218.0 ┆ 569.0 ┆ 38.3      │
    └─────────────────────────┴───────┴───────┴───────────┘



### Recipe 7 — Most efficient scorers ⚡ (true shooting %)

Points-per-game rewards volume; **true shooting %** rewards *efficiency* — it folds threes and free throws into one rate via `TS% = PTS / (2 · (FGA + 0.44·FTA))`. We compute it straight from [`load_mbb_player_boxscore`](../mbb/reference/loaders.md#load_mbb_player_boxscore), keeping only high-usage scorers.


```python
pbox = sdv.mbb.load_mbb_player_boxscore(seasons=[2024])
(pbox
    .filter(pl.col("points").is_not_null())
    .group_by(["athlete_display_name", "team_abbreviation"])
    .agg(
        pl.len().alias("g"),
        pl.col("points").cast(pl.Float64, strict=False).sum().alias("pts"),
        pl.col("field_goals_attempted").cast(pl.Float64, strict=False).sum().alias("fga"),
        pl.col("free_throws_attempted").cast(pl.Float64, strict=False).sum().alias("fta"),
    )
    .with_columns(
        (pl.col("pts") / (2 * (pl.col("fga") + 0.44 * pl.col("fta"))) * 100)
        .round(1).alias("ts_pct"))
    .filter((pl.col("g") >= 25) & (pl.col("pts") >= 400))
    .sort("ts_pct", descending=True)
    .select(["athlete_display_name", "team_abbreviation", "g", "pts", "ts_pct"])
    .head(10))
```




    shape: (10, 5)
    ┌──────────────────────┬───────────────────┬─────┬───────┬────────┐
    │ athlete_display_name ┆ team_abbreviation ┆ g   ┆ pts   ┆ ts_pct │
    │ ---                  ┆ ---               ┆ --- ┆ ---   ┆ ---    │
    │ str                  ┆ str               ┆ u32 ┆ f64   ┆ f64    │
    ╞══════════════════════╪═══════════════════╪═════╪═══════╪════════╡
    │ Jayson Kent          ┆ INST              ┆ 38  ┆ 514.0 ┆ 73.3   │
    │ Aubin Gateretse      ┆ STET              ┆ 35  ┆ 407.0 ┆ 71.2   │
    │ Lynn Kidd            ┆ VT                ┆ 33  ┆ 434.0 ┆ 70.7   │
    │ Reed Sheppard        ┆ UK                ┆ 33  ┆ 411.0 ┆ 70.5   │
    │ Vladislav Goldin     ┆ FAU               ┆ 34  ┆ 534.0 ┆ 69.1   │
    │ Ryan Kalkbrenner     ┆ CREI              ┆ 35  ┆ 604.0 ┆ 68.9   │
    │ Cedric Coward        ┆ EWU               ┆ 32  ┆ 494.0 ┆ 68.3   │
    │ Jaylin Williams      ┆ AUB               ┆ 34  ┆ 422.0 ┆ 68.2   │
    │ Chaz Lanier          ┆ UNF               ┆ 32  ┆ 629.0 ┆ 67.3   │
    │ Zach Edey            ┆ PUR               ┆ 39  ┆ 983.0 ┆ 67.3   │
    └──────────────────────┴───────────────────┴─────┴───────┴────────┘



### Recipe 8 — One conference's power board 🏟️ (ESPN, join)

[`espn_mbb_conferences`](../mbb/reference/additional.md#espn_mbb_conferences) is the group catalog; [`espn_mbb_standings`](../mbb/reference/site.md#espn_mbb_standings) carries a `group_name` per team. Filter standings to a single league — here the Big 12 — to get a clean intra-conference pecking order.


```python
confs = safe("conferences", lambda: sdv.mbb.espn_mbb_conferences())
if confs is not None and getattr(confs, "height", 0):
    print("some conferences:",
          confs.filter(pl.col("is_conference"))["name"].to_list()[:8])
st = safe("standings 2024", lambda: sdv.mbb.espn_mbb_standings(season=2024))
if st is not None and getattr(st, "height", 0) and "group_name" in st.columns:
    keep = ["team_display_name", "wins", "losses", "win_percent", "point_differential"]
    out = (st.filter(pl.col("group_name").str.contains("Big 12"))
             .select([c for c in keep if c in st.columns])
             .sort("win_percent", descending=True)
             .head(12))
    out = out if out.height else st.select(
        [c for c in keep if c in st.columns]).sort(
        "win_percent", descending=True).head(12)
else:
    out = "standings unavailable right now"
out
```

    ✅ conferences
    some conferences: ['NCAA Division I', 'Non-NCAA Division I']


    ✅ standings 2024





    shape: (12, 5)
    ┌────────────────────────┬──────┬────────┬─────────────┬────────────────────┐
    │ team_display_name      ┆ wins ┆ losses ┆ win_percent ┆ point_differential │
    │ ---                    ┆ ---  ┆ ---    ┆ ---         ┆ ---                │
    │ str                    ┆ f64  ┆ f64    ┆ f64         ┆ f64                │
    ╞════════════════════════╪══════╪════════╪═════════════╪════════════════════╡
    │ Houston Cougars        ┆ 15.0 ┆ 3.0    ┆ 0.8333333   ┆ 191.0              │
    │ Iowa State Cyclones    ┆ 13.0 ┆ 5.0    ┆ 0.7222222   ┆ 71.0               │
    │ Baylor Bears           ┆ 11.0 ┆ 7.0    ┆ 0.6111111   ┆ 52.0               │
    │ Texas Tech Red Raiders ┆ 11.0 ┆ 7.0    ┆ 0.6111111   ┆ 39.0               │
    │ BYU Cougars            ┆ 10.0 ┆ 8.0    ┆ 0.5555556   ┆ 19.0               │
    │ …                      ┆ …    ┆ …      ┆ …           ┆ …                  │
    │ TCU Horned Frogs       ┆ 9.0  ┆ 9.0    ┆ 0.5         ┆ 21.0               │
    │ Oklahoma Sooners       ┆ 8.0  ┆ 10.0   ┆ 0.444444    ┆ -33.0              │
    │ Kansas State Wildcats  ┆ 8.0  ┆ 10.0   ┆ 0.444444    ┆ -23.0              │
    │ Cincinnati Bearcats    ┆ 7.0  ┆ 11.0   ┆ 0.3888889   ┆ 5.0                │
    │ UCF Knights            ┆ 7.0  ┆ 11.0   ┆ 0.3888889   ┆ -44.0              │
    └────────────────────────┴──────┴────────┴─────────────┴────────────────────┘



### Recipe 9 — A team's full season schedule 🗓️ (ESPN)

[`espn_mbb_team_schedule`](../mbb/reference/additional.md#espn_mbb_team_schedule) returns every game on one team's slate for a season — matchup name, week and season type — perfect for building an opponent list. We use UConn's 2024 championship run.


```python
tid_sched = int(row["team_id"][0]) if row.height else 41  # UConn fallback
tsched = safe(
    f"team schedule {tid_sched}",
    lambda: sdv.mbb.espn_mbb_team_schedule(team_id=tid_sched, season=2024),
)
if tsched is not None and getattr(tsched, "height", 0):
    keep = ["id", "short_name", "season_type_name", "week_text"]
    out = tsched.select([c for c in keep if c in tsched.columns]).head(12)
else:
    out = "team schedule unavailable right now"
out
```

    ✅ team schedule 41





    shape: (12, 4)
    ┌───────────┬──────────────┬──────────────────┬───────────┐
    │ id        ┆ short_name   ┆ season_type_name ┆ week_text │
    │ ---       ┆ ---          ┆ ---              ┆ ---       │
    │ str       ┆ str          ┆ str              ┆ str       │
    ╞═══════════╪══════════════╪══════════════════╪═══════════╡
    │ 401584359 ┆ NAU @ CONN   ┆ Regular Season   ┆ Week 1    │
    │ 401589296 ┆ STO @ CONN   ┆ Regular Season   ┆ Week 1    │
    │ 401591369 ┆ MVSU @ CONN  ┆ Regular Season   ┆ Week 2    │
    │ 401591374 ┆ CONN VS IU   ┆ Regular Season   ┆ Week 2    │
    │ 401601491 ┆ CONN VS TEX  ┆ Regular Season   ┆ Week 3    │
    │ …         ┆ …            ┆ …                ┆ …         │
    │ 401574563 ┆ CONN @ KU    ┆ Regular Season   ┆ Week 4    │
    │ 401580309 ┆ UNC VS CONN  ┆ Regular Season   ┆ Week 5    │
    │ 401591372 ┆ UAPB @ CONN  ┆ Regular Season   ┆ Week 5    │
    │ 401591373 ┆ CONN VS GONZ ┆ Regular Season   ┆ Week 6    │
    │ 401599441 ┆ CONN @ HALL  ┆ Regular Season   ┆ Week 7    │
    └───────────┴──────────────┴──────────────────┴───────────┘



### Recipe 10 — Top rebounding teams 🧲 (FoxSports)

[`fox_mbb_league_leaders`](../mbb/reference/additional.md#fox_mbb_league_leaders) isn't just a player board — flip `who="team"` and pick `category="rebounds"` to rank programs on the glass straight from FoxSports. No IDs needed.


```python
team_reb = safe(
    "fox team rebounds",
    lambda: sdv.mbb.fox_mbb_league_leaders(category="rebounds", who="team"),
)
if team_reb is not None and getattr(team_reb, "height", 0):
    keep = ["teams", "gp", "w", "l", "ppg", "ppg_diff"]
    out = team_reb.select([c for c in keep if c in team_reb.columns]).head(10)
else:
    out = "Fox team leaders unavailable right now"
out
```

    ✅ fox team rebounds





    shape: (10, 6)
    ┌───────┬─────┬─────┬─────┬──────┬──────────┐
    │ teams ┆ gp  ┆ w   ┆ l   ┆ ppg  ┆ ppg_diff │
    │ ---   ┆ --- ┆ --- ┆ --- ┆ ---  ┆ ---      │
    │ str   ┆ str ┆ str ┆ str ┆ str  ┆ str      │
    ╞═══════╪═════╪═════╪═════╪══════╪══════════╡
    │ 1     ┆ 37  ┆ 21  ┆ 16  ┆ null ┆ null     │
    │ 2     ┆ 35  ┆ 21  ┆ 15  ┆ null ┆ null     │
    │ 3     ┆ 35  ┆ 18  ┆ 17  ┆ null ┆ null     │
    │ 4     ┆ 35  ┆ 23  ┆ 13  ┆ null ┆ null     │
    │ 5     ┆ 35  ┆ 15  ┆ 20  ┆ null ┆ null     │
    │ 6     ┆ 35  ┆ 19  ┆ 18  ┆ null ┆ null     │
    │ 7     ┆ 35  ┆ 30  ┆ 9   ┆ null ┆ null     │
    │ 8     ┆ 35  ┆ 19  ┆ 16  ┆ null ┆ null     │
    │ 9     ┆ 34  ┆ 29  ┆ 6   ┆ null ┆ null     │
    │ 10    ┆ 34  ┆ 36  ┆ 3   ┆ null ┆ null     │
    └───────┴─────┴─────┴─────┴──────┴──────────┘



### Recipe 11 — Crunch-time buckets 🔥 (parquet PBP)

[`load_mbb_pbp`](../mbb/reference/loaders.md#load_mbb_pbp) is the whole season's play-by-play in one parquet — no live game needed. We slice it to scoring plays in the final minute of the second half: every late-game dagger across the year.


```python
season_pbp = sdv.mbb.load_mbb_pbp(seasons=[2024])
print("season pbp rows:", season_pbp.shape)
(season_pbp
    .filter(
        (pl.col("scoring_play") == True)  # noqa: E712
        & (pl.col("period_number") >= 2)
        & (pl.col("end_period_seconds_remaining").cast(pl.Float64, strict=False) <= 60)
    )
    .select(["game_id", "period_display_value", "clock_display_value",
             "text", "home_score", "away_score"])
    .head(10))
```

    season pbp rows: (2004997, 58)





    shape: (10, 6)
    ┌───────────┬────────────────────┬───────────────────┬───────────────────┬────────────┬────────────┐
    │ game_id   ┆ period_display_val ┆ clock_display_val ┆ text              ┆ home_score ┆ away_score │
    │ ---       ┆ ue                 ┆ ue                ┆ ---               ┆ ---        ┆ ---        │
    │ i32       ┆ ---                ┆ ---               ┆ str               ┆ i32        ┆ i32        │
    │           ┆ str                ┆ str               ┆                   ┆            ┆            │
    ╞═══════════╪════════════════════╪═══════════════════╪═══════════════════╪════════════╪════════════╡
    │ 401573353 ┆ 2nd Half           ┆ 0:19              ┆ Aanen Moody made  ┆ 89         ┆ 79         │
    │           ┆                    ┆                   ┆ Three Point J…    ┆            ┆            │
    │ 401573354 ┆ 2nd Half           ┆ 0:20              ┆ Terren Frank made ┆ 48         ┆ 62         │
    │           ┆                    ┆                   ┆ Jumper.           ┆            ┆            │
    │ 401573355 ┆ 2nd Half           ┆ 0:04              ┆ Miguel Tomley     ┆ 75         ┆ 76         │
    │           ┆                    ┆                   ┆ made Free Throw.  ┆            ┆            │
    │ 401573355 ┆ 2nd Half           ┆ 0:04              ┆ Miguel Tomley     ┆ 76         ┆ 76         │
    │           ┆                    ┆                   ┆ made Free Throw.  ┆            ┆            │
    │ 401573355 ┆ OT                 ┆ 1:16              ┆ Carson Basham     ┆ 83         ┆ 79         │
    │           ┆                    ┆                   ┆ made Free Throw.  ┆            ┆            │
    │ 401573355 ┆ OT                 ┆ 0:51              ┆ Trent McLaughlin  ┆ 83         ┆ 82         │
    │           ┆                    ┆                   ┆ made Three Po…    ┆            ┆            │
    │ 401573355 ┆ OT                 ┆ 0:29              ┆ Maleek Arington   ┆ 84         ┆ 82         │
    │           ┆                    ┆                   ┆ made Free Thro…   ┆            ┆            │
    │ 401573355 ┆ OT                 ┆ 0:06              ┆ Liam Lloyd made   ┆ 84         ┆ 83         │
    │           ┆                    ┆                   ┆ Free Throw.       ┆            ┆            │
    │ 401573355 ┆ OT                 ┆ 0:06              ┆ Liam Lloyd made   ┆ 84         ┆ 84         │
    │           ┆                    ┆                   ┆ Free Throw.       ┆            ┆            │
    │ 401573355 ┆ 2OT                ┆ 0:42              ┆ Jayden Jackson    ┆ 88         ┆ 90         │
    │           ┆                    ┆                   ┆ made Layup.       ┆            ┆            │
    └───────────┴────────────────────┴───────────────────┴───────────────────┴────────────┴────────────┘



### Recipe 12 — Double-double leaders 🐼 (pandas interop)

Prefer pandas? Pass `return_as_pandas=True` to any loader and stay in your comfort zone. Here we count games where a player hit double digits in at least two of points / rebounds / assists — the classic double-double — entirely in pandas.


```python
import pandas as pd

pbox_pd = sdv.mbb.load_mbb_player_boxscore(seasons=[2024], return_as_pandas=True)
for col in ["points", "rebounds", "assists"]:
    pbox_pd[col] = pd.to_numeric(pbox_pd[col], errors="coerce")
pbox_pd["is_dd"] = (pbox_pd[["points", "rebounds", "assists"]] >= 10).sum(axis=1) >= 2
(pbox_pd[pbox_pd["is_dd"]]
    .groupby(["athlete_display_name", "team_abbreviation"])
    .size()
    .reset_index(name="double_doubles")
    .sort_values("double_doubles", ascending=False)
    .head(10)
    .reset_index(drop=True))
```




       athlete_display_name team_abbreviation  double_doubles
    0       Enrique Freeman               AKR              31
    1             Zach Edey               PUR              30
    2  Vonterius Woolbright               WCU              27
    3              DJ Burns               YSU              22
    4           Oumar Ballo              ARIZ              20
    5         Armando Bacot               UNC              19
    6         Fardaws Aimaq               CAL              19
    7       Yaxel Lendeborg               UAB              19
    8          Saint Thomas              UNCO              19
    9           Riley Minix              MORE              19



## 🧾 One call, the whole game: `espn_mbb_summary`

[`espn_mbb_summary`](../mbb/reference/site.md#espn_mbb_summary) is the Swiss
army knife — a single `event_id` returns a dict with team & player box scores,
play-by-play, win probability, leaders, officials and more. Let's grab the team
box score from that 2024 title game.


```python
summ = safe("summary 401638636", lambda: sdv.mbb.espn_mbb_summary(event_id=401638636))
if isinstance(summ, dict) and summ.get("boxscore_team") is not None:
    tb = summ["boxscore_team"]
    tb = tb if isinstance(tb, pl.DataFrame) else pl.DataFrame(tb)
    print("box score sections available:", [k for k in summ.keys()][:8])
    out = tb.head()
else:
    out = "summary unavailable right now"
out
```

    ✅ summary 401638636
    box score sections available: ['boxscore_player', 'boxscore_team', 'plays', 'winprobability', 'leaders', 'game_info', 'officials', 'header']





    shape: (5, 9)
    ┌─────────┬────────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
    │ team_id ┆ team_abbre ┆ team_disp ┆ home_away ┆ … ┆ stat_name ┆ stat_labe ┆ stat_disp ┆ stat_valu │
    │ ---     ┆ viation    ┆ lay_name  ┆ ---       ┆   ┆ ---       ┆ l         ┆ lay_value ┆ e         │
    │ str     ┆ ---        ┆ ---       ┆ str       ┆   ┆ str       ┆ ---       ┆ ---       ┆ ---       │
    │         ┆ str        ┆ str       ┆           ┆   ┆           ┆ str       ┆ str       ┆ str       │
    ╞═════════╪════════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
    │ 156     ┆ CREI       ┆ Creighton ┆ away      ┆ … ┆ fieldGoal ┆ FG        ┆ 26-58     ┆ null      │
    │         ┆            ┆ Bluejays  ┆           ┆   ┆ sMade-fie ┆           ┆           ┆           │
    │         ┆            ┆           ┆           ┆   ┆ ldGoalsAt ┆           ┆           ┆           │
    │         ┆            ┆           ┆           ┆   ┆ tem…      ┆           ┆           ┆           │
    │ 156     ┆ CREI       ┆ Creighton ┆ away      ┆ … ┆ fieldGoal ┆ Field     ┆ 45        ┆ null      │
    │         ┆            ┆ Bluejays  ┆           ┆   ┆ Pct       ┆ Goal %    ┆           ┆           │
    │ 156     ┆ CREI       ┆ Creighton ┆ away      ┆ … ┆ threePoin ┆ 3PT       ┆ 11-23     ┆ null      │
    │         ┆            ┆ Bluejays  ┆           ┆   ┆ tFieldGoa ┆           ┆           ┆           │
    │         ┆            ┆           ┆           ┆   ┆ lsMade-th ┆           ┆           ┆           │
    │         ┆            ┆           ┆           ┆   ┆ ree…      ┆           ┆           ┆           │
    │ 156     ┆ CREI       ┆ Creighton ┆ away      ┆ … ┆ threePoin ┆ Three     ┆ 48        ┆ null      │
    │         ┆            ┆ Bluejays  ┆           ┆   ┆ tFieldGoa ┆ Point %   ┆           ┆           │
    │         ┆            ┆           ┆           ┆   ┆ lPct      ┆           ┆           ┆           │
    │ 156     ┆ CREI       ┆ Creighton ┆ away      ┆ … ┆ freeThrow ┆ FT        ┆ 12-13     ┆ null      │
    │         ┆            ┆ Bluejays  ┆           ┆   ┆ sMade-fre ┆           ┆           ┆           │
    │         ┆            ┆           ┆           ┆   ┆ eThrowsAt ┆           ┆           ┆           │
    │         ┆            ┆           ┆           ┆   ┆ tem…      ┆           ┆           ┆           │
    └─────────┴────────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘



## 🙌 Who suited up: game rosters

[`espn_mbb_game_rosters`](../mbb/reference/additional.md#espn_mbb_game_rosters)
returns one row per dressed player for a game, flagging starters — handy for
joining onto play-by-play or box scores.


```python
gr = safe("game rosters 401638636", lambda: sdv.mbb.espn_mbb_game_rosters(game_id=401638636))
if gr is not None and getattr(gr, "height", 0):
    keep = ["athlete_display_name", "team_abbreviation", "starter"]
    out = gr.select([c for c in keep if c in gr.columns]).head(10)
else:
    out = "game rosters unavailable right now"
out
```

    ✅ game rosters 401638636





    shape: (10, 3)
    ┌──────────────────────┬───────────────────┬─────────┐
    │ athlete_display_name ┆ team_abbreviation ┆ starter │
    │ ---                  ┆ ---               ┆ ---     │
    │ str                  ┆ str               ┆ bool    │
    ╞══════════════════════╪═══════════════════╪═════════╡
    │ Jonas Aidoo          ┆ TENN              ┆ true    │
    │ Dalton Knecht        ┆ TENN              ┆ true    │
    │ Zakai Zeigler        ┆ TENN              ┆ true    │
    │ Jahmai Mashack       ┆ TENN              ┆ true    │
    │ Josiah-Jordan James  ┆ TENN              ┆ true    │
    │ Tobe Awaka           ┆ TENN              ┆ false   │
    │ J.P. Estrella        ┆ TENN              ┆ false   │
    │ Freddie Dilione V    ┆ TENN              ┆ false   │
    │ Cameron Carr         ┆ TENN              ┆ false   │
    │ Jordan Gainey        ┆ TENN              ┆ false   │
    └──────────────────────┴───────────────────┴─────────┘



## 🔧 A multi-season pipeline: highest-scoring tournament games

The schedule loader is stable, so here's a pure-polars analysis with no live
dependency. We load the 2024 season schedule and rank games by combined
points — March Madness shootouts float right to the top.


```python
schedule_2024 = sdv.mbb.load_mbb_schedule(seasons=[2024])
print("season schedule rows:", schedule_2024.shape)
(schedule_2024
    .with_columns(
        (pl.col("home_score").cast(pl.Int64, strict=False)
         + pl.col("away_score").cast(pl.Int64, strict=False)).alias("total"))
    .filter(pl.col("total").is_not_null())
    .sort("total", descending=True)
    .select(["game_date", "home_display_name", "away_display_name",
             "home_score", "away_score", "total"])
    .head(10))
```

    season schedule rows: (6249, 84)





    shape: (10, 6)
    ┌────────────┬───────────────────────────┬───────────────────────┬────────────┬────────────┬───────┐
    │ game_date  ┆ home_display_name         ┆ away_display_name     ┆ home_score ┆ away_score ┆ total │
    │ ---        ┆ ---                       ┆ ---                   ┆ ---        ┆ ---        ┆ ---   │
    │ date       ┆ str                       ┆ str                   ┆ i32        ┆ i32        ┆ i64   │
    ╞════════════╪═══════════════════════════╪═══════════════════════╪════════════╪════════════╪═══════╡
    │ 2024-01-03 ┆ George Washington         ┆ Fordham Rams          ┆ 113        ┆ 119        ┆ 232   │
    │            ┆ Revolutionar…             ┆                       ┆            ┆            ┆       │
    │ 2024-01-13 ┆ Samford Bulldogs          ┆ VMI Keydets           ┆ 134        ┆ 96         ┆ 230   │
    │ 2023-12-14 ┆ Tulane Green Wave         ┆ Furman Paladins       ┆ 117        ┆ 110        ┆ 227   │
    │ 2024-01-25 ┆ Denver Pioneers           ┆ South Dakota Coyotes  ┆ 111        ┆ 110        ┆ 221   │
    │ 2023-11-09 ┆ Kent State Golden Flashes ┆ James Madison Dukes   ┆ 108        ┆ 113        ┆ 221   │
    │ 2023-11-08 ┆ Bryant Bulldogs           ┆ Fisher College Eagles ┆ 140        ┆ 79         ┆ 219   │
    │ 2024-02-03 ┆ UAlbany Great Danes       ┆ UMBC Retrievers       ┆ 102        ┆ 114        ┆ 216   │
    │ 2024-01-21 ┆ UTSA Roadrunners          ┆ Florida Atlantic Owls ┆ 103        ┆ 112        ┆ 215   │
    │ 2024-03-02 ┆ Kentucky Wildcats         ┆ Arkansas Razorbacks   ┆ 111        ┆ 102        ┆ 213   │
    │ 2024-02-10 ┆ Appalachian State         ┆ Toledo Rockets        ┆ 109        ┆ 104        ┆ 213   │
    │            ┆ Mountaineers              ┆                       ┆            ┆            ┆       │
    └────────────┴───────────────────────────┴───────────────────────┴────────────┴────────────┴───────┘



## 🎉 Where to next

- 🟥 **ESPN** wrappers (`espn_mbb_*`) cover the live site + core APIs —
  scoreboards, standings, rankings, summaries, play-by-play and more. See the
  [additional](../mbb/reference/additional.md) and
  [site](../mbb/reference/site.md) reference pages.
- 🦊 **FoxSports** wrappers (`fox_mbb_*`) — leaders, standings, rosters,
  boxscores and odds in [additional](../mbb/reference/additional.md).
- 📦 **Loaders** (`load_mbb_*`) read whole seasons of parquet — see
  [loaders](../mbb/reference/loaders.md). Pass `return_as_pandas=True` anywhere
  for pandas instead of polars.
- 🏀 R user? The same surface lives in
  [hoopR](https://hoopR.sportsdataverse.org) (NBA + NCAA men's basketball).
- 🚺 Women's hoops? Check out the **WBB** module and its companion
  [wehoop](https://wehoop.sportsdataverse.org).

Now go bracket something! 🏀🔥
