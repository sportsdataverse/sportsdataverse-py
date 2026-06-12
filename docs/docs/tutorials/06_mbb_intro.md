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
