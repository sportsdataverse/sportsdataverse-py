---
title: MLB tutorial
sidebar_label: MLB
sidebar_position: 8
---

# ⚾ Baseball with `sportsdataverse-py`

Welcome to the ballpark! 🏟️ In just a few lines of Python you're about to
pull **official MLB data** — schedules, standings, rosters, box scores,
play-by-play — straight from the league's own **MLB Stats API**, plus
**pitch-level Statcast** tracking from [Baseball Savant](https://baseballsavant.mlb.com/).
Every premium call hands you back a tidy **polars** DataFrame (or raw JSON
when you want it), ready to model. 🚀

If you've used the R package [baseballr](https://billpetti.github.io/baseballr/),
or Python's [pybaseball](https://github.com/jldbc/pybaseball), the data shapes
will feel right at home. Let's play ball! ⚾

## 🧰 The toolbox

We **lead with the premium sources** — the **MLB Stats API** (`mlb_api_*`,
backed by `statsapi.mlb.com`) and **Statcast** (`statcast_*`, from Baseball
Savant). ESPN (`espn_mlb_*`) is a handy secondary path. Click any name for the
full reference:

| Function | What it gives you | Source |
|---|---|---|
| [`mlb_api_schedule`](../mlb/reference/additional.md#mlb_api_schedule) · [`parse_mlb_api_schedule`](../mlb/reference/additional.md#mlb_api_schedule) | Games for a date / range — one row per game (with `game_pk`) | 🟢 **MLB Stats API** |
| [`mlb_api_teams`](../mlb/reference/additional.md#mlb_api_teams) · [`parse_mlb_api_teams`](../mlb/reference/additional.md#mlb_api_teams) | Every club — one row per team | 🟢 **MLB Stats API** |
| [`mlb_api_standings`](../mlb/reference/additional.md#mlb_api_standings) · [`parse_mlb_api_standings`](../mlb/reference/additional.md#mlb_api_standings) | Division standings — wins, losses, run diff | 🟢 **MLB Stats API** |
| [`mlb_api_team_roster`](../mlb/reference/mlb_api.md#mlb_api_team_roster) | A team's roster — one row per player | 🟢 **MLB Stats API** |
| [`mlb_api_person`](../mlb/reference/mlb_api.md#mlb_api_person) | A player's bio (one tidy row) | 🟢 **MLB Stats API** |
| [`mlb_api_person_stats`](../mlb/reference/additional.md#mlb_api_person_stats) · [`parse_mlb_api_person_stats`](../mlb/reference/additional.md#mlb_api_person_stats) | A player's season stat splits | 🟢 **MLB Stats API** |
| [`mlb_api_boxscore`](../mlb/reference/mlb_api.md#mlb_api_boxscore) | Full game box score | 🟢 **MLB Stats API** |
| [`mlb_api_play_by_play`](../mlb/reference/mlb_api.md#mlb_api_play_by_play) | Plate-appearance-level play-by-play | 🟢 **MLB Stats API** |
| [`statcast_search`](../mlb/reference/additional.md#statcast_search) | Every pitch matching a filter — 100+ columns/pitch | 🔵 **Statcast** |
| [`statcast_leaderboard_sprint_speed`](../mlb/reference/additional.md#statcast_leaderboard_sprint_speed) | Pre-aggregated sprint-speed leaderboard | 🔵 **Statcast** |
| [`statcast_leaderboard_bat_tracking`](../mlb/reference/additional.md#statcast_leaderboard_bat_tracking) | Bat-speed / swing-tracking leaderboard | 🔵 **Statcast** |
| [`statcast_gamefeed`](../mlb/reference/additional.md#statcast_gamefeed) | Savant single-game feed | 🔵 **Statcast** |
| [`espn_mlb_teams`](../mlb/reference/additional.md#espn_mlb_teams) · [`espn_mlb_schedule`](../mlb/reference/additional.md#espn_mlb_schedule) | ESPN teams / schedule (wide frames) | ⚪ ESPN |
| [`most_recent_mlb_season`](../mlb/reference/additional.md#most_recent_mlb_season) | Current season helper | ⚪ helper |

## 🔌 Setup

```sh
pip install sportsdataverse
```

**No API key needed** for any of the premium MLB endpoints — the MLB Stats API
and Baseball Savant are both public. 🎉


```python
import polars as pl
import sportsdataverse.mlb as mlb

pl.Config.set_tbl_rows(12)
print("most recent MLB season:", mlb.most_recent_mlb_season())
```

The MLB Stats API and Savant are public and reliable, but they're still
**live network calls** — a date with no games, an offseason day, or a blip can
make a call come back empty. So we use a tiny `safe()` helper: you get the
frame when the feed is up, and a friendly one-liner when it isn't (never a
scary traceback). 🛟

We also pick a stable **completed-season** date for our examples so the page
renders the same in June as in October.


```python
def safe(label, thunk):
    """Run a live call defensively: return its result, or print a one-liner."""
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None

# A known completed regular-season slate — stable for the docs build.
SAMPLE_SEASON = 2024
SAMPLE_DATE = "2024-07-01"  # YYYY-MM-DD for the Stats API
JUDGE_ID = 592450           # Aaron Judge, NYY — our running example player
YANKEES_ID = 147            # New York Yankees team_id
```

## 📅 The schedule (MLB Stats API)

[`mlb_api_schedule`](../mlb/reference/additional.md#mlb_api_schedule) returns the
raw JSON `dict`; its partner
[`parse_mlb_api_schedule`](../mlb/reference/additional.md#mlb_api_schedule)
flattens it to **one row per game**. The most important column is `game_pk` —
that's the id you feed to the box score and play-by-play endpoints. Pass a
single `date=`, or a `start_date`/`end_date` range, `team_id`, or `season`.


```python
schedule = safe(
    "schedule",
    lambda: mlb.parse_mlb_api_schedule(mlb.mlb_api_schedule(date=SAMPLE_DATE)),
)
cols = ["game_pk", "status_detailed_state",
        "teams_away_team_name", "teams_away_score",
        "teams_home_team_name", "teams_home_score"]
(schedule.select([c for c in cols if c in schedule.columns]).head()
 if schedule is not None else "schedule unavailable right now")
```

## 🏆 Standings (MLB Stats API)

[`mlb_api_standings`](../mlb/reference/additional.md#mlb_api_standings) covers
both leagues by default (`league_id="103,104"`).
[`parse_mlb_api_standings`](../mlb/reference/additional.md#mlb_api_standings)
returns one row per team with wins/losses, division rank, and winning
percentage.


```python
standings = safe(
    "standings",
    lambda: mlb.parse_mlb_api_standings(mlb.mlb_api_standings(season=SAMPLE_SEASON)),
)
keep = ["team_name", "standings_division_name", "wins", "losses",
        "winning_percentage", "division_rank"]
(standings.select([c for c in keep if c in standings.columns])
         .sort("wins", descending=True).head(10)
 if standings is not None else "standings unavailable right now")
```

## 🧢 Teams & rosters (MLB Stats API)

[`mlb_api_teams`](../mlb/reference/additional.md#mlb_api_teams) +
[`parse_mlb_api_teams`](../mlb/reference/additional.md#mlb_api_teams) lists every
club — grab a `team_id` here.
[`mlb_api_team_roster`](../mlb/reference/mlb_api.md#mlb_api_team_roster) then
returns a tidy frame directly (one row per player).


```python
teams = safe(
    "teams",
    lambda: mlb.parse_mlb_api_teams(mlb.mlb_api_teams(season=SAMPLE_SEASON)),
)
(teams.select(["id", "name", "abbreviation", "location_name", "team_name"]).head()
 if teams is not None else "teams unavailable right now")
```


```python
roster = safe(
    "Yankees roster",
    lambda: mlb.mlb_api_team_roster(team_id=YANKEES_ID, season=SAMPLE_SEASON),
)
rcols = ["jersey_number", "person_id", "person_full_name",
         "position_abbreviation", "status_description"]
(roster.select([c for c in rcols if c in roster.columns]).head()
 if roster is not None else "roster unavailable right now")
```

## 🧍 Player bio & season stats (MLB Stats API)

[`mlb_api_person`](../mlb/reference/mlb_api.md#mlb_api_person) returns a one-row
bio frame. [`mlb_api_person_stats`](../mlb/reference/additional.md#mlb_api_person_stats)
returns the raw stat-split `dict`;
[`parse_mlb_api_person_stats`](../mlb/reference/additional.md#mlb_api_person_stats)
flattens it. Our running example is Aaron Judge (`person_id=592450`).


```python
bio = safe("Judge bio", lambda: mlb.mlb_api_person(person_id=JUDGE_ID))
bcols = ["id", "full_name", "primary_number", "birth_date",
         "height", "weight", "mlb_debut_date"]
(bio.select([c for c in bcols if c in bio.columns])
 if bio is not None else "bio unavailable right now")
```


```python
hitting = safe(
    "Judge 2024 hitting",
    lambda: mlb.parse_mlb_api_person_stats(
        mlb.mlb_api_person_stats(person_id=JUDGE_ID, stats="season",
                                 group="hitting", season=SAMPLE_SEASON)
    ),
)
scols = ["season", "stat_games_played", "stat_home_runs", "stat_rbi",
         "stat_avg", "stat_obp", "stat_slg", "stat_ops"]
(hitting.select([c for c in scols if c in hitting.columns])
 if hitting is not None else "stats unavailable right now")
```

## 🎯 Pitch-level Statcast (Baseball Savant)

Now the fun part — **every single pitch**.
[`statcast_search`](../mlb/reference/additional.md#statcast_search) pulls each
pitch matching your filter, with 100+ columns (velocity, spin, launch angle,
expected stats). Keep windows **small** (one player, one game, or a 1–2 day
slice) — a full season is millions of pitches. Here's every pitch Aaron Judge
saw over a two-day window.


```python
pitches = safe(
    "Judge pitches (2-day)",
    lambda: mlb.statcast_search(start_date="2024-07-01", end_date="2024-07-02",
                                batters_lookup=JUDGE_ID),
)
if pitches is not None and pitches.height:
    print("shape:", pitches.shape)
    out = pitches.select(["game_date", "player_name", "pitch_type", "release_speed",
                          "launch_speed", "launch_angle", "events", "description"]).head()
else:
    out = "no pitches in that window right now"
out
```

## 🍳 Cookbook: common baseball tasks

A handful of recipes you'll reach for constantly — every one leads with a
**premium** source.

### Recipe 1 — A team's schedule + where they sit in the standings 📋

Pull one club's slate with `mlb_api_schedule(team_id=...)`, then find their row
in the standings. Two premium calls, one tidy snapshot.


```python
yanks_sched = safe(
    "Yankees July schedule",
    lambda: mlb.parse_mlb_api_schedule(
        mlb.mlb_api_schedule(team_id=YANKEES_ID,
                             start_date="2024-07-01", end_date="2024-07-07")
    ),
)
sched_cols = ["game_pk", "official_date", "teams_away_team_name",
              "teams_home_team_name", "teams_away_score", "teams_home_score"]
if yanks_sched is not None and yanks_sched.height:
    games = yanks_sched.select([c for c in sched_cols if c in yanks_sched.columns])
else:
    games = "schedule unavailable right now"

if standings is not None and "team_name" in standings.columns:
    rank = (standings.filter(pl.col("team_name").str.contains("Yankees"))
                     .select([c for c in ["team_name", "wins", "losses", "division_rank"]
                              if c in standings.columns]))
else:
    rank = "standings unavailable"
print(rank)
games
```

### Recipe 2 — A Statcast leaderboard 🏃

The `statcast_leaderboard_*` family wraps Savant's *pre-aggregated* season
leaderboards — fast, because the heavy lifting happens server-side. Here's the
2024 **sprint speed** leaderboard, fastest first.


```python
sprint = safe(
    "sprint speed leaderboard",
    lambda: mlb.statcast_leaderboard_sprint_speed(year=SAMPLE_SEASON, min_opp=10),
)
spcols = ["last_name, first_name", "team", "position", "competitive_runs", "sprint_speed"]
(sprint.select([c for c in spcols if c in sprint.columns])
       .sort("sprint_speed", descending=True).head(10)
 if sprint is not None and sprint.height else "leaderboard unavailable right now")
```

### Recipe 3 — Box score for one game 📊

Take a `game_pk` from any schedule and pull the full box score with
[`mlb_api_boxscore`](../mlb/reference/mlb_api.md#mlb_api_boxscore). Asking for
`return_parsed=False` gives the raw `dict`, which carries per-team batting and
pitching lines under `teams.home` / `teams.away`.


```python
def team_line(game_pk):
    box = mlb.mlb_api_boxscore(game_pk=game_pk, return_parsed=False)
    rows = []
    for side in ("away", "home"):
        t = box["teams"][side]
        bat = t["teamStats"]["batting"]
        rows.append({"side": side, "team": t["team"]["name"],
                     "runs": bat["runs"], "hits": bat["hits"],
                     "home_runs": bat["homeRuns"], "rbi": bat["rbi"], "avg": bat["avg"]})
    return pl.DataFrame(rows)

# Use a game_pk from the schedule we pulled, or fall back to a known game.
gid = int(schedule["game_pk"][0]) if (schedule is not None and schedule.height) else 744914
box_df = safe(f"boxscore {gid}", lambda: team_line(gid))
out = box_df if box_df is not None else "boxscore unavailable right now"
out
```

### Recipe 4 — Plate-appearance play-by-play + outcome mix ⚾

[`mlb_api_play_by_play`](../mlb/reference/mlb_api.md#mlb_api_play_by_play)
returns a `dict` with an `allPlays` list — one entry per plate appearance.
Flatten it with `pl.json_normalize` (dot-notation columns), then tally the
plate-appearance outcomes.


```python
def pbp_frame(game_pk):
    raw = mlb.mlb_api_play_by_play(game_pk=game_pk, return_parsed=False)
    return pl.json_normalize(raw["allPlays"], separator=".", max_level=2)

plays = safe(f"play-by-play {gid}", lambda: pbp_frame(gid))
if plays is not None and plays.height:
    pcols = ["about.inning", "about.halfInning", "matchup.batter.fullName",
             "matchup.pitcher.fullName", "result.event"]
    out = plays.select([c for c in pcols if c in plays.columns]).head()
else:
    out = "play-by-play unavailable right now"
out
```


```python
# Outcome mix for the game — the shape of every plate appearance.
if plays is not None and plays.height and "result.event" in plays.columns:
    out = (plays.group_by("result.event")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True).head(10))
else:
    out = "no play-by-play to summarize right now"
out
```

## 📅 A whole season's schedule via ESPN

Want *every* game in a season without looping over dates? The bulk
`load_mlb_*` release-parquet loaders are still being wired up (they raise a
friendly `NotImplementedError` for now), and they point you to the working
path: [`espn_mlb_schedule`](../mlb/reference/additional.md#espn_mlb_schedule)
with `dates=<season year>` pulls the full slate as one wide frame. Scores come
back as **strings** — cast before doing arithmetic.


```python
season_sched = safe(
    "ESPN 2024 season schedule",
    lambda: mlb.espn_mlb_schedule(dates=2024),
)
if season_sched is not None and season_sched.height:
    print("games:", season_sched.height)
    scols = ["game_id", "away_display_name", "away_score",
             "home_display_name", "home_score", "status_type_completed"]
    out = season_sched.select([c for c in scols if c in season_sched.columns]).head()
else:
    out = "ESPN schedule unavailable right now"
out
```

## ⚪ Secondary path: ESPN teams (`espn_mlb_*`)

[`espn_mlb_teams`](../mlb/reference/additional.md#espn_mlb_teams) returns one
wide polars frame — handy as a cross-check, or when you want ESPN's display
names and ids alongside the MLB Stats API ones.


```python
espn_teams = safe("ESPN teams", lambda: mlb.espn_mlb_teams())
ecols = ["team_id", "team_location", "team_name", "team_abbreviation", "team_display_name"]
(espn_teams.select([c for c in ecols if c in espn_teams.columns]).head()
 if espn_teams is not None else "ESPN teams unavailable right now")
```

## 🎉 Where to next

- Everything returns **polars** by default — pass `return_as_pandas=True` for a
  pandas frame, or `return_parsed=False` on the `mlb_api_*` wrappers for raw JSON.
- Full reference: the **MLB** pages in the sidebar —
  [MLB Stats API + Statcast helpers](../mlb/reference/additional.md),
  [the full MLB Stats API surface](../mlb/reference/mlb_api.md),
  and the ESPN [core](../mlb/reference/core.md) / [site](../mlb/reference/site.md) / [web](../mlb/reference/web.md) endpoints.
- R user? The same data lives in [baseballr](https://billpetti.github.io/baseballr/).
- Compare conventions with the other league intros (`04_nba_intro.ipynb`,
  `07_nhl_intro.ipynb`) or the cross-sport `01_quickstart.ipynb`.

Now go find the next 60-homer season. ⚾🔥
