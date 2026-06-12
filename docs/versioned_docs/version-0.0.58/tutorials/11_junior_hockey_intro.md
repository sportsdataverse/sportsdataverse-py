---
title: Junior & minor hockey tutorial
sidebar_label: Junior & minor hockey
sidebar_position: 11
---

# 🏒 Junior & minor hockey with `sportsdataverse-py`

Four leagues, **one toolkit**. `sportsdataverse` wraps the HockeyTech /
LeagueStat feed behind the American Hockey League (**AHL**) and the three
Canadian Hockey League major-junior loops — Ontario (**OHL**), Western
(**WHL**) and Quebec Maritimes (**QMJHL**). They share an *identical* module
shape, so learn one and you've learned all four. 🎉

And the best part: **no API key needed** — the public HockeyTech client keys
ship with the package. Let's go scout some future pros!

## 🗺️ The four leagues

| League | Module | Level |
|---|---|---|
| AHL | `sportsdataverse.ahl` | Minor pro (one rung below the NHL) |
| OHL | `sportsdataverse.ohl` | CHL major junior |
| WHL | `sportsdataverse.whl` | CHL major junior |
| QMJHL | `sportsdataverse.qmjhl` | CHL major junior |

Every module exposes the same `<league>_*` surface. Here's the AHL kit —
swap `ahl_` for `ohl_` / `whl_` / `qmjhl_` and it all works identically.
Every accessor returns **polars** by default (`return_as_pandas=True` for pandas):

| Function | What it gives you |
|---|---|
| [`ahl_schedule`](../ahl/reference/additional.md#ahl_schedule) | Games + results, one row per game |
| [`ahl_standings`](../ahl/reference/additional.md#ahl_standings) | Team standings |
| [`ahl_teams`](../ahl/reference/additional.md#ahl_teams) | Teams in a season (grab `team_id`s) |
| [`ahl_team_roster`](../ahl/reference/additional.md#ahl_team_roster) | A team's roster |
| [`ahl_pbp`](../ahl/reference/additional.md#ahl_pbp) | Event-level play-by-play |
| [`ahl_player_stats`](../ahl/reference/additional.md#ahl_player_stats) | A player's full season-by-season stat line |
| [`ahl_leaders`](../ahl/reference/additional.md#ahl_leaders) | Statistical leaders |
| [`ahl_game_summary`](../ahl/reference/additional.md#ahl_game_summary) | Box-score summary for a game (goals / penalties / stars) |
| [`ahl_game_corsi`](../ahl/reference/additional.md#ahl_game_corsi) | Corsi / Fenwick shot-attempt metrics |
| [`ahl_game_shifts`](../ahl/reference/additional.md#ahl_game_shifts) | Shift charts |
| [`ahl_player_toi`](../ahl/reference/additional.md#ahl_player_toi) | Time-on-ice |
| [`ahl_season_id`](../ahl/reference/additional.md#ahl_season_id) · `most_recent_ahl_season` | Season helpers |

## 🔌 Setup

```sh
pip install sportsdataverse
```


```python
import sportsdataverse.ahl as ahl
import sportsdataverse.ohl as ohl
import sportsdataverse.whl as whl
import sportsdataverse.qmjhl as qmjhl

LEAGUES = {"ahl": ahl, "ohl": ohl, "whl": whl, "qmjhl": qmjhl}
```

Junior/minor feeds are seasonal and occasionally rate-limited, so a tiny
`safe()` helper runs each call defensively — you get the frame when the feed
is up, and a friendly one-liner when it isn't (never a scary traceback). 🛟


```python
def safe(label, thunk):
    try:
        out = thunk()
        print(f"✅ {label}")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"⏭️  {label}: unavailable right now ({type(e).__name__})")
        return None
```

## 📅 Schedule

[`ahl_schedule`](../ahl/reference/additional.md#ahl_schedule) (and its siblings) returns one row per game.
Pass `season=<end year>` or let it default to the current season.


```python
sched = safe("AHL schedule", lambda: ahl.ahl_schedule(season=ahl.most_recent_ahl_season()))
sched.shape if sched is not None else None
```


```python
cols = ["game_id", "game_date", "home_team", "away_team", "home_score", "away_score"]
(sched.select([c for c in cols if c in sched.columns]).head()
 if sched is not None else "schedule unavailable")
```

## 🍳 Cookbook: common hockey tasks

The shared surface makes these recipes work the same in every league — just
swap the module. A dozen recipes follow, from leaderboards to box scores to
pandas hand-offs; mix and match them however your scouting report needs. 📋

### Recipe 1 — Standings table 🏆

[`ohl_standings`](../ahl/reference/additional.md#ahl_standings) (shown for the OHL) gives one row per team.


```python
standings = safe("OHL standings", lambda: ohl.ohl_standings(season=ohl.most_recent_ohl_season()))
cols = ["team", "games_played", "wins", "losses", "ot_losses", "points", "goals_for", "goals_against"]
(standings.select([c for c in cols if c in standings.columns]).head()
 if standings is not None and standings.height else "standings unavailable")
```

### Recipe 2 — A team and its roster 👥

List teams with [`whl_teams`](../ahl/reference/additional.md#ahl_teams), grab a `team_id`, then pull the
roster with [`whl_team_roster`](../ahl/reference/additional.md#ahl_team_roster).


```python
teams = safe("WHL teams", lambda: whl.whl_teams(season=whl.most_recent_whl_season()))
if teams is not None and teams.height:
    tid_col = next((c for c in ("team_id", "id") if c in teams.columns), None)
    tid = int(teams[tid_col][0]) if tid_col else None
    roster = safe(f"WHL roster {tid}", lambda: whl.whl_team_roster(team_id=tid)) if tid else None
    out = roster.head() if roster is not None and roster.height else teams.head()
else:
    out = "teams unavailable"
out
```

### Recipe 3 — A game's play-by-play + shot attempts 📈

Take a `game_id` from the schedule, then [`ahl_pbp`](../ahl/reference/additional.md#ahl_pbp) for events and
[`ahl_game_corsi`](../ahl/reference/additional.md#ahl_game_corsi) for Corsi/Fenwick — derived from the same feed.


```python
import polars as pl

def latest_game_id(sched):
    """Most-recent game_id available in a schedule frame (or None)."""
    if sched is None or not sched.height or "game_id" not in sched.columns:
        return None
    return int(sched.select(pl.col("game_id").cast(pl.Int64, strict=False)).max().item())

gid = latest_game_id(sched)
if gid is not None:
    pbp = safe(f"AHL pbp {gid}", lambda: ahl.ahl_pbp(game_id=gid))
    corsi = safe(f"AHL corsi {gid}", lambda: ahl.ahl_game_corsi(game_id=gid))
    print("pbp rows:", None if pbp is None else pbp.height,
          "| corsi rows:", None if corsi is None else corsi.height)
else:
    print("no schedule rows to pick a game_id from")
```

### Recipe 4 — Compare all four leagues at once 🔁

Because the surface is identical, one loop tours every league.


```python
rows = []
for lg, mod in LEAGUES.items():
    season = safe(f"{lg} season", getattr(mod, f'most_recent_{lg}_season'))
    sch = safe(f"{lg} schedule", lambda mod=mod, lg=lg: getattr(mod, f'{lg}_schedule')()) if season else None
    rows.append({"league": lg.upper(), "season": season, "games": None if sch is None else sch.height})
pl.DataFrame(rows)
```

### Recipe 5 — The scoring race 🥇

[`qmjhl_leaders`](../ahl/reference/additional.md#ahl_leaders) hands back a ready-made leaderboard —
`rank`, `name`, `team_code`, the `stat_formatted` value and what it measures
(`type_formatted`). Same call, swap the module, and you've got every league's
points race.


```python
leaders = safe("QMJHL leaders", lambda: qmjhl.qmjhl_leaders(season=qmjhl.most_recent_qmjhl_season()))
cols = ["rank", "name", "team_code", "position", "stat_formatted", "type_formatted"]
(leaders.select([c for c in cols if c in leaders.columns]).head(10)
 if leaders is not None and leaders.height else "leaders unavailable (offseason?)")
```

### Recipe 6 — Who's hot, who's not 🌡️

Standings come back unsorted-ish — one `sort` on `points` puts the contenders
on top and the lottery teams on the bottom. We also add a quick goal-differential
column so you can see *why* a team is where it is.


```python
st = safe("AHL standings", lambda: ahl.ahl_standings(season=ahl.most_recent_ahl_season()))
if st is not None and st.height and {"points", "goals_for", "goals_against"}.issubset(st.columns):
    ranked = (
        st.with_columns(
            goal_diff=(pl.col("goals_for").cast(pl.Int64, strict=False)
                       - pl.col("goals_against").cast(pl.Int64, strict=False))
        )
        .sort(pl.col("points").cast(pl.Int64, strict=False), descending=True)
        .select([c for c in ["team", "points", "goals_for", "goals_against", "goal_diff"]
                 if c in st.columns or c == "goal_diff"])
    )
    out = ranked.head()
else:
    out = "standings unavailable"
out
```

### Recipe 7 — A player's career stat line 📊

Grab any `player_id` (the leaderboard is a handy source) and
[`whl_player_stats`](../ahl/reference/additional.md#ahl_player_stats) returns every season they've played —
regular season, playoffs and exhibition, tagged by `stat_type`.


```python
wlead = safe("WHL leaders", lambda: whl.whl_leaders(season=whl.most_recent_whl_season()))
if wlead is not None and wlead.height and "player_id" in wlead.columns:
    pid = int(wlead["player_id"][0])
    who = wlead["name"][0] if "name" in wlead.columns else pid
    stats = safe(f"WHL stats for {who}", lambda: whl.whl_player_stats(player_id=pid))
    cols = ["season_name", "team_name", "games_played", "goals", "assists", "points", "stat_type"]
    out = (stats.select([c for c in cols if c in stats.columns]).head()
           if stats is not None and stats.height else "no career rows")
else:
    out = "leaders unavailable to source a player_id"
out
```

### Recipe 8 — The box score: goals & three stars 🌟

[`ohl_game_summary`](../ahl/reference/additional.md#ahl_game_summary) returns a **dict of frames** —
`game`, `goals`, `penalties`, `shots_by_period` and `three_stars`. Here's the
scoring summary plus the post-game stars for one game.


```python
osched = safe("OHL schedule", lambda: ohl.ohl_schedule(season=ohl.most_recent_ohl_season()))
ogid = latest_game_id(osched)
if ogid is not None:
    summ = safe(f"OHL game summary {ogid}", lambda: ohl.ohl_game_summary(game_id=ogid))
    if summ is not None:
        goals = summ.get("goals")
        gcols = ["period_id", "time", "goal_scorer_team_code",
                 "goal_scorer_first_name", "goal_scorer_last_name", "power_play", "empty_net"]
        out = (goals.select([c for c in gcols if c in goals.columns]).head()
               if goals is not None and goals.height else "no goals frame")
    else:
        out = "summary unavailable"
else:
    out = "no OHL game_id available"
out
```

And the three-star selections from the same dict — no extra call needed:


```python
if ogid is not None and 'summ' in dir() and summ is not None and summ.get("three_stars") is not None:
    stars = summ["three_stars"]
    scols = ["first_name", "last_name", "jersey_number", "home"]
    out = (stars.select([c for c in scols if c in stars.columns])
           if stars.height else "no three-star data for this game")
else:
    out = "summary unavailable"
out
```

### Recipe 9 — Slice the play-by-play: just the goals ⛳

The play-by-play frame carries a boolean `goal` flag, so pulling the scoring
plays (with scorer names and the period clock) is a one-line filter.


```python
if gid is not None:
    pbp9 = safe(f"AHL pbp {gid}", lambda: ahl.ahl_pbp(game_id=gid))
    if pbp9 is not None and pbp9.height and "goal" in pbp9.columns:
        goals_only = pbp9.filter(pl.col("goal") == True)  # noqa: E712 -- polars boolean
        cols = ["period_of_game", "clock", "team_id",
                "player_name_first", "player_name_last"]
        out = (goals_only.select([c for c in cols if c in goals_only.columns])
               if goals_only.height else "no goals parsed for this game")
    else:
        out = "play-by-play unavailable"
else:
    out = "no game_id available"
out
```

### Recipe 10 — Head-to-head history 🤝

The schedule is just a frame, so a rivalry view is two `str.contains` filters.
Pick the most-frequent home team, then every game where its top opponent
visited — final scores included.


```python
if sched is not None and sched.height and "home_team" in sched.columns:
    top_home = sched["home_team"].value_counts(sort=True)["home_team"][0]
    vs = sched.filter(pl.col("home_team") == top_home)
    top_away = vs["away_team"].value_counts(sort=True)["away_team"][0] if vs.height else None
    h2h = vs.filter(pl.col("away_team") == top_away) if top_away else vs
    print(f"{top_home} hosting {top_away}:")
    out = h2h.select([c for c in ["game_date", "home_team", "away_team", "home_score", "away_score"]
                      if c in h2h.columns]).head()
else:
    out = "schedule unavailable"
out
```

### Recipe 11 — Scout the roster: shooters & positions 🔍

The roster frame is loaded with scouting fields. Here we break a team down by
handedness (`shoots`) — a quick way to eyeball a power-play unit — straight
from [`whl_team_roster`](../ahl/reference/additional.md#ahl_team_roster).


```python
rteams = safe("WHL teams", lambda: whl.whl_teams(season=whl.most_recent_whl_season()))
if rteams is not None and rteams.height and "team_id" in rteams.columns:
    rtid = int(rteams["team_id"][0])
    ros = safe(f"WHL roster {rtid}", lambda: whl.whl_team_roster(team_id=rtid))
    if ros is not None and ros.height and "shoots" in ros.columns:
        out = ros.group_by("shoots").len().sort("len", descending=True).rename({"len": "players"})
    elif ros is not None and ros.height:
        out = ros.head()
    else:
        out = "roster empty for this team/season"
else:
    out = "teams unavailable"
out
```

### Recipe 12 — Hand off to pandas 🐼

Every accessor takes `return_as_pandas=True`, so the whole toolkit drops
straight into a pandas workflow. And [`ahl_season_id`](../ahl/reference/additional.md#ahl_season_id)
lists every season with its end-year and game-type label — handy for pinning
an exact `season_id` instead of guessing the end year.


```python
seasons = safe("AHL season_id", lambda: ahl.ahl_season_id(return_as_pandas=True))
if seasons is not None and len(seasons):
    keep = [c for c in ["season_id", "season_name", "season_yr", "game_type_label"]
            if c in seasons.columns]
    recent = seasons[keep].sort_values("season_id", ascending=False).head()
    print(type(recent).__module__)  # -> pandas.core.frame
    out = recent
else:
    out = "season list unavailable"
out
```

## 🥅 On-ice analytics

Beyond the box score, the package derives advanced metrics from the same
play-by-play feed:

| Function | Metric |
|---|---|
| [`ahl_game_corsi`](../ahl/reference/additional.md#ahl_game_corsi) | Corsi / Fenwick shot-attempt share |
| [`ahl_game_shifts`](../ahl/reference/additional.md#ahl_game_shifts) | shift charts (who's on the ice) |
| [`ahl_player_toi`](../ahl/reference/additional.md#ahl_player_toi) | time-on-ice per player |
| [`ahl_leaders`](../ahl/reference/additional.md#ahl_leaders) | statistical leaders |

Heads-up: shift-derived metrics (`*_game_shifts`, `*_player_toi`,
`*_game_corsi`) depend on the league publishing a shift feed for that game —
when it's absent you'll get an empty frame rather than an error, which is why
the recipes above guard on `height` before using them. 🛟

## 🎉 Where to next

- The same `<league>_*` calls work for **`ahl`**, **`ohl`**, **`whl`** and
  **`qmjhl`** — just swap the module.
- Women's pro hockey? See the dedicated **PWHL** tutorial.
- Full reference: the **AHL / OHL / WHL / QMJHL** pages in the sidebar.
- Override a league's public key only if it rotates: `SDV_<LEAGUE>_API_KEY`.

Now go find the next first-overall pick! 🏒
