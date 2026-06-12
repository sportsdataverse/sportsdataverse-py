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
| [`ahl_player_stats`](../ahl/reference/additional.md#ahl_player_stats) | A player's stat line |
| [`ahl_leaders`](../ahl/reference/additional.md#ahl_leaders) | Statistical leaders |
| [`ahl_game_summary`](../ahl/reference/additional.md#ahl_game_summary) | Box-score summary for a game |
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
cols = ["game_id", "game_date", "home_team", "away_team", "home_goal_count", "visiting_goal_count"]
(sched.select([c for c in cols if c in sched.columns]).head()
 if sched is not None else "schedule unavailable")
```

## 🍳 Cookbook: common hockey tasks

The shared surface makes these recipes work the same in every league — just
swap the module.

### Recipe 1 — Standings table 🏆

[`ohl_standings`](../ahl/reference/additional.md#ahl_standings) (shown for the OHL) gives one row per team.


```python
standings = safe("OHL standings", lambda: ohl.ohl_standings(season=ohl.most_recent_ohl_season()))
standings.head() if standings is not None else "standings unavailable"
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
    out = roster.head() if roster is not None else teams.head()
else:
    out = "teams unavailable"
out
```

### Recipe 3 — A game's play-by-play + shot attempts 📈

Take a `game_id` from the schedule, then [`ahl_pbp`](../ahl/reference/additional.md#ahl_pbp) for events and
[`ahl_game_corsi`](../ahl/reference/additional.md#ahl_game_corsi) for Corsi/Fenwick — derived from the same feed.


```python
if sched is not None and sched.height:
    gid = int(sched["game_id"][0])
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
import polars as pl
pl.DataFrame(rows)
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


## 🎉 Where to next

- The same `<league>_*` calls work for **`ahl`**, **`ohl`**, **`whl`** and
  **`qmjhl`** — just swap the module.
- Women's pro hockey? See the dedicated **PWHL** tutorial.
- Full reference: the **AHL / OHL / WHL / QMJHL** pages in the sidebar.
- Override a league's public key only if it rotates: `SDV_<LEAGUE>_API_KEY`.

Now go find the next first-overall pick! 🏒
