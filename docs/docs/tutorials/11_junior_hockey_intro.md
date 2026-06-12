---
title: Junior & minor hockey tutorial
sidebar_label: Junior & minor hockey
sidebar_position: 11
---

# Junior & minor hockey - `sportsdataverse-py`

`sportsdataverse` wraps the **HockeyTech / LeagueStat** feed behind the
American Hockey League (**AHL**) and the three Canadian Hockey League
major-junior loops - Ontario (**OHL**), Western (**WHL**) and Quebec
Maritimes (**QMJHL**). All four share **one module shape**, so once you
know one you know all four.

| League | Module | Level |
|---|---|---|
| AHL | `sportsdataverse.ahl` | Minor pro |
| OHL | `sportsdataverse.ohl` | CHL major junior |
| WHL | `sportsdataverse.whl` | CHL major junior |
| QMJHL | `sportsdataverse.qmjhl` | CHL major junior |

**No API key needed** - the public HockeyTech client keys ship with the
package (override a league's key with `SDV_<LEAGUE>_API_KEY` if one rotates).

## Setup

```sh
pip install sportsdataverse
# or
uv add sportsdataverse
```


```python
import sportsdataverse.ahl as ahl
import sportsdataverse.ohl as ohl
import sportsdataverse.whl as whl
import sportsdataverse.qmjhl as qmjhl

LEAGUES = {"ahl": ahl, "ohl": ohl, "whl": whl, "qmjhl": qmjhl}
```

## One shape, four leagues

Every league module exposes the same `<league>_*` surface - schedule,
standings, teams, rosters, play-by-play, player stats, leaders, plus
on-ice analytics (Corsi, shifts, TOI). Here is the AHL surface; OHL / WHL /
QMJHL are identical with their own prefix.


```python
[fn for fn in dir(ahl) if fn.startswith("ahl_")]
```

Live junior/minor feeds are seasonal and occasionally rate-limited, so a
tiny helper runs each call defensively - you get the frame when it is up and
a one-line note when it is not, instead of a traceback.


```python
def safe(label, thunk):
    try:
        out = thunk()
        print(f"{label}: ok")
        return out
    except Exception as e:  # noqa: BLE001 -- demo resilience
        print(f"{label}: unavailable right now ({type(e).__name__}: {e})")
        return None
```

## Most recent season per league

Each module ships a `most_recent_<league>_season()` helper.


```python
seasons = {lg: safe(lg, getattr(mod, f"most_recent_{lg}_season")) for lg, mod in LEAGUES.items()}
seasons
```

## Schedule

`<league>_schedule(season=...)` returns one row per game with teams + result.
Because the surface is identical, the same call works across all four leagues.


```python
sched = safe("AHL schedule", lambda: ahl.ahl_schedule(season=ahl.most_recent_ahl_season()))
sched.shape if sched is not None else None
```


```python
cols = ["game_id", "game_date", "home_team", "away_team", "home_goal_count", "visiting_goal_count"]
(sched.select([c for c in cols if c in sched.columns]).head()
 if sched is not None else "schedule unavailable")
```

## Standings

`<league>_standings(season=...)` - one row per team.


```python
standings = safe("OHL standings", lambda: ohl.ohl_standings(season=ohl.most_recent_ohl_season()))
standings.head() if standings is not None else "standings unavailable"
```

## Teams & rosters

`<league>_teams(...)` lists teams; `<league>_team_roster(team_id=...)` a roster.


```python
teams = safe("WHL teams", lambda: whl.whl_teams(season=whl.most_recent_whl_season()))
teams.head() if teams is not None else "teams unavailable"
```

## Play-by-play & on-ice analytics

`<league>_pbp(game_id=...)` returns event-level play-by-play; the package also
derives `<league>_game_corsi`, `<league>_game_shifts` and `<league>_player_toi`
from the same feed. Grab a `game_id` from the schedule above, then:


```python
if sched is not None and sched.height:
    gid = int(sched["game_id"][0])
    pbp = safe(f"AHL pbp {gid}", lambda: ahl.ahl_pbp(game_id=gid))
    print(pbp.shape if pbp is not None else "pbp unavailable")
else:
    print("no schedule rows to pick a game_id from")
```

## Where to go next

- The same `<league>_*` calls work for **`ohl`**, **`whl`** and **`qmjhl`** -
  just swap the module.
- For the **PWHL** (women's pro), see the dedicated PWHL tutorial.
- Full reference: the **AHL / OHL / WHL / QMJHL** pages in the sidebar.
- Every accessor takes `return_as_pandas=True` for a pandas frame.
