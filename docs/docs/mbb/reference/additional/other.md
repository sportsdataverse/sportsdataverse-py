---
title: "MBB — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 10
description: "MBB — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Other

### mbb_strength_of_schedule {#mbb_strength_of_schedule}

`mbb_strength_of_schedule(seasons: 'list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Season-level SoS / Quad / WAB résumé from the released ESPN data.

Loads the schedule + team boxscores, builds the opponent-adjusted ratings,
and applies `strength_of_schedule` per season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to compute (e.g. `[2024]`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (season, team_id) -- see `strength_of_schedule`.

**Example**

```python
from sportsdataverse.mbb import mbb_strength_of_schedule
resume = mbb_strength_of_schedule([2024])

# Pipeline next step (one line)

resume.sort("wab", descending=True).head(20)
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### strength_of_schedule {#strength_of_schedule}

`strength_of_schedule(results: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Per-team SoS + Quad 1-4 record + WAB from completed games and ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | Completed games with `game_id, season, home_team_id, away_team_id, home_score, away_score, neutral_site`. |
| `ratings` | `DataFrame` |  | One row per team with `season, team_id, adj_em, rank` (the `mbb_team_ratings` output). Team-id dtype must match `results`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (quad thresholds, HFA, bubble EM). |

**Returns**

One row per (season, team_id): `season, team_id, sos, sos_rank, wab, quad1_w .. quad4_l, quality_wins`. `sos` is the mean opponent `adj_em` (rank 1 = hardest schedule); quads follow the NET venue-adjusted opponent-rank thresholds; `quality_wins` is Quad-1 + Quad-2 wins; `wab` is actual wins minus a bubble-quality team's expected wins against the same schedule. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule
resume = strength_of_schedule(results, ratings)
```

### has_kenpom_login {#has_kenpom_login}

`has_kenpom_login() -> 'bool'`

Whether KenPom credentials are set in the environment.

The Python counterpart of hoopR's `has_kp_user_and_pw()`; gates a live
test without attempting a login.

**Returns**

`True` when both an e-mail and a password resolve from the environment.

**Example**

```python
import pytest
from sportsdataverse.mbb import has_kenpom_login

pytestmark = pytest.mark.skipif(not has_kenpom_login(), reason="no KenPom login")
```

### kenpom_login {#kenpom_login}

`kenpom_login(email: 'Optional[str]' = None, password: 'Optional[str]' = None, *, proxy: 'Any' = None) -> 'requests.Session'`

Log into kenpom.com and return the authenticated session.

The Python counterpart of hoopR's `login()`. Calling this directly is
optional -- every wrapper logs in on demand and reuses a cached session --
but it is the fastest way to verify credentials or a proxy before a long
pull, and the returned session can be passed to a wrapper as `session=`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `email` | `Optional[str]` | `None` | KenPom account e-mail. Falls back to `KENPOM_EMAIL` / `KP_USER` / `SDV_PY_KENPOM_EMAIL`. |
| `password` | `Optional[str]` | `None` | KenPom password. Falls back to `KENPOM_PW` / `KP_PW` / `SDV_PY_KENPOM_PW`. |
| `proxy` | `Any` | `None` | Proxy URL `str` or `requests` `proxies=` `dict`. Falls back to `SDV_PY_KENPOM_PROXY` then `SDV_PY_PROXY`. |

**Returns**

An authenticated `requests.Session` carrying the subscription cookie and the resolved proxy.

**Example**

```python
from sportsdataverse.mbb import kenpom_login

session = kenpom_login(proxy="http://user:pw@proxy.example:8080")
```
