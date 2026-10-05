---
title: "NBA — additional Python functions — Player"
sidebar_label: "Player"
sidebar_position: 7
description: "NBA — additional Python functions — Player — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Player

### player_play_context {#player_play_context}

`player_play_context(possessions: 'pl.DataFrame', *, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-player offensive On/Off Play-Context table (CTG's On/Off page, offense half).

For each player: their team's offensive play-context **with them on the floor**
(`on_*`), **without them** (`off_*`), and the on-minus-off difference
(`diff_*`) — which is the number CTG actually displays.

The OFF side is derived by **subtraction** (team total minus on-court), not by a
second scan. That is deliberate: it makes the partition exact by construction —
`on_poss + off_poss == team_poss` and the same for points — so a leak (a
double-counted possession, a dropped lineup slot) is impossible to hide. The
test suite asserts that identity directly.

Like CTG's on/off, this is a **raw** split: no luck adjustment, no opponent
adjustment, no minutes threshold. It is a descriptive difference, not a causal
estimate — for that, use the RAPM surface
(`~sportsdataverse.nba.nba_rapm.nba_rapm`).

Requires `off_player_1..5` from
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context` **with lineups attached**. |
| `league_non_transition_ppp` | `Optional[float]` | `None` | Pts+/Poss baseline; see `team_play_context`. One baseline is shared across the on and off sides so the diffs are comparable. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time / heave / non-counting possessions first. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player, team) with `PLAYER_PLAY_CONTEXT_SCHEMA`. Empty input returns a zero-row frame with that schema.

**Example**

```python
poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
onoff = player_play_context(poss)
print(onoff.sort("diff_pts_per_100", descending=True).head())

# Who makes their team run?

print(onoff.sort("diff_transition_freq", descending=True).head())
```

### player_rates {#player_rates}

`player_rates(box_logs: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-player per-minute rate stats from box logs.

Rows with null minutes (DNPs) are dropped. Rate = total stat / total
minutes across the player's games; `minutes_pg` is the mean minutes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_logs` | `DataFrame` |  | Per-player-per-game frame with `player_id, team_id, minutes, pts, reb, ast, fg3m`. |

**Returns**

One row per player: `player_id, team_id, games, minutes_pg, pts_per_min, reb_per_min, ast_per_min, fg3m_per_min`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.nba.nba_player_props import player_rates
rates = player_rates(box_logs)
```

### players_on_court_from_pbp {#players_on_court_from_pbp}

`players_on_court_from_pbp(enhanced_pbp: 'pl.DataFrame', raw_box: 'dict', *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'`

Reconstruct the 5-on-5 on-court lineup from pbp subs + boxscore starters.

Pure function (no network). A gamerotation-free alternative to
`players_on_court_from_rotation` returning the identical
`LINEUPS_SCHEMA` frame (one row per action, slots sorted ascending or
`None`). See the module design for the algorithm.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `enhanced_pbp_from_payload`. Must carry `game_id`, `action_number`, `order_index`, `period`, `team_id`, `person_id`, `description`, `is_substitution`. |
| `raw_box` | `dict` |  | Raw `boxscoretraditionalv3` dict (starters + name map). |
| `home_team_id` | `int` |  | Home team id (from `boxscore_home_away`). |
| `away_team_id` | `int` |  | Away team id (from `boxscore_home_away`). |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA`. Empty input returns a zero-row frame (never raises).

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, players_on_court_from_pbp,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
oc = players_on_court_from_pbp(enh, box, home_team_id=home, away_team_id=away)
print(oc.shape)
```

### players_on_court_from_quarter_boxscores {#players_on_court_from_quarter_boxscores}

`players_on_court_from_quarter_boxscores(enhanced_pbp: 'pl.DataFrame', period_boxscores: 'Dict[int, dict]', raw_box: 'Optional[dict]' = None, *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'`

Reconstruct the 5-on-5 on-court lineup, seeding each period exactly where possible.

A structural sibling of `players_on_court_from_pbp` — same
`LINEUPS_SCHEMA` output, same sub-batching walk / ffill-bfill / ascending
sort tail — whose only difference is *how each period is seeded*: when
that period's range-boxscore (period_box_oncourt`) narrows to
exactly 5 on-court candidates for a team, the period is seeded EXACTLY
from it; otherwise it falls back to the same gamerotation-free
first-appearance inference `players_on_court_from_pbp` uses
(period_starters`, carrying the prior period's ending lineup as
the silent-starter fallback). See period_box_oncourt` for the
narrowing recipe (empirically re-derived against pbpstats'
`StartOfPeriod._get_starters_from_boxscore_request` — see that
function's docstring for the concrete evidence behind its zero-sentinel
polarity).

Substitution name resolution merges up to three sources via
merge_name_maps`: name_map_from_period_boxes` (the union
of every period's range-box roster), name_map_from_pbp_actors`
(every row's own actor identity — covers bench players who never touch a
period boundary but do record at least one action), and — when the
caller supplies it — boxscore_name_map` over the full-game
`raw_box` payload, the SAME full-roster source
`players_on_court_from_pbp` uses. That third source is what fixes
the one residual name-resolution gap the first two cannot cover: a
player who is subbed in and then records **zero** further pbp actions for
the rest of the game (so never appears in name_map_from_pbp_actors`)
and never happens to be on court at an exact period-opening tick (so
never appears in name_map_from_period_boxes`) is still present in the
full-game boxscore roster — which lists every player on both teams
regardless of playing time — and therefore still resolvable. Passing
`raw_box` is optional (`None` preserves the pre-existing two-source
behavior) but strongly recommended: without it this producer's per-game
agreement with the gamerotation oracle can regress well below
`players_on_court_from_pbp`'s own floor on a fixture with a
late, stat-less bench appearance (see
`tests/nba/test_nba_lineups.py::test_quarter_box_agreement_floors`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. Must carry `game_id`, `action_number`, `order_index`, `period`, `team_id`, `person_id`, `player_name`, `player_name_i`, `description`, `is_substitution`. |
| `period_boxscores` | `Dict[int, dict]` |  | `{period: raw_boxscoretraditionalv3_range_payload}` — one entry per period, captured at that period's period_start_range` window. A missing period key falls back to pbp seeding for that period only (never raises). |
| `raw_box` | `Optional[dict]` | `None` | Optional raw full-game `boxscoretraditionalv3` payload (the same one `players_on_court_from_pbp` and `boxscore_home_away` consume) — supplies the full-roster name map described above. `None` (default) falls back to resolving names from `period_boxscores` + pbp actors only. |
| `home_team_id` | `int` |  | Home team id (from `boxscore_home_away`). |
| `away_team_id` | `int` |  | Away team id (from `boxscore_home_away`). |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA`. Empty `enhanced_pbp` returns a zero-row frame (never raises).

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, players_on_court_from_quarter_boxscores,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
periods = json.loads(pathlib.Path("boxv3_periods.json").read_text())
period_boxscores = {int(k): v for k, v in periods.items()}
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
oc = players_on_court_from_quarter_boxscores(
    enh, period_boxscores, box, home_team_id=home, away_team_id=away
)
print(oc.shape)
```

### players_on_court_from_rotation {#players_on_court_from_rotation}

`players_on_court_from_rotation(enhanced_pbp: 'pl.DataFrame', rotation: 'dict[str, list[dict]]', *, home_team_id: 'int', away_team_id: 'int') -> 'pl.DataFrame'`

Reconstruct the 5-on-5 on-court lineup via the rotation (gamerotation) algorithm.

Pure function — no network calls.  Port of hoopR's `.players_on_court_v3()`
(R/nba_stats_pbp.R lines 857-1041).

The rotation dict may use either `"HomeTeam"`/`"AwayTeam"` or
`"homeTeam"`/`"awayTeam"` as keys — both are accepted.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Output of `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. Must contain `game_id`, `action_number`, `period`, `seconds_remaining`, `is_substitution`, and `team_id`. |
| `rotation` | `dict[str, list[dict]]` |  | Parsed rotation dict, typically from `parse_rotation_resultsets`. Each team's list contains stint dicts with numeric `PERSON_ID`, `IN_TIME_REAL`, `OUT_TIME_REAL`. |
| `home_team_id` | `int` |  | Integer team ID of the home team. |
| `away_team_id` | `int` |  | Integer team ID of the away team. |

**Returns**

`polars.DataFrame` conforming to `LINEUPS_SCHEMA` with one row per action in *enhanced_pbp* (same row count, same ordering). Never raises — empty/malformed rotation returns a zero-row frame.

**Example**

```python
import json, pathlib
import polars as pl
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away, parse_rotation_resultsets,
    players_on_court_from_rotation,
)
box = json.loads(pathlib.Path("boxscoretraditionalv3.json").read_text())
pbp = json.loads(pathlib.Path("playbyplayv3.json").read_text())
rot = json.loads(pathlib.Path("gamerotation.json").read_text())
enh = enhanced_pbp_from_payload(pbp)
home, away = boxscore_home_away(box)
rotation = parse_rotation_resultsets(rot)
df = players_on_court_from_rotation(
    enh, rotation, home_team_id=home, away_team_id=away
)
print(df.shape)
```
