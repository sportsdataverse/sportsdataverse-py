---
title: "MBB — additional Python functions — Play-by-play processing"
sidebar_label: "Play-by-play processing"
sidebar_position: 5
description: "MBB — additional Python functions — Play-by-play processing — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Play-by-play processing

### build_3p_shot_info {#build_3p_shot_info}

`build_3p_shot_info(p: 'LineupStatSet') -> 'OffLuckShotInfo3P'`

3P-only shot-decomposition wrapper.

Public port of `build3PShotInfo` (`LuckUtils.ts:741-759`) --
remaps build_shot_info`'s generic keys to the 3pm`/
3pa`/3p` suffixes used throughout the luck-adjustment engine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `p` | `LineupStatSet` |  | The player's `LineupStatSet`/`IndivStatSet`-shaped dict. |

**Returns**

`{"shot_info_ast_3pm", "shot_info_early_3pa", "shot_info_scramble_3pa", "shot_info_unast_3pm", "shot_info_unknown_3pM", "shot_info_total_3p"}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import build_3p_shot_info

info = build_3p_shot_info(player)
print(info["shot_info_total_3p"])
```

### build_adjusted_3p {#build_adjusted_3p}

`build_adjusted_3p(p: 'LineupStatSet', info: 'OffLuckShotInfo3P') -> 'OffLuckAdj3P'`

3P-only approx-unassisted/assisted-FG% wrapper.

Public port of `buildAdjusted3P` (`LuckUtils.ts:812-835`, "retained
for bwc [backwards compat]" per the upstream comment) -- a thin remap of
build_adjusted_fg` called with `shot_type="3p"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `p` | `LineupStatSet` |  | The (typically base-period) player dict driving `off_3p`/ `off_3p_ast`. |
| `info` | `OffLuckShotInfo3P` |  | An `build_3p_shot_info`-shaped dict (the "biggest sample available" per the upstream comment -- normally the base period, not the sample being luck-adjusted). |

**Returns**

`{"base3P", "unassisted3P", "assisted3P", "baseAssistPct"}`.

**Example**

```python
from sportsdataverse.mbb.mbb_luck import build_3p_shot_info, build_adjusted_3p

base_info = build_3p_shot_info(base_player)
adj = build_adjusted_3p(base_player, base_info)
print(adj["assisted3P"], adj["unassisted3P"])
```

### build_athlete_identity_lookup {#build_athlete_identity_lookup}

`build_athlete_identity_lookup(rosters: 'dict[int | str, dict]') -> 'dict[str, dict[str, Any]]'`

R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `dict[int \| str, dict]` |  | Mapping of team_id -> that team's raw roster payload (`wbb/team_rosters/json/{season}/{team_id}.json`). NOTE: R walks `raw$athletes` directly here (no position-bucket unwrap, unlike the rosters dataset itself). |

**Returns**

athlete_id (str) -> identity fields for `helper_wbb_player_season_stats`.

### build_available_team_list {#build_available_team_list}

`build_available_team_list(in_by_year: 'dict[str, list[tuple[TeamId, str, ConferenceId]]]') -> 'dict[ConferenceId, Callable[[str], str]]'`

Builds a per-conference team-index JSON fragment for

`cbb-on-off-analyzer` (`TeamIdParser.build_available_team_list`,
`TeamIdParser.scala:105-124`) -- the caller inserts the app-specific
index key to get the final JSON string.

See `build_lineup_cli_array`'s docstring for why this port doesn't
attempt to reproduce Scala's hash-map iteration order (both the
conference-level and, here, the team-level grouping) -- the upstream
oracle covering this ordering is itself permanently disabled.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_by_year` | `dict[str, list[tuple[TeamId, str, ConferenceId]]]` |  | Season-key (e.g. `"2018/9"`) -> that season's `(team, ncaa_id, conference)` triples, e.g. from repeated `get_team_triples` calls. |

**Returns**

Conference -> a function `index_key -> JSON-fragment string`, one `' "team": [ ... ],'` block per team in that conference (each block listing every season that team appeared in, in encounter order).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import build_available_team_list
from sportsdataverse.mbb.mbb_ncaa_models import ConferenceId, TeamId

by_year = {"2018/9": [(TeamId("Kentucky"), "450591", ConferenceId("SEC"))]}
build_available_team_list(by_year)[ConferenceId("SEC")]("test")
```

### build_base_event {#build_base_event}

`build_base_event(box_lineup: 'LineupEvent') -> 'ShotEvent'`

Fills in the fields a shot event can borrow straight from the

box-score lineup, leaving the shot-specific fields as overridable
placeholders (`ShotEventParser.build_base_event`, `:379-410`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event. |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.ShotEvent` with `date`/`location_type`/`team`/`opponent` populated and every other field at its Scala-literal placeholder default (`player=None`, `is_off=True`, `lineup_id=None`, `players=[]`, `score=Score(0, 0)`, `min=0.0`, `loc=ShotLocation(0.0, 0.0)`, `geo=ShotGeo(0.0, 0.0)`, `dist=0.0`, `pts=0`, `value=0`, `ast_by=None`, `is_ast=None`, `is_trans=None`, `raw_event=None`) -- every caller immediately overrides the placeholders it cares about via `dataclasses.replace`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import build_base_event
base = build_base_event(box_lineup)
```

### build_d_rtg {#build_d_rtg}

`build_d_rtg(stat_set: 'LineupStatSet | None', avg_efficiency: 'float', calc_diags: 'bool', override_adjusted: 'bool') -> 'tuple[dict[str, float] | None, dict[str, float] | None, dict[str, float] | None, dict[str, float] | None, DRtgDiagnostics | None]'`

Individual defensive rating (Dean-Oliver DRtg) + diagnostics.

Faithful port of `RatingUtils.buildDRtg` (`RatingUtils.ts:1252-1485`).
Mirrors `build_o_rtg`'s structure (`stat_get` closure,
`calc_diags`/`override_adjusted` flag pair, recursive
un-overridden raw-value pass) over the simpler
`(stat_set, avg_efficiency, calc_diags, override_adjusted)` 4-arg
signature (no roster/extra-team-stat args, confirmed against the TS).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_set` | `LineupStatSet \| None` |  | The player's stat dict. `None` returns an all-`None` 5-tuple (`RatingUtils.ts:1264-1265`'s `if (!statSet)` -- null/undefined only). Unlike `build_o_rtg`, an **empty dict computes cleanly** -- every division in `buildDRtg` is guard-ternary'd (see the module docstring's "Contrast" note), so `{}` does not raise `ZeroDivisionError`. |
| `avg_efficiency` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `calc_diags` | `bool` |  | When `True`, populate the 5th tuple slot (`DRtgDiagnostics`); otherwise it is `None`. |
| `override_adjusted` | `bool` |  | When `True`, apply build_def_overrides` to the raw opponent-FGM/points fields before computing, and additionally recurse once (with `calc_diags=False, override_adjusted=False`) to compute the un-overridden "raw" values for the 3rd/4th tuple slots. |

**Returns**

A 5-tuple `(d_rtg, adj_d_rtg, raw_d_rtg, raw_adj_d_rtg, d_rtg_diags)`: - `d_rtg`: `{"value": DRtg}` when `Opponent_Possessions_Box > 0`, else `None`. - `adj_d_rtg`: `{"value": Adj_DRtgPlus}` under the same guard. - `raw_d_rtg` / `raw_adj_d_rtg`: the un-overridden values from the recursive call when `override_adjusted=True`; `None` otherwise (unlike `build_o_rtg`, there is no internal-usage special case here -- the TS destructures only the first 2 slots of the recursive 5-tuple). - `d_rtg_diags`: the full `DRtgDiagnostics` dict (`None` unless `calc_diags=True`).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_d_rtg

d_rtg, adj_d_rtg, _, _, diags = build_d_rtg(player, 100.0, True, False)
print(d_rtg["value"], diags["dRtg"])

# Override-adjusted (manual 3P-defense-% override applied)

d_rtg2, adj_d_rtg2, raw_d_rtg2, raw_adj_d_rtg2, _ = build_d_rtg(
    player, 100.0, False, True,
)
```

### build_efficiency_margins {#build_efficiency_margins}

`build_efficiency_margins(mutable_stat_set: 'LineupStatSet', key_override: 'str | None' = None) -> 'None'`

Derive `off_net` / `off_raw_net` on a stat set, in place.

Faithful port of `LineupUtils.buildEfficiencyMargins` (`LineupUtils.ts:145`).
`off_net` is `off_adj_ppp - def_adj_ppp` (adjusted efficiency margin);
`off_raw_net` is `off_ppp - def_ppp` (raw/unadjusted margin). Both are
only written when their two source fields are both present on
`mutable_stat_set`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_stat_set` | `LineupStatSet` |  | The `LineupStatSet` (or team-report equivalent) to mutate in place. |
| `key_override` | `str \| None` | `None` | `"value"` or `"old_value"` -- which sub-key to read from the source fields and write into `off_net` / `off_raw_net`. When `None` (the default), the upstream `nonLuckKey` fallback applies: use `"old_value"` if `mutable_stat_set["off_ppp"]["old_value"]` is present, otherwise `"value"`. When given explicitly, the written field is merged onto any existing `off_net` / `off_raw_net` dict (so a second call with the other key preserves the first call's key) rather than replacing it outright. |

**Returns**

None. `mutable_stat_set` is mutated in place.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import build_efficiency_margins

build_efficiency_margins(team_info, "value")
off_ppp = team_info.get("off_ppp")
if isinstance(off_ppp, dict) and off_ppp.get("old_value") is not None:
    build_efficiency_margins(team_info, "old_value")
print(team_info["off_net"]["value"])
```

### build_exp_3p {#build_exp_3p}

`build_exp_3p(info: 'OffLuckShotTypeAndAdj3P') -> 'float'`

Expected made-3P count given a player's shot-type mix + shooting %s.

Public port of `buildExp3P` (`LuckUtils.ts:838-847`): `(assisted
3PM * assisted3P%) + (unassisted 3PM * unassisted3P%) +
(early/scramble/unknown 3PA * base3P%)`. Pure weighted sum -- no
division, so this introduces no landmine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `info` | `OffLuckShotTypeAndAdj3P` |  | A dict carrying both `build_3p_shot_info`'s `shot_info_*` keys and `build_adjusted_3p`'s `*3P` keys (i.e. an `OffLuckShotTypeAndAdj3P`). |

**Returns**

The expected number of made 3-pointers (`3P% * total 3P`).

**Example**

```python
from sportsdataverse.mbb.mbb_luck import (
    build_3p_shot_info, build_adjusted_3p, build_exp_3p,
)

base_info = build_3p_shot_info(base_player)
info = {**build_3p_shot_info(player), **build_adjusted_3p(base_player, base_info)}
expected_makes = build_exp_3p(info)
```

### build_lineup_cli_array {#build_lineup_cli_array}

`build_lineup_cli_array(in_triples: 'list[tuple[TeamId, str, ConferenceId]]') -> 'dict[ConferenceId, str]'`

Builds the per-conference team array for `lineups-cli.sh` files

(`TeamIdParser.build_lineup_cli_array`, `TeamIdParser.scala:94-100`).

**Iteration-order note (upstream-DISABLED context).** Scala's
`List.groupBy` returns an immutable `Map` whose iteration order is
hash-bucket-dependent, not insertion order -- the disabled oracle's
expected `Map.toList` ordering (`SEC` before `B1G`) reflects that
JVM-specific hashing, not a documented contract. This port uses a plain
`dict` (Python 3.7+ preserves insertion order), the natural pythonic
choice; since the upstream test asserting a specific cross-conference
order is itself permanently disabled (see the module docstring), there
is no live oracle to match here regardless of dict vs hash-map ordering.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_triples` | `list[tuple[TeamId, str, ConferenceId]]` |  | `(team, ncaa_id, conference)` triples, e.g. from `get_team_triples`. |

**Returns**

Conference -> newline-joined `" 'ncaa_id::URL-encoded team name'"` lines, one per team in that conference (in encounter order).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import build_lineup_cli_array
from sportsdataverse.mbb.mbb_ncaa_models import ConferenceId, TeamId

triples = [(TeamId("Penn St."), "1", ConferenceId("B1G"))]
build_lineup_cli_array(triples)[ConferenceId("B1G")]
# "   '1::Penn+St.'"
```

### build_lineup_id {#build_lineup_id}

`build_lineup_id(players: 'list[PlayerCodeId]') -> 'LineupId'`

Builds a lineup id from a list of players (`ExtractorUtils.scala:602-606`):

every player's `code`, sorted, joined with `"_"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerCodeId]` |  | The players on the floor for this lineup. |

**Returns**

The opaque `~sportsdataverse.mbb.mbb_ncaa_models.LineupId`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId
from sportsdataverse.mbb.mbb_ncaa_stints import build_lineup_id
build_lineup_id([PlayerCodeId("BbBob", PlayerId("Bob")), PlayerCodeId("AaAl", PlayerId("Al"))])
# LineupId("AaAl_BbBob")
```

### build_mbb_player_identity_lookup {#build_mbb_player_identity_lookup}

`build_mbb_player_identity_lookup(player_box: 'pl.DataFrame') -> 'dict[str, dict[str, Any]]'`

R `build_identity_lookup(season)`: athlete_id -> identity from the

season's already-compiled `player_box` -- the authoritative "who played
in season Y" source (ESPN's team-roster endpoint is current-only and
cannot answer that for historical seasons).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_box` | `DataFrame` |  | The season's compiled player_box frame (e.g. `nba/player_box/parquet/player_box_{season}.parquet`, or whatever the season builder just wrote for this pass). Must carry `athlete_id`; other identity columns are best-effort. |

**Returns**

athlete_id (str) -> identity fields for `helper_nba_player_season_stats`. When an athlete appears in multiple rows (multiple games), the LAST row (by frame order) wins -- mirroring R's `!duplicated(athlete_id, fromLast = TRUE)`, which keeps an athlete's most recent team within the season.

### build_mbb_season_wp {#build_mbb_season_wp}

`build_mbb_season_wp(season: 'int', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

A season's play-by-play with win-probability columns joined in.

Loads the season's play-by-play, schedule, and team boxscores, builds a
leakage-free weekly as-of pregame anchor per game, scores every play through
the bundled in-game win-probability artifact, and returns the full
`load_mbb_pbp` frame with `pregame_home_prob` + `home_win_prob`
appended -- the enrich-in-place shape that overwrites the season's
`play_by_play_<season>.parquet` release asset.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (e.g. `2024`); bounded by `load_mbb_pbp` release availability (`>= 2002`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the loaders + constants). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The season's `load_mbb_pbp` frame (every column preserved) with the two WP_COLS` appended (both `Float64`), sorted by `game_id` then `game_play_number`.

**Example**

```python
from sportsdataverse.mbb import build_mbb_season_wp
wp = build_mbb_season_wp(2024)
wp.select("game_id", "game_play_number", "home_win_prob").head()
```

### build_net_points {#build_net_points}

`build_net_points(player_rapm_and_poss_pct: 'LineupStatSet', ortg: 'ORtgDiagnostics', drtg: 'DRtgDiagnostics', avg_eff: 'float', scale_type: "Literal['T%', 'P%', '/G']", num_games: 'float' = 1, missing_game_adjustment: 'float' = 1) -> 'NetPoints'`

Decompose ORtg/DRtg + RAPM into a Net-Points-like breakdown.

Faithful port of `RatingUtils.buildNetPoints` (`RatingUtils.ts:1036-1234`).
Genuinely public upstream (called from `buildLeaderboards.ts`,
`PlayerImpactBreakdownTable.tsx`, and `ImpactBreakdownUtils.ts`), so
this port is public too.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_rapm_and_poss_pct` | `LineupStatSet` |  | The player's stat dict -- reads `off_team_poss_pct`/`def_team_poss_pct` (nullish-coalesced to `0.0`, see nullish`) and, when present, `off_adj_rapm`/`def_adj_rapm` (each a `{"value": float}` "Statistic"-shaped field) for the RAPM "with-or-without-you" (WOWY) deltas. |
| `ortg` | `ORtgDiagnostics` |  | An `ORtgDiagnostics` dict from `build_o_rtg` (`calc_diags=True`), typically with `adjPtsFactor`/ `adjPossFactor` overridden from their `1` default by a missing-possession correction. |
| `drtg` | `DRtgDiagnostics` |  | A `DRtgDiagnostics` dict from `build_d_rtg` (`calc_diags=True`). If it carries an `onBallDiags` key (this port's `build_d_rtg` never sets one -- see the module docstring's deferred-work note), the on-ball-adjusted branch is used instead of the base `dRtg`/`adjDRtgPlus`. |
| `avg_eff` | `float` |  | League/context average efficiency. |
| `scale_type` | `Literal['T%', 'P%', '/G']` |  | `"T%"` (scale by on-floor team-possession share, `avgEff`-adjusted possession count), `"P%"` (scale to 100 possessions), or `"/G"` (scale to per-game). |
| `num_games` | `float` | `1` | Divisor for the `"/G"` scale type. Default `1`. |
| `missing_game_adjustment` | `float` | `1` | Multiplier folded into the `"T%"` scale factor for imputed-missing-games correction. Default `1`. |

**Returns**

A `NetPoints` dict -- 20 keys, plus an optional `defNetPtsIndiv` 21st key present only when `drtg["onBallDiags"]` is set (TS-verbatim key names throughout).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_o_rtg, build_d_rtg, build_net_points

_, _, _, _, o_diags = build_o_rtg(player, {}, {}, 100.0, True, False)
_, _, _, _, d_diags = build_d_rtg(player, 100.0, True, False)
net_pts = build_net_points(player, o_diags, d_diags, 100.0, "T%")
print(net_pts["offNetPts"], net_pts["defNetPts"])
```

### build_new_player_list {#build_new_player_list}

`build_new_player_list(curr: 'LineupEvent', prev: 'LineupEvent') -> 'list[PlayerCodeId]'`

Builds a player list from the previous (or current, if pre-initialized)

lineup and the current lineup's in/out subs (`ExtractorUtils.scala:654-693`).

Three candidate reconciliations are computed --

* `poss1`: `prev.players` minus everyone in `curr.players_out`,
  plus everyone in `curr.players_in` (subs-out removed first, then
  subs-in merged on top).
* `poss2`: `prev.players` plus `curr.players_in`, minus everyone in
  `curr.players_out` (subs-in merged first, then subs-out removed).
* `poss3`: just `curr.players_in`.

-- and whichever has exactly 5 players wins (checked in `poss3`,
`poss1`, `poss2` order); if none does, a common play-by-play error
(a player appearing in both the in- and out- lists for the same sub
event) is corrected by dropping the common players from both sides
before reconciling via the `poss1` recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr` | `LineupEvent` |  | The lineup event whose `players_in`/`players_out` describe the subs to apply. |
| `prev` | `LineupEvent` |  | The lineup event whose `players` is the starting roster (complete_lineup` passes `curr` for both arguments -- see its docstring). |

**Returns**

The reconciled player list, sorted by `code`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import build_new_player_list
build_new_player_list(curr_lineup_event, prev_lineup_event)
```

### build_o_rtg {#build_o_rtg}

`build_o_rtg(stat_set: 'LineupStatSet | None', roster_stats_by_code: 'dict[str, LineupStatSet] | None', extra_team_stat_info: 'LineupStatSet', avg_efficiency: 'float', calc_diags: 'bool', override_adjusted: 'bool') -> 'tuple[dict[str, float] | None, dict[str, float] | None, dict[str, float] | None, dict[str, float] | None, ORtgDiagnostics | None]'`

Individual offensive rating (Dean-Oliver ORtg) + diagnostics.

Faithful port of `RatingUtils.buildORtg` (`RatingUtils.ts:398-960`).
See the module docstring for the signature-vs-brief note (this mirrors
the TS 6-positional-arg / 5-tuple contract verbatim, snake_cased) and
the diagnostics-dict key-naming convention (TS-verbatim, not
snake_cased).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_set` | `LineupStatSet \| None` |  | The player's `LineupStatSet` (ES-aggregation-shaped per-player doc, "IndivStatSet" upstream). `None` returns an all-`None` 5-tuple (`RatingUtils.ts:412-413`'s `if (!statSet)` -- null/undefined only). An **empty dict does NOT short-circuit** (`{}` is truthy in JS and falls through to compute upstream); in this port it falls into unguarded-division landmine 1 and raises `ZeroDivisionError` where the TS degrades to a NaN-laced degenerate result -- see the module docstring's landmine list. |
| `roster_stats_by_code` | `dict[str, LineupStatSet] \| None` |  | `{player_code: LineupStatSet}` for every player on the roster -- used for the approximate team-ORB apportionment and the per-shot-location assisted-eFG fallback. `None` is treated as `{}` (every vendored jest call passes a literal `{}`). |
| `extra_team_stat_info` | `LineupStatSet` |  | `{"total_off_to": {...}, "sum_total_off_to": {...}}` -- team-level TOV bookkeeping used to compute "unblamed" team turnovers apportioned by `off_team_poss_pct`. |
| `avg_efficiency` | `float` |  | League/context average efficiency (`100` in every vendored jest call). |
| `calc_diags` | `bool` |  | When `True`, populate the 5th tuple slot (`ORtgDiagnostics`); otherwise it is `None`. |
| `override_adjusted` | `bool` |  | When `True`, apply build_off_overrides` to the raw made/attempt/turnover fields before computing, and additionally recurse once (with `calc_diags=False, override_adjusted=False`) to compute the un-overridden "raw" values for the 3rd/4th tuple slots. |

**Returns**

A 5-tuple `(o_rtg, adj_o_rtg, raw_o_rtg, raw_adj_o_rtg, o_rtg_diags)`: - `o_rtg`: `{"value": ORtg}` when `TotPoss > 0`, else `None`. - `adj_o_rtg`: `{"value": Adj_ORtgPlus}` when `TotPoss > 0`, else `None`. - `raw_o_rtg`: when `calc_diags or override_adjusted`, the un-overridden `ORtg` (`None` if `override_adjusted=False`, since no un-overridden pass was computed); otherwise a special internal-recursion value `{"value": usage}` (`RatingUtils.ts:835`'s "if called internally return usage here" case). - `raw_adj_o_rtg`: the un-overridden `adj_o_rtg` (`None` when `override_adjusted=False`). - `o_rtg_diags`: the full `ORtgDiagnostics` dict (`None` unless `calc_diags=True`).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_o_rtg

o_rtg, adj_o_rtg, _, _, diags = build_o_rtg(
    player, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
    100.0, True, False,
)
print(o_rtg["value"], diags["oRtg"])

# Override-adjusted (manual shooting-% overrides applied)

o_rtg2, adj_o_rtg2, raw_o_rtg2, raw_adj_o_rtg2, _ = build_o_rtg(
    player, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
    100.0, False, True,
)
```

### build_partial_lineup_list {#build_partial_lineup_list}

`build_partial_lineup_list(reversed_partial_events: 'Iterable[PlayByPlayEvent]', box_lineup: 'LineupEvent') -> 'list[LineupEvent]'`

Converts a stream of partially parsed events into a list of lineup

events (`ExtractorUtils.scala:118-227`).

`box_lineup` is expected to carry every player on the team's roster,
with the top 5 (by whatever order the caller supplies) being the
starters. The events are first reordered into forward-chronological
order via `reorder_and_reverse`, then folded through a
`LineupBuildingState`:

* A `SubIn`/`SubOut` event either **opens a new stint** (if the
  current lineup `~LineupBuildingState.is_active`: the just-built
  lineup is completed via complete_lineup` and appended, and a
  fresh lineup is started via new_lineup_event`) or **keeps
  accumulating** onto the current (not-yet-active) lineup via
  `~LineupBuildingState.with_player_in`/`with_player_out`. A
  `SubIn` event naming literally `"team"` (case-insensitive) is
  always a no-op, win or lose the active check; `SubOut` has no such
  exemption. Every sub name is resolved through
  `~sportsdataverse.mbb.mbb_ncaa_names.tidy_player` first.
* The **old/new play-by-play format** is latched (once, forever) the
  first time a sub name is seen: an all-caps name (no lowercase letters
  at all) means the old (pre-2018-ish) format; this only ever updates on
  a sub-event branch, never on a `GameBreakEvent`.
* `OtherTeamEvent`/`OtherOpponentEvent` accumulate onto the current
  lineup via `~LineupBuildingState.with_team_event`/
  `with_opponent_event` plus `~LineupBuildingState.with_latest_score`.
* `GameBreakEvent` (half/quarter/OT boundary short of the game's end)
  completes the current lineup and starts a fresh one -- but whether
  that fresh lineup **resets to the starting 5** or **carries over** the
  just-completed lineup's players depends on the (possibly still
  unlatched) `old_format` flag: old format resets to
  `starters_only`, new format (2018+, the default once
  `box_lineup.team.year.value >= 2018` if never latched) carries over.
* `GameEndEvent` only completes the current lineup (no new one is
  started, and it is not appended to `prev` here -- `LineupBuildingState.build`
  folds it in as the trailing entry).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `reversed_partial_events` | `Iterable[PlayByPlayEvent]` |  | The full play-by-play event stream for one team's box-score lineup, in reverse-chronological order. |
| `box_lineup` | `LineupEvent` |  | The team's roster lineup event (`players` is the full roster; the first 5 are the starters). |

**Returns**

The chronological list of lineup (stint) events.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import build_partial_lineup_list
stints = build_partial_lineup_list(reversed(events), box_lineup)
print(len(stints))
```

### build_player_code {#build_player_code}

`build_player_code(in_name: 'str', team: 'Optional[TeamId]') -> 'PlayerCodeId'`

Build a short player code from a name, in any of the NCAA formats

(`ExtractorUtils.scala:290-391`).

The code is a compact `FirstInitials + [Middle] + Lastname` string
(e.g. `"Mitchell, Makhi"` -> `"MiMitchell"`) unique within a team +
season, used to join play-by-play name fragments to box-score rosters.
Supported input shapes: `"First [Middle...] Last"` (no comma),
`"Last, First [Middle...]"`, and `"Last, Suffix, First"`.

The full name is first corrected via the team-scoped misspelling table
and diacritic-stripped -- that corrected string becomes the returned
`~sportsdataverse.mbb.mbb_ncaa_models.PlayerId`. Each fragment is
lowercased, de-dotted, individually misspelling-corrected, and truncated
to `PLAYER_CODE_MAX_FRAGMENT_LENGTH`. Junk fragments (jr/sr/roman
numerals/ordinals/digit-leading) are dropped -- except the first-name
fragment, which is never dropped for being short.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_name` | `str` |  | The raw player name as it appears in the source HTML. |
| `team` | `Optional[TeamId]` |  | The team, for team-scoped misspelling corrections; `None` uses only the generic corrections. |

**Returns**

A `PlayerCodeId` with the derived `code` and the corrected full name as `id` (`ncaa_id` is always `None` here).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code
pc = build_player_code("Mitchell, Makhi", None)
print(pc.code)  # "MiMitchell"

# Play-by-play (all-caps, truncated) form

build_player_code("BIGBY-WILLIAM,KAVELL", None).code  # "KaBigby-will"
```

### build_player_context {#build_player_context}

`build_player_context(players: 'list[PlayerOnOffStats]', lineups: 'list[LineupStatSet]', players_baseline: 'dict[PlayerId, IndivStatSet]', stats_averages: 'PureStatSet', avg_efficiency: 'float', agg_value_key: 'ValueKey' = 'value', config: 'RapmConfig' = {'prior_mode': -1, 'removal_pct': 0.06, 'fixed_regression': -1}) -> 'RapmPlayerContext'`

Build the context object the RAPM matrix-solve layer consumes.

Faithful port of `RapmUtils.buildPlayerContext` (`RapmUtils.ts:427-541`).
Removes low-possession players (`config["removal_pct"]` of total
on+off possessions), flags fully-removed lineups (mutating `lineups`
in place -- see the module docstring's landmine 5), builds the
player-to-column index, and folds `build_priors` into
`prior_info`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerOnOffStats]` |  | The per-player on/off splits (`PlayerOnOffStats`), e.g. `mbb_lineup_stats.lineup_to_team_report(...)["players"]`. |
| `lineups` | `list[LineupStatSet]` |  | The per-lineup `LineupStatSet` docs feeding this team's aggregate (**mutated in place** -- see landmine 5). |
| `players_baseline` | `dict[PlayerId, IndivStatSet]` |  | `{player_id: IndivStatSet}` -- forwarded to `build_priors` unchanged. |
| `stats_averages` | `PureStatSet` |  | League/context average stat set -- forwarded to `build_priors` unchanged. |
| `avg_efficiency` | `float` |  | League/context average efficiency. |
| `agg_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- forwarded to `build_priors` as its `value_key` (only affects prior calculations, not the lineup-filtering/aggregation above it). |
| `config` | `RapmConfig` | `{'prior_mode': -1, 'removal_pct': 0.06, 'fixed_regression': -1}` | Removal-percent / prior-mode / regression config. Defaults to `DEFAULT_RAPM_CONFIG`; never mutated by this function (only `config["removal_pct"]`/`config["prior_mode"]` are read), matching the TS default parameter's own read-only usage. |

**Returns**

A `RapmPlayerContext`.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report
from sportsdataverse.mbb.mbb_rapm import build_player_context, DEFAULT_RAPM_CONFIG

report = lineup_to_team_report({"lineups": buckets, "error_code": None})
ctx = build_player_context(
    report["players"], buckets, {}, {}, 100.0, "value", DEFAULT_RAPM_CONFIG
)
print(ctx["num_players"], ctx["team_info"]["off_poss"]["value"])

# Filtering lineups by side (the ``filtered_lineups`` closure)

off_lineups = ctx["filtered_lineups"]("off")
def_lineups = ctx["filtered_lineups"]("def")
```

### build_position {#build_position}

`build_position(confs: 'dict[str, float]', confs_no_height: 'dict[str, float] | None', player: 'dict[str, Any]', team_season: 'str') -> 'tuple[str, str]'`

Classify a player into a position label + diagnostic trace string.

Faithful port of `PositionUtils.buildPosition` (`PositionUtils.ts:401-580`)
-- the PG / s-PG / CG / WG / WF / S-PF / PF/C / C decision tree. A
`ABSOLUTE_POSITION_FIXES` manual override short-circuits the whole
tree (recursing once, with `team_season=""`, purely to compute the
diagnostic "what would this have been" string); otherwise the function
walks the confidence-threshold / assist-rate / 3PT-rate branch cascade,
applies the "too few effective possessions" (< 25) fallback, and
reconciles the result against roster metadata via `using_roster_pos`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `confs` | `dict[str, float]` |  | The 5-way positional confidence dict (`TRAD_POS_LIST` keys), typically the height-adjusted output of `build_position_confidences`. |
| `confs_no_height` | `dict[str, float] \| None` |  | The pre-height-adjustment confidences, or `None` when the caller has no height data. When present, a PG <-> s-PG flip caused solely by the height adjustment is reverted (the `maybeIgnoreHeight` closure, `ts:433-457`). The check is `is not None` (JS object-truthiness: an empty dict is still a truthy JS object), NOT a Python-falsy `if confs_no_height`. |
| `player` | `dict[str, Any]` |  | The player stat dict. Reads `key` (override lookup), `off_assist` / `off_3pr` / `off_usage` / `off_team_poss` (each `{"value": N}`-wrapped), and `roster` (a plain `{"pos": ..., "role": ...}` dict of un-wrapped strings). |
| `team_season` | `str` |  | `"{sport}_{team}_{season}"` key into `ABSOLUTE_POSITION_FIXES`. Pass `""` to disable override lookup for a given call (the recursive diagnostic call inside the override branch does exactly this). |

**Returns**

A `(position, diagnostic)` tuple. `position` is one of `ID_TO_POSITION`'s keys; `diagnostic` is a human-readable trace of which rule fired, byte-identical to the TS's template strings (including `.toFixed(1)`-style percentage formatting).

**Example**

```python
from sportsdataverse.mbb.mbb_positions import build_position, TRAD_POS_LIST
confs = dict(zip(TRAD_POS_LIST, [0.9, 0.1, 0, 0, 0]))
player = {"off_assist": {"value": 0.10}, "off_3pr": {"value": 0.20},
          "off_team_poss": {"value": 1000}, "off_usage": {"value": 0.20}}
build_position(confs, None, player, "Men_Boston College_2019/20")

# A manual-override short-circuit

build_position(confs, None, {"key": "Popovic, Nik",
    "off_usage": {"value": 1}, "off_team_poss": {"value": 200},
    "off_assist": {"value": 0.10}}, "Men_Boston College_2019/20")
```

### build_position_confidences {#build_position_confidences}

`build_position_confidences(player: 'dict[str, Any]', height_in: 'float | None' = None) -> 'tuple[dict[str, float], dict[str, Any]]'`

Build the 5-way positional confidence vector for a player.

Faithful port of `PositionUtils.buildPositionConfidences`
(`PositionUtils.ts:263-338`). Derives the six `calc_*` ratios from the
player's box-score fields, dot-products the resulting 17-feature vector
against `POSITION_FEATURE_WEIGHTS` (each field regressed via
`regress_shot_quality` and multiplied by its per-feature `scale`)
plus the `POSITION_FEATURE_INIT` intercepts, applies a softmax over
the five raw scores, and -- when `height_in` is supplied -- reweights the
confidences via `incorporate_height`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player` | `dict[str, Any]` |  | The player stat dict (ES-aggregation bucket shape); each stat field is `{"value": N}`. Reads `total_off_assist`, `total_off_to`, `off_3p`, `off_efg`, `off_2pmid`, `off_2prim`, `total_off_fga`, `total_off_fta`, `total_off_ftm` (for the `calc_*` ratios) plus every non-`calc_` field in `POSITION_FEATURE_WEIGHTS`. |
| `height_in` | `float \| None` | `None` | Optional player height in inches. When truthy, the returned confidences are height-adjusted; when `None` / `0`, the raw softmax confidences are returned. (JS `height_in ? ... : ...` falsy check, ts:324 -- a `0` height is treated as "no height".) |

**Returns**

A `(confidences, diagnostics)` tuple. `confidences` maps each `TRAD_POS_LIST` key (in order) to its final confidence. `diagnostics` carries `"scores"` (raw scores x `0.1`, keyed by position), `"confsNoHeight"` (the pre-height confidences, present only when `height_in` is truthy, else `None`), and `"calculated"` (the six derived `calc_*` ratios). The upstream diag object has exactly these three fields -- no UI-only fields are dropped.

**Example**

```python
from sportsdataverse.mbb.mbb_positions import build_position_confidences
confs, diags = build_position_confidences(player_bucket)
print(confs["pos_pg"], diags["calculated"]["calc_ast_tov"])

# Height-adjusted confidences

confs_h, diags_h = build_position_confidences(player_bucket, 78.0)
```

### build_positional_aware_filter {#build_positional_aware_filter}

`build_positional_aware_filter(filter_str: 'str') -> 'tuple[list[dict[str, Any]], list[dict[str, Any]], bool]'`

Decompose a search-filter string into positionally-aware +ve/-ve fragments.

Faithful port of `PositionUtils.buildPositionalAwareFilter`
(`PositionUtils.ts:764-828`). Picks a fragment separator by scanning
`[";", "/", ","]` in priority order for the first one present anywhere
in `filter_str` (a fragment separator of `"!!!"` -- never itself
present -- is the "no separator found" fallback, which leaves the whole
string as a single fragment). Splits on that separator, trims whitespace,
drops empty fragments and `[`-prefixed ones (reserved for aggregation-key
filters elsewhere in the app), then routes each fragment to the positive
or negative bucket by a leading `-`, and parses each fragment's optional
`=<tokens>` position spec via decomp_positional_filter_fragment`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filter_str` | `str` |  | A raw filter string, e.g. `"test1=pg / -test2=Pf+C / test3"`. |

**Returns**

A `(positive_fragments, negative_fragments, has_position)` triple. Each fragment is `{"filter": <lowercased name>, "pos": [indices]}`. `has_position` is `True` iff any fragment (either side) carried at least one recognized position token.

**Example**

```python
::

from sportsdataverse.mbb.mbb_positions import build_positional_aware_filter
build_positional_aware_filter("test1=pg / -test2=Pf+C / test3")
```

### build_priors {#build_priors}

`build_priors(players_baseline: 'dict[PlayerId, IndivStatSet]', stats_averages: 'PureStatSet', avg_efficiency: 'float', col_to_player: 'list[str]', prior_mode: 'float', value_key: 'ValueKey' = 'value') -> 'RapmPriorInfo'`

Build strong/weak per-player RAPM priors for every column.

Faithful port of `RapmUtils.buildPriors` (`RapmUtils.ts:237-407`).
See the module docstring's landmine list, item 1, for the critical
Python-vs-JS `{}`-truthiness gotcha this function's implementation
deliberately avoids.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players_baseline` | `dict[PlayerId, IndivStatSet]` |  | `{player_id: IndivStatSet}` -- the most-general per-player baseline info (in production, sourced from `mbb_ratings.build_productivity`'s output; see the module docstring's "RAPM prior source" note). |
| `stats_averages` | `PureStatSet` |  | League/context average stat set, used by the (currently dead-code, see landmine 4) `get_prior_basis` fallback and by `with_avg_or_undef`'s nil-check gate. |
| `avg_efficiency` | `float` |  | League/context average efficiency. |
| `col_to_player` | `list[str]` |  | The player ids, in column order -- `playersStrong`/ `playersWeak` are index-aligned with this list. |
| `prior_mode` | `float` |  | `-1` for adaptive mode, `-2` (or lower) for no prior, `0`-`1` for a fixed strong-prior weight. |
| `value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- allows priors to be built from luck-adjusted parameters. |

**Returns**

A `RapmPriorInfo`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import build_priors

priors = build_priors({}, {}, 100.0, ["Wiggins, Aaron"], -1)
print(priors["players_weak"][0])
```

### build_productivity {#build_productivity}

`build_productivity(o_rtg: 'float', o_adj: 'float', usage: 'float', avg_efficiency: 'float') -> 'dict[str, float]'`

Public port of `RatingUtils.buildProductivity` (`RatingUtils.ts:963-990`).

Promoted to public in Task 2.3 -- see the module docstring's "Ported
behavior" section for the promotion rationale (Phase-3 RAPM needs to
import this across module boundaries).

Converts `ORtg` and a few other numbers into "productivity" using Dean
Oliver's PUE ("Player Usage Efficiency") formulation, SoS-adjusted via
`o_adj = avgEfficiency / Def_SOS`. **RAPM prior source (Phase 3):**
`Adj_ORtgPlus` is the value RAPM uses as an individual-offense prior --
see `PLAN-phase2.md`'s self-review notes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `o_rtg` | `float` |  | The player's (possibly override-adjusted) `ORtg`. |
| `o_adj` | `float` |  | `avg_efficiency / Def_SOS` -- the strength-of-schedule adjustment factor. |
| `usage` | `float` |  | `100 * TotPoss / (Team_Poss or 1)` -- the player's possession-usage percentage. |
| `avg_efficiency` | `float` |  | The league/context average efficiency (`100` in every vendored jest call). |

**Returns**

`{"Adj_ORtg": float, "Adj_ORtgPlus": float, "Usage_Bonus": float, "SoS_Bonus": float}` -- keys kept TS-verbatim (see module docstring's naming-convention note).

### build_strength_adjusted_stats {#build_strength_adjusted_stats}

`build_strength_adjusted_stats(teams: 'Sequence[TeamDetail]', *, max_iterations: 'int' = 100, tolerance: 'float' = 1e-06) -> 'StrengthAdjustedResult'`

Run the full strength-adjustment compute over a team list.

Ports the COMPUTE half of the CLI `main()` (`ts:594-662`): dedupe
teams by name (first-wins, as `main` does across its tier files),
compute possession splits + league averages, run
`run_iterative_adjustment_with_hca`, then assemble each team's
`raw` / `adj` / `adj_hca` field maps. The file/CLI glue
(`fs`/`argv`/`dataLastUpdated`/serialization) is intentionally not
ported -- pass an already-loaded `team_details` list.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `Sequence[TeamDetail]` |  | The `team_details` team dicts (each `{team_name, conf, opponents: [...]}`). Duplicate `team_name`s keep the first occurrence. |
| `max_iterations` | `int` | `100` | Solver iteration cap (default `MAX_ITERATIONS`). |
| `tolerance` | `float` | `1e-06` | Solver convergence tolerance (default `TOLERANCE`). |

**Returns**

A `StrengthAdjustedResult` (`averages` per field + per-team `raw`/`adj`/`adj_hca`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import build_strength_adjusted_stats

result = build_strength_adjusted_stats(team_details)
print(result.averages["3p"].league_off)
print(result.teams[0].adj["3p"])  # {"off": ..., "def": ...}
```

### build_sub_error {#build_sub_error}

`build_sub_error(*subids: 'str', error: 'str') -> 'ParseError'`

Build a location-less `ParseError` from id fragments

(`ParseUtils.build_sub_error`, `ParseUtils.scala:83-85`, delegating
through `build_error`/`build_errors`/`build_error_id` with
`location=""`/`base_id=""`; the `shapeless`-based
`sequence_kv_results` accumulation machinery in the same file is out
of scope).

Scala's call shape is curried -- `build_sub_error("team")("message")`
(two argument groups: varargs `subids`, then a single `error`
string). Python has no currying sugar for that shape, so `subids` is
a plain `*args` tuple and `error` is a required keyword-only
argument at the same call site.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `error` | `str` |  | The single human-readable error message. |

**Returns**

A `ParseError` with `location=""` and `messages=[error]`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import build_sub_error

err = build_sub_error("team", error="Could not match team names")
err.id  # '[team]'
```

### build_tidy_player_context {#build_tidy_player_context}

`build_tidy_player_context(box_lineup: 'LineupEvent') -> 'TidyPlayerContext'`

Build the alternative player-code lookup maps for a box-score lineup

(`LineupErrorAnalysisUtils.build_tidy_player_context`, `:59-73`).

Sometimes the play-by-play uses `SURNAME,INITIAL` instead of
`SURNAME,NAME`, or `SURNAME,NAME1` instead of `SURNAME,NAME1
NAME2` -- both collapse to the same *truncated* code, so grouping by
truncated code (and only keeping groups with exactly one distinct name)
lets `tidy_player` recover the box-score name unambiguously.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | The box-score lineup event to index. |

**Returns**

A fresh `TidyPlayerContext` (empty `resolution_cache`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context
ctx = build_tidy_player_context(box_lineup)
```

### build_weak_prior_from_rapm {#build_weak_prior_from_rapm}

`build_weak_prior_from_rapm(rapm_results: 'list[float]', off_or_def: 'str') -> 'list[dict[str, float]]'`

Wrap a flat RAPM-estimate vector into `playersWeak`-shaped dicts.

Faithful port of `RapmUtils.buildWeakPriorFromRapm` (`RapmUtils.ts:410-419`),
used only by `pick_ridge_regression`'s `use_recursive_weak_prior`
branch to substitute the just-computed (pre-strong-prior) RAPM values as
the *weak* prior for a follow-up `apply_weak_priors` call -- "the
recursive prior" per the upstream `/** For "recursive" prior */` comment.

**Uncovered by the oracle** -- `semiRealRapmResults.testContext.priorInfo
.useRecursiveWeakPrior` is `false`, so `RapmUtils.test.ts`'s
`"pickRidgeRegression"` test never calls this function. Ported
faithfully from TS regardless (per "TS governs"); flagged as a documented
gap rather than backed by a synthetic test, matching this module's
existing convention for other upstream-untested branches (e.g. the
"Task 3.3 coverage gap" note above).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rapm_results` | `list[float]` |  | A flat per-player RAPM estimate vector, e.g. `pick_ridge_regression`'s own `results_pre_prior`. |
| `off_or_def` | `str` |  | `"off"` or `"def"` -- selects the output key, `f"{off_or_def}_adj_ppp"`. |

**Returns**

One `{f"{off_or_def}_adj_ppp": rapm}` dict per input element, index-aligned with `rapm_results`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import build_weak_prior_from_rapm

weak_prior = build_weak_prior_from_rapm([5.0, 4.5], "off")
print(weak_prior[0])  # {"off_adj_ppp": 5.0}
```

### classify_point_value {#classify_point_value}

`classify_point_value(dist_ft: 'float', x: 'float', y: 'float', *, league: 'str', season: 'int') -> 'int'`

2 or 3 from basket-relative geometry (arc radius + corner band).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dist_ft` | `float` |  | Euclidean distance from the basket, feet. |
| `x` | `float` |  | Lateral offset from the basket, feet (baseline direction). |
| `y` | `float` |  | Distance up-court from the basket, feet. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year (selects the arc era). |

**Returns**

`3` at/beyond the arc or in the corner band, else `2`.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_point_value
classify_point_value(24.0, 0.0, 24.0, league="mens", season=2020)
```

### classify_zone_geometry {#classify_zone_geometry}

`classify_zone_geometry(dist_ft: 'float', x: 'float', y: 'float', *, league: 'str', season: 'int') -> 'str'`

Shot zone from geometry: `rim | paint | mid | corner3 | abovebreak3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dist_ft` | `float` |  | Euclidean distance from the basket, feet. |
| `x` | `float` |  | Lateral offset from the basket, feet. |
| `y` | `float` |  | Distance up-court from the basket, feet. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year (selects the arc era). |

**Returns**

One of `rim`, `paint`, `mid`, `corner3`, `abovebreak3`.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_geometry
classify_zone_geometry(2.0, 0.0, 2.0, league="mens", season=2020)
```

### classify_zone_type {#classify_zone_type}

`classify_zone_type(type_text: "'str | None'") -> "'str | None'"`

Collapse a source shot-type label to `rim | arc3 | jump`.

Note: the 2025+ ESPN shots release carries NO three-point marker in
`type_text` (vocabulary is JumpShot/LayUpShot/DunkShot/TipShot), so
`arc3` typically comes from geometry/score_value there; the branch
exists for sources that do label threes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type_text` | `str \| None` |  | Source label (e.g. `"DunkShot"`); `None` passes through. |

**Returns**

`rim`, `arc3`, `jump`, or `None` for null input.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_type
classify_zone_type("DunkShot")
```

### espn_shots_to_canonical {#espn_shots_to_canonical}

`espn_shots_to_canonical(espn: 'pl.DataFrame', *, league: 'str', season: 'int', scale: "'tuple[float, float, float] | None'" = None) -> 'pl.DataFrame'`

ESPN `load_mbb_shots` frame -> the canonical shot frame.

Field-goal attempts only (free throws and sentinel-coordinate rows are
dropped). `point_value` comes from `score_value` -- the release
populates it on misses too, and its `type_text` carries NO three-point
marker, so `arc3` is value-derived. Coordinates are re-based to the
fitted basket origin and scaled to feet.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn` | `DataFrame` |  | `load_mbb_shots`-shaped frame. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year. |
| `scale` | `tuple[float, float, float] \| None` | `None` | Optional pre-fitted `(origin_x, origin_y, feet_per_unit)`; fitted from `espn` when `None`. |

**Returns**

The canonical shot frame (`CANONICAL_SHOT_SCHEMA`); empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_shots
from sportsdataverse.mbb.mbb_shots_adapter import espn_shots_to_canonical
df = espn_shots_to_canonical(load_mbb_shots([2025]), league="mens", season=2025)
```

### fit_espn_court_scale {#fit_espn_court_scale}

`fit_espn_court_scale(espn: 'pl.DataFrame', *, league: 'str', season: 'int') -> "'tuple[float, float, float]'"`

Fit the ESPN raw-coordinate court scale: `(origin_x, origin_y, feet_per_unit)`.

The release's `coordinate_{x,y}_raw` grid is basket-anchored half-court
(width 0-50, rim cluster near `(25, 2)`). Origin = median raw
coordinates of made rim-type shots; `feet_per_unit` = arc radius /
median unit-distance of made threes from that origin -- fitted, not
guessed, so a units change in the release shows up as a scale shift.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn` | `DataFrame` |  | `load_mbb_shots`-shaped frame. |
| `league` | `str` |  | `"mens"` or `"womens"`. |
| `season` | `int` |  | Season-ending year (selects the arc radius). |

**Returns**

`(origin_x, origin_y, feet_per_unit)`; documented fallbacks `(25.0, 2.0, 1.0)` when either calibration subset is empty.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import fit_espn_court_scale
scale = fit_espn_court_scale(espn, league="mens", season=2025)
```

### mbb_pbp_disk {#mbb_pbp_disk}

`mbb_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### mbb_shot_data {#mbb_shot_data}

`mbb_shot_data(seasons: "'int | list[int]'", *, source: 'str' = 'espn', league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Season(s) of shots in the canonical frame (the spine's data entry point).

`source="espn"` loads the sportsdataverse-data shots release
(`load_mbb_shots` / `load_wbb_shots`) and canonicalizes it. The NCAA
HTML path is per-game, not per-season -- parse with
`create_shot_event_data` and flatten via `shot_events_to_frame`
instead (`source="ncaa"` raises with that pointer).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (e.g. `2025`) or list of seasons. |
| `source` | `str` | `'espn'` | `"espn"` (the only batch source). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The canonical shot frame; seasons the release doesn't cover are skipped, and no coverage at all returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data
shots = mbb_shot_data(2025)

# Pipeline next step (one line)

shots.group_by("shot_zone").agg(pl.col("made").mean()).sort("shot_zone")
```

### ncaa_mbb_game_pbp {#ncaa_mbb_game_pbp}

`ncaa_mbb_game_pbp(game_id: 'object', *, fetcher: 'Optional[_SupportsFetchGamePbp]' = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Scrape one MBB game's play-by-play (bigballR `scrape_game`).

Fetches `stats.ncaa.org/contests/{game_id}/play_by_play` and parses it
through `parse_ncaa_bb_game_pbp` with the MBB period model
`(2, 1200, 300)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `object` |  | NCAA contest id (e.g. `"6470186"`). |
| `fetcher` | `Optional[_SupportsFetchGamePbp]` | `None` | Optional injected fetcher exposing `fetch_game_pbp` (for tests/offline use). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The 35-column play-by-play frame (zero rows when the game is not found).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_game_pbp
df = ncaa_mbb_game_pbp("6470186")
print(df.shape)

# Offline with an injected fetcher

df = ncaa_mbb_game_pbp("6470186", fetcher=my_fetcher)

# Pipeline next step (one line)

df.filter(pl.col("event_type") == "Three Point Jumper").head()
```

### shot_events_to_frame {#shot_events_to_frame}

`shot_events_to_frame(events: 'list[ShotEvent]', *, season: 'int', league: 'str' = 'mens') -> 'pl.DataFrame'`

Flatten NCAA HTML `ShotEvent` objects to the canonical frame.

The NCAA SVG shot maps carry location + made/miss but no shot-type label
(`shot_type = "unknown"`); `point_value`/`shot_zone` come from the
geometry classifiers. The parser-phase `pts` field is the MADE flag
(1/0), not the point value.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `events` | `list[ShotEvent]` |  | Parsed shot events (`create_shot_event_data` output). |
| `season` | `int` |  | Season-ending year the events belong to. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

The canonical shot frame (`CANONICAL_SHOT_SCHEMA`); empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb.mbb_shots_adapter import shot_events_to_frame
df = shot_events_to_frame(events, season=2025)
```
