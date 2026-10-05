---
title: "MBB — additional Python functions — Other: load_artifact–adjust_efficiency"
sidebar_label: "Other: load_artifact–adjust_efficiency"
sidebar_position: 9
description: "MBB — additional Python functions — Other: load_artifact–adjust_efficiency — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Other: load_artifact–adjust_efficiency

### load_artifact {#load_artifact}

`load_artifact(name: 'str') -> 'dict'`

Read a bundled player-value artifact (`mbb/models/<name>.json`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Artifact stem, e.g. `"mbb_box_bpm"`. |

**Returns**

The parsed JSON dict.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import load_artifact
art = load_artifact("mbb_box_bpm")
```

### load_proxybonanza_pool {#load_proxybonanza_pool}

`load_proxybonanza_pool(api_key: 'str', pkg: 'str', *, transport: 'Optional[PoolTransport]' = None) -> "'list[str]'"`

Resolve a ProxyBonanza package into a list of `http://login:pass@ip:port` URLs.

Graduated from `dev/ncaa_proxy.py`'s `load_proxy_pool` -- same
endpoint shape, minus the `.Renviron` reader (creds are now explicit
params, per the creds-hygiene directive).

Endpoint: `GET https://api.proxybonanza.com/v1/userpackages/{pkg}.json`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `api_key` | `str` |  | ProxyBonanza API key. |
| `pkg` | `str` |  | ProxyBonanza package id. |
| `transport` | `Optional[PoolTransport]` | `None` | Injectable `(url, headers) -> (status, text)` callable for offline testing. Defaults to a curl_cffi GET. |

**Returns**

One `http://login:password@ip:port` URL per IP in the package.

**Example**

```python
def fake(url, headers):
    return 200, '{"data": {"login": "u", "password": "p", "ippacks": []}}'
pool = load_proxybonanza_pool("key", "pkg", transport=fake)
```

### AssistEvent {#AssistEvent}

`AssistEvent(player_code: 'str', count: 'ShotClockStats' = <factory>) -> None`

One assist relationship's counts (`LineupEventStats.AssistEvent`,

`LineupEventStats.scala:64-67`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_code` | `str` |  | The other player in the assist event (by code). |
| `count` | `ShotClockStats` | `<factory>` | The assist counts, by shot-clock segment. |

### AssistInfo {#AssistInfo}

`AssistInfo(counts: 'ShotClockStats' = <factory>, target: 'Optional[list[AssistEvent]]' = None, source: 'Optional[list[AssistEvent]]' = None) -> None`

Detailed assist info, split into given/received

(`LineupEventStats.AssistInfo`, `LineupEventStats.scala:87-91`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `counts` | `ShotClockStats` | `<factory>` | Raw assist statistics. |
| `target` | `Optional[list[AssistEvent]]` | `None` | Players "I" assisted, if tracked. |
| `source` | `Optional[list[AssistEvent]]` | `None` | Players who assisted "me", if tracked. |

### BadLineupClump {#BadLineupClump}

`BadLineupClump(evs: 'list[LineupEvent]', next_good: 'Optional[LineupEvent]' = None) -> None`

A run of consecutive bad :class:`~sportsdataverse.mbb.mbb_ncaa_models

.LineupEvent`\ s that were merged together, plus the first following
good event (`LineupErrorAnalysisUtils.BadLineupClump`, `:223-226`).

The Scala case class is `protected` (module-private), but this port
exports it: the Task 5d.3 fixers and the (not-yet-ported) Task 5e
orchestrator both consume `BadLineupClump` instances directly, so
keeping it private here would just force every caller to reach past a
leading underscore.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `list[LineupEvent]` |  | The clumped lineup events, in chronological order. |
| `next_good` | `Optional[LineupEvent]` | `None` | The first known-good lineup event following the clump, if any -- used by the Task 5d.3 fixers to reason about a player who should have subbed back in. |

### ConcurrentClump {#ConcurrentClump}

`ConcurrentClump(evs: 'list[RawGameEvent]' = <factory>, lineups: 'list[LineupEvent]' = <factory>) -> None`

A clump of concurrent raw events, together with the lineups that end

in that clump (`Concurrency.ConcurrentClump`, `PossessionUtils.scala
:64-69`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `list[RawGameEvent]` | `<factory>` | The raw game events in this clump, in chronological order. |
| `lineups` | `list[LineupEvent]` | `<factory>` | The lineups (if any) whose `end_min` falls in this clump. |

### ConferenceId {#ConferenceId}

`ConferenceId(name: 'str') -> None`

CBB conference identifier (`ConferenceId`, ``models/ConferenceId

.scala:7`, `AnyVal`). **Scope addition, Task 5e.4** -- the first model
consumed by `mbb_ncaa_team_parsers.py` (`TeamIdParser.get_team_triples`
/ `build_lineup_cli_array` / `build_available_team_list``). Appended
here (not inserted among the 5a-reviewed classes above) to keep this an
additive-only change, matching `RosterEntry`'s precedent.

**`ConferenceId.is_high_major` (the companion object's other member,
`models/ConferenceId.scala:11-16`) is NOT ported.** It has no call site
anywhere in `TeamIdParser`/`TeamScheduleParser` (verified: the only
other `ConferenceId` construction sites in the upstream tree are
`kenpom/TeamParser.scala` and `BuildIngestPipeline.scala`, neither of
which is in this port's scope, and neither calls `is_high_major`
either) -- nothing in Phase 5e would exercise it. Noted here rather than
silently dropped, matching this module's precedent for other
unreferenced companion-object members (see the module docstring's
`Year.until` / `Game.Score.by_winner` notes).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The unique name of the conference. |

### CutdownShotEvent {#CutdownShotEvent}

`CutdownShotEvent(loc: 'Optional[ShotLocation]', geo: 'Optional[ShotGeo]', dist: 'Optional[float]', pts: 'int', value: 'int', is_ast: 'Optional[bool]', is_trans: 'Optional[bool]', is_orb: 'Optional[bool]') -> None`

A narrowed `ShotEvent`, keeping only the fields needed once a

shot has been matched to a player/lineup event (`CutdownShotEvent`,
`models/ncaa/ShotEvent.scala:31-40`). **Scope addition, Task 5e.5** --
ported for shape fidelity even though **it is dead code in the ENTIRE
upstream tree**: grepping shows it appears only in its own definition
(`ShotEvent.scala`) and as the never-populated `shot_info:
Option[CutdownShotEvent]` field on `PlayerEvent.scala` -- it is never
constructed anywhere. (`PlayByPlayUtils.shot_value` is an UNRELATED
`event_str -> int` point-value classifier that merely shares a similar
name -- Task 5e.6 will NOT produce this type either.) Appended here (not
inserted among the 5a-reviewed classes above) to keep this an
additive-only change, matching `RosterEntry`/`ConferenceId`'s
precedent.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `loc` | `Optional[ShotLocation]` |  | The shot's court location, in feet, if known. |
| `geo` | `Optional[ShotGeo]` |  | The shot's synthetic lat/lon, if known. |
| `dist` | `Optional[float]` |  | The shot's distance from the basket, in feet, if known. |
| `pts` | `int` |  | The point value if made (`2`/`3`), else `0`. |
| `value` | `int` |  | The shot's attempt value (`2`/`3`), regardless of make/miss. |
| `is_ast` | `Optional[bool]` |  | Whether the shot was assisted, if known. |
| `is_trans` | `Optional[bool]` |  | Whether the shot was in transition, if known. |
| `is_orb` | `Optional[bool]` |  | Whether the shot followed an offensive rebound, if known. |

### Direction {#Direction}

`Direction(*values)`

Which team is in possession (`RawGameEvent.Direction`, `:119-121`).

### FieldAverage {#FieldAverage}

`FieldAverage(league_off: 'float', league_def: 'float', hca_off: 'float', hca_def: 'float') -> None`

League average + estimated HCA for one stat field (`ts:620-625`).

`league_off`/`league_def` are the possession-weighted league means of
the per-game raw rate; `hca_off`/`hca_def` are the residual-derived
home-court advantages the solver converged on.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_off` | `float` |  |  |
| `league_def` | `float` |  |  |
| `hca_off` | `float` |  |  |
| `hca_def` | `float` |  |  |

### FieldGoalStats {#FieldGoalStats}

`FieldGoalStats(attempts: 'ShotClockStats' = <factory>, made: 'ShotClockStats' = <factory>, ast: 'Optional[ShotClockStats]' = None) -> None`

Field-goal counting stats (`LineupEventStats.FieldGoalStats`,

`LineupEventStats.scala:75-79`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `attempts` | `ShotClockStats` | `<factory>` | Shot attempts, successful or not. |
| `made` | `ShotClockStats` | `<factory>` | Successful shot attempts. |
| `ast` | `Optional[ShotClockStats]` | `None` | Successful shot attempts that were assisted, if tracked. |

### FuzzyMatchError {#FuzzyMatchError}

`FuzzyMatchError(message: 'str') -> None`

A failed `fuzzy_box_match` resolution (Scala's `Left[String]`

half of `Either[String, String]` -- Python has no `Either`, so the
error is returned directly; check `isinstance(result, FuzzyMatchError)`,
matching the `parse_team_name` / `~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError`
convention already used in this port).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `message` | `str` |  | Human-readable description of why no name won. |

### GameBreakEvent {#GameBreakEvent}

`GameBreakEvent(min: 'float', score: 'Score') -> None`

A break in play (timeout, end of period, etc.) short of the end of

the game (`Model.GameBreakEvent`, `ExtractorUtils.scala:874-877`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |

**Methods**

#### GameBreakEvent.with_min

`GameBreakEvent.with_min(new_min: 'float') -> "'GameBreakEvent'"`

Return a copy with `min` replaced (`:876`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### GameEndEvent {#GameEndEvent}

`GameEndEvent(min: 'float', score: 'Score') -> None`

The end of the game (`Model.GameEndEvent`, `ExtractorUtils.scala:878-881`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |

**Methods**

#### GameEndEvent.with_min

`GameEndEvent.with_min(new_min: 'float') -> "'GameEndEvent'"`

Return a copy with `min` replaced (`:880`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### IterationResult {#IterationResult}

`IterationResult(adj_values: ForwardRef('AdjValues'), hca_per_field: ForwardRef('HcaPerField'))`

Return of `run_iterative_adjustment_with_hca` (`ts:314-317`).

`adj_values` maps `team_name -> field -> {"off","def"}` (the converged
strength-of-schedule adjustment); `hca_per_field` maps `field ->
{"hca_off","hca_def"}`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adj_values` | `ForwardRef('AdjValues')` |  |  |
| `hca_per_field` | `ForwardRef('HcaPerField')` |  |  |

### LeagueConstants {#LeagueConstants}

`LeagueConstants(hfa: 'float', margin_sd: 'float', em_scale: 'float', avg_tempo: 'float', avg_efficiency: 'float', quad_thresholds: 'dict[str, dict[str, int]]', bubble_adj_em: 'float', in_game_wp_artifact: 'str') -> None`

Per-league fitted constants for the prediction & tournament stack.

Algorithms in the stack are league-agnostic; every men's/women's-specific
number lives here so a WBB caller is a by-reference shim plus this table
(the same pattern `wbb_rapm` / `wbb_ratings` already use).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `hfa` | `float` |  | Home-court advantage in points (fitted on the 2024 backtest). |
| `margin_sd` | `float` |  | Std. dev. of the game-margin residual (fitted on the 2024 backtest; the Brier-minimizing sigma agrees to within 0.04). |
| `em_scale` | `float` |  | Slope applied to the AdjEM difference when predicting a game margin. AdjEM is per-100-possessions, so a game margin scales by ~tempo/100 (~0.67); the fitted value is lower still because the as-of AdjEM estimate is noisy and the optimal predictive slope is attenuated (regression dilution). Fitted jointly with `hfa`. |
| `avg_tempo` | `float` |  | League baseline possessions per game (adjusted-tempo anchor). |
| `avg_efficiency` | `float` |  | League baseline points per 100 possessions. |
| `quad_thresholds` | `dict[str, dict[str, int]]` |  | NET-style quadrant opponent-rank upper bounds, keyed by venue (`home` / `neutral` / `away`) then `q1` / `q2` / `q3` (Quad 4 is any opponent ranked worse than `q3`). |
| `bubble_adj_em` | `float` |  | AdjEM of a bubble-quality team on THIS engine's scale (mean of engine ranks 40-50 on the fit season) -- the WAB baseline. |
| `in_game_wp_artifact` | `str` |  | Filename of the bundled in-game-WP coefficients under `sportsdataverse/mbb/models` (fitted + committed in Phase 3). |

### LineupBuildingState {#LineupBuildingState}

`LineupBuildingState(curr: 'LineupEvent', tidy_ctx: "'TidyPlayerContext'", prev: 'list[LineupEvent]' = <factory>, old_format: 'Optional[bool]' = None) -> None`

State for building raw lineup data across a fold over play-by-play

events (`Model.LineupBuildingState`, `ExtractorUtils.scala:735-819`).

See the module docstring's "`with_*` methods return NEW instances"
note -- every mutator below returns a fresh `LineupBuildingState`
rather than mutating `self`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr` | `LineupEvent` |  | The lineup event currently being built. |
| `tidy_ctx` | `TidyPlayerContext` |  | Name-resolution context for the current game (see `~sportsdataverse.mbb.mbb_ncaa_names.TidyPlayerContext`); threaded through `build_partial_lineup_list`, which calls `~sportsdataverse.mbb.mbb_ncaa_names.tidy_player` with it on every sub event. |
| `prev` | `list[LineupEvent]` | `<factory>` | Completed lineup events, most-recently-completed first (i.e. the reverse of `build`'s output order). |
| `old_format` | `Optional[bool]` | `None` | `True` once latched onto the legacy (pre-2018-ish) NCAA play-by-play format, `None` until the first sub is seen. |

**Methods**

#### LineupBuildingState.build

`LineupBuildingState.build() -> 'list[LineupEvent]'`

The full chronological lineup-event list (`:742-744`:

`(curr :: prev).reverse`).

#### LineupBuildingState.is_active

`LineupBuildingState.is_active(min: 'float') -> 'bool'`

Whether the current lineup has non-sub activity, or has simply

been on the floor long enough to trust (`:760-766`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute to check against. |

**Returns**

`True` if any raw event on `curr` isn't an opponent sub, or if `min` is more than `SUB_SAFETY_DELTA_MINS` past `curr`'s `end_min`.

#### LineupBuildingState.is_sub

`LineupBuildingState.is_sub(raw: 'RawGameEvent') -> 'bool'`

Whether `raw` is an *opponent*-side substitution line

(`:749-758`).

Only `~sportsdataverse.mbb.mbb_ncaa_models.RawGameEvent.opponent`
is inspected -- per the Scala scaladoc, "opposition subs are
currently treated as game events but shouldn't result in new
lineups"; the team's own subs never reach here as raw events in the
first place (they route through the fold's dedicated `Sub*Event`
branches, not `with_team_event`), so this check only ever
needs to look at the opponent side. Ported verbatim, quirk and all.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `raw` | `RawGameEvent` |  | The raw event to classify. |

**Returns**

`True` if `raw.opponent` ends with one of the four substitution phrases (case/whitespace-insensitive), else `False` (including when `raw.opponent` is `None`).

#### LineupBuildingState.with_latest_score

`LineupBuildingState.with_latest_score(score: 'Score') -> "'LineupBuildingState'"`

Update `curr`'s running end-of-event score (`:788-796`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `score` | `Score` |  |  |

#### LineupBuildingState.with_opponent_event

`LineupBuildingState.with_opponent_event(min: 'float', event_string: 'str') -> "'LineupBuildingState'"`

Append an opponent-side raw event and bump `end_min` (`:808-818`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  |  |
| `event_string` | `str` |  |  |

#### LineupBuildingState.with_player_in

`LineupBuildingState.with_player_in(player_name: 'str') -> "'LineupBuildingState'"`

Prepend a new "subbed in" player code onto `curr` (`:770-778`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_name` | `str` |  |  |

#### LineupBuildingState.with_player_out

`LineupBuildingState.with_player_out(player_name: 'str') -> "'LineupBuildingState'"`

Prepend a new "subbed out" player code onto `curr` (`:779-787`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_name` | `str` |  |  |

#### LineupBuildingState.with_team_event

`LineupBuildingState.with_team_event(min: 'float', event_string: 'str') -> "'LineupBuildingState'"`

Append a team-side raw event and bump `end_min` (`:797-807`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  |  |
| `event_string` | `str` |  |  |

### LineupEvent {#LineupEvent}

`LineupEvent(date: 'datetime', location_type: 'LocationType', start_min: 'float', end_min: 'float', duration_mins: 'float', score_info: 'ScoreInfo', team: 'TeamSeasonId', opponent: 'TeamSeasonId', lineup_id: 'LineupId', players: 'list[PlayerCodeId]', players_in: 'list[PlayerCodeId]', players_out: 'list[PlayerCodeId]', raw_game_events: 'list[RawGameEvent]', team_stats: 'LineupEventStats', opponent_stats: 'LineupEventStats', player_count_error: 'Optional[int]' = None) -> None`

A portion of a game during which a given lineup was on the floor

(`LineupEvent`, `LineupEvent.scala:41-58`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `datetime` |  | The date of the game. |
| `location_type` | `LocationType` |  | Home/away/neutral (etc.) for this game. |
| `start_min` | `float` |  | The point in the game at which the lineup entered. |
| `end_min` | `float` |  | The point in the game at which the lineup changed. |
| `duration_mins` | `float` |  | The duration of the lineup. |
| `score_info` | `ScoreInfo` |  | The score differential context for this event. |
| `team` | `TeamSeasonId` |  | The team under analysis. |
| `opponent` | `TeamSeasonId` |  | The opposing team. |
| `lineup_id` | `LineupId` |  | A string that defines the set of players on the floor. |
| `players` | `list[PlayerCodeId]` |  | Mapping from player code to full identity, for this lineup. |
| `players_in` | `list[PlayerCodeId]` |  | Players who subbed in for this event. |
| `players_out` | `list[PlayerCodeId]` |  | Players who subbed out for this event. |
| `raw_game_events` | `list[RawGameEvent]` |  | The raw NCAA event strings for both teams. |
| `team_stats` | `LineupEventStats` |  | Numerical stats extracted for the lineup (team side). |
| `opponent_stats` | `LineupEventStats` |  | Numerical stats extracted for the lineup (opponent side). |
| `player_count_error` | `Optional[int]` | `None` | If the lineup is "impossible", the number of players actually seen (for analysis purposes). |

### LineupEventStats {#LineupEventStats}

`LineupEventStats(num_events: 'int' = 0, num_possessions: 'int' = 0, fg: 'FieldGoalStats' = <factory>, fg_rim: 'FieldGoalStats' = <factory>, fg_mid: 'FieldGoalStats' = <factory>, fg_2p: 'FieldGoalStats' = <factory>, fg_3p: 'FieldGoalStats' = <factory>, ft: 'FieldGoalStats' = <factory>, orb: 'Optional[ShotClockStats]' = None, drb: 'Optional[ShotClockStats]' = None, to: 'ShotClockStats' = <factory>, stl: 'Optional[ShotClockStats]' = None, blk: 'Optional[ShotClockStats]' = None, assist: 'Optional[ShotClockStats]' = None, ast_rim: 'Optional[AssistInfo]' = None, ast_mid: 'Optional[AssistInfo]' = None, ast_3p: 'Optional[AssistInfo]' = None, foul: 'Optional[ShotClockStats]' = None, player_shot_info: 'Optional[PlayerShotInfo]' = None, pts: 'int' = 0, plus_minus: 'int' = 0) -> None`

A lineup event's full counting-stat tree (`LineupEventStats`,

`LineupEventStats.scala:7-38`).

Only `num_events`/`num_possessions`/`pts`/`plus_minus` are
exercised by Phase 5a -- see the module docstring's scope note.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `num_events` | `int` | `0` | Number of raw events folded into this lineup event. |
| `num_possessions` | `int` | `0` | Number of possessions attributed to this lineup event. |
| `fg` | `FieldGoalStats` | `<factory>` | Overall field-goal stats. |
| `fg_rim` | `FieldGoalStats` | `<factory>` | Rim field-goal stats. |
| `fg_mid` | `FieldGoalStats` | `<factory>` | Mid-range field-goal stats. |
| `fg_2p` | `FieldGoalStats` | `<factory>` | 2pt field-goal stats. |
| `fg_3p` | `FieldGoalStats` | `<factory>` | 3pt field-goal stats. |
| `ft` | `FieldGoalStats` | `<factory>` | Free-throw stats. |
| `orb` | `Optional[ShotClockStats]` | `None` | Offensive-rebound stats, if tracked. |
| `drb` | `Optional[ShotClockStats]` | `None` | Defensive-rebound stats, if tracked. |
| `to` | `ShotClockStats` | `<factory>` | Turnover stats. |
| `stl` | `Optional[ShotClockStats]` | `None` | Steal stats, if tracked. |
| `blk` | `Optional[ShotClockStats]` | `None` | Block stats, if tracked. |
| `assist` | `Optional[ShotClockStats]` | `None` | Assist stats, if tracked. |
| `ast_rim` | `Optional[AssistInfo]` | `None` | Rim-shot assist info, if tracked. |
| `ast_mid` | `Optional[AssistInfo]` | `None` | Mid-range-shot assist info, if tracked. |
| `ast_3p` | `Optional[AssistInfo]` | `None` | 3pt-shot assist info, if tracked. |
| `foul` | `Optional[ShotClockStats]` | `None` | Foul stats, if tracked. |
| `player_shot_info` | `Optional[PlayerShotInfo]` | `None` | Per-player shot-quality info, if tracked. |
| `pts` | `int` | `0` | Points scored. |
| `plus_minus` | `int` | `0` | Point differential while this lineup was on the floor. |

### LineupId {#LineupId}

`LineupId(value: 'str') -> None`

The set of players on the floor, as an opaque id string

(`LineupEvent.LineupId`, `LineupEvent.scala:172`, `AnyVal`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `value` | `str` |  | The opaque lineup identifier. |

### LocationType {#LocationType}

`LocationType(*values)`

Game location (`Game.LocationType`, `Game.scala:36-38`).

### NcaaFetchConfig {#NcaaFetchConfig}

`NcaaFetchConfig(cache_dir: 'Optional[Path]' = None, proxy_url: 'Optional[str]' = None, proxybonanza_key: 'Optional[str]' = None, proxybonanza_pkg: 'Optional[str]' = None, timeout: 'int' = 45, impersonate: 'str' = 'chrome', max_retries: 'int' = 2, rotation_backoff: 'float' = 1.0, rotate_every: 'int' = 200, terms_backoff: 'float' = 300.0, terms_retries: 'int' = 3, transport: 'Optional[FetchTransport]' = None) -> None`

Runtime configuration for the stats.ncaa.org fetch layer.

Exactly one proxy source should be configured: either a single explicit
`proxy_url` (`http://login:password@ip:port`), or a ProxyBonanza pool
via `proxybonanza_key` + `proxybonanza_pkg` (resolved lazily by

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache_dir` | `Optional[Path]` | `None` |  |
| `proxy_url` | `Optional[str]` | `None` |  |
| `proxybonanza_key` | `Optional[str]` | `None` |  |
| `proxybonanza_pkg` | `Optional[str]` | `None` |  |
| `timeout` | `int` | `45` |  |
| `impersonate` | `str` | `'chrome'` |  |
| `max_retries` | `int` | `2` |  |
| `rotation_backoff` | `float` | `1.0` |  |
| `rotate_every` | `int` | `200` |  |
| `terms_backoff` | `float` | `300.0` |  |
| `terms_retries` | `int` | `3` |  |
| `transport` | `Optional[FetchTransport]` | `None` |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import get_config
cfg = get_config()
cfg.cache_dir     # ~/.sportsdataverse/ncaa_cache
cfg.impersonate   # "chrome"

# Configure a single proxy explicitly (rarely needed -- prefer ``update_config`` or the env vars)

from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetchConfig
cfg = NcaaFetchConfig(proxy_url="http://user:pass@1.2.3.4:8080")
```

### NcaaFetcher {#NcaaFetcher}

`NcaaFetcher(config: 'Optional[NcaaFetchConfig]' = None, *, proxy_pool: "Optional['list[str]']" = None) -> 'None'`

Cache-first stats.ncaa.org fetcher, proxy-bound per the binding directive.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config` | `Optional[NcaaFetchConfig]` | `None` |  |
| `proxy_pool` | `Optional['list[str]']` | `None` |  |

**Example**

```python
# Scrape game-detail data (the **suggested** path -- browser transport clears the Akamai bm-verify wall; see :meth:`with_browser`)

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
    with NcaaFetcher.with_browser() as fetcher:
        pbp = fetcher.fetch_game_pbp("1613299")               # raw PBP HTML
        box = fetcher.fetch_game_individual_stats("1613299")  # raw box HTML

# Un-challenged pages (landing / team) via the curl_cffi proxy path

    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher, update_config
    update_config(proxy_url="http://user:pass@1.2.3.4:8080")
    fetcher = NcaaFetcher()
    html = fetcher.fetch_team_schedule("391")  # cached after this call

# Offline (injected transport + explicit pool, no network/env needed)

    def fake(url, proxies, headers):
        return 200, "<html>...</html>"
    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetchConfig
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=fake)
    fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1"])
```

**Methods**

#### NcaaFetcher.fetch_game_box

`NcaaFetcher.fetch_game_box(contest_id: 'object', period: 'int' = 1, *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a game's box-score *landing* page for *period* (1-indexed).

Note: on current (2026) stats.ncaa.org this page is the team-stats /
game-leaders view -- the per-player box the box-score parser consumes
split out into `fetch_game_individual_stats`. Kept for the
team-stats surface and the legacy layout.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `contest_id` | `object` |  |  |
| `period` | `int` | `1` |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

#### NcaaFetcher.fetch_game_individual_stats

`NcaaFetcher.fetch_game_individual_stats(contest_id: 'object', *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a game's per-player box (the `individual_stats` tab).

This is the page `~sportsdataverse.mbb.mbb_ncaa_boxscore_parser
.get_box_lineup` parses on current markup (`format_version=1`): two
`table.dataTable.small_font#competitor_*` per-team player tables.
The server ignores `?period_no` here (returns the full-game box), so
no period arg -- see `dev/phase5f-live-proof.md`.

ponytail: the modern box split out of `box_score` into this tab; the
legacy (pre-2018) layout has no separate individual-stats page, so
`legacy=True` falls back to the legacy `box_score` path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `contest_id` | `object` |  |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

#### NcaaFetcher.fetch_game_pbp

`NcaaFetcher.fetch_game_pbp(contest_id: 'object', *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a game's play-by-play page.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `contest_id` | `object` |  |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

#### NcaaFetcher.fetch_html

`NcaaFetcher.fetch_html(path: 'str', *, force: 'bool' = False) -> 'str'`

Fetch *path* (bare path or full stats.ncaa.org URL), cache-first.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | e.g. `"contests/4690813/play_by_play"` or a full `https://stats.ncaa.org/...` URL. |
| `force` | `bool` | `False` | Bypass the cache and re-fetch, overwriting the cache file. |

**Returns**

The response HTML, decoded as UTF-8.

#### NcaaFetcher.fetch_team_roster

`NcaaFetcher.fetch_team_roster(team_id: 'object', year_id: 'object', *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a team's roster page for *year_id*.

ponytail: URL shape by analogy to the confirmed team-id scheme, not
independently live-confirmed -- see module docstring; fix in Task
5f.2 if the real path differs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `object` |  |  |
| `year_id` | `object` |  |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

#### NcaaFetcher.fetch_team_schedule

`NcaaFetcher.fetch_team_schedule(team_id: 'object', *, legacy: 'bool' = False, force: 'bool' = False) -> 'str'`

Fetch a team's game-by-game schedule page.

Modern shape (`teams/{id}/game_by_game`) is confirmed by
`dev/phase5-ncaa-proxy-proof.md`; the legacy shape is by analogy
(see `fetch_team_roster`'s note).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `object` |  |  |
| `legacy` | `bool` | `False` |  |
| `force` | `bool` | `False` |  |

### NoSurnameMatch {#NoSurnameMatch}

`NoSurnameMatch(box_name: 'str', exact_first_name: 'Optional[str]', near_first_name: 'Optional[str]', err: 'str') -> None`

No candidate surname fragment scored well enough

(`NameFixer.NoSurnameMatch`, `:638-643`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `exact_first_name` | `Optional[str]` |  | A first-name fragment shared verbatim between candidate and box name, if any. |
| `near_first_name` | `Optional[str]` |  | A first-name fragment fuzzy-matching the box name's first name, if any (only computed when `exact_first_name` is absent). |
| `err` | `str` |  | Human-readable diagnostic (debug-only; see the module docstring's fuzzy-match-parity note for why its embedded score may not byte-match the upstream Java oracle). |

### OtherOpponentEvent {#OtherOpponentEvent}

`OtherOpponentEvent(min: 'float', score: 'Score', event_string: 'str') -> None`

A non-sub event belonging to the opponent (`Model.OtherOpponentEvent`,

`ExtractorUtils.scala:866-873`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |
| `event_string` | `str` |  | The raw play-by-play event string. |

**Methods**

#### OtherOpponentEvent.with_min

`OtherOpponentEvent.with_min(new_min: 'float') -> "'OtherOpponentEvent'"`

Return a copy with `min` replaced (`:871`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### OtherTeamEvent {#OtherTeamEvent}

`OtherTeamEvent(min: 'float', score: 'Score', event_string: 'str') -> None`

A non-sub event belonging to the team under analysis

(`Model.OtherTeamEvent`, `ExtractorUtils.scala:858-865`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |
| `event_string` | `str` |  | The raw play-by-play event string. |

**Methods**

#### OtherTeamEvent.with_min

`OtherTeamEvent.with_min(new_min: 'float') -> "'OtherTeamEvent'"`

Return a copy with `min` replaced (`:863`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### ParseError {#ParseError}

`ParseError(location: 'str', id: 'str', messages: 'list[str]') -> None`

A parse-time error (`ParseError`, `ParseError.scala:9`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `location` | `str` |  | The module in which the error occurred. |
| `id` | `str` |  | The module-specific id for which the error occurred. |
| `messages` | `list[str]` |  | Human-readable description(s) of the error. |

### PbpBuilders {#PbpBuilders}

`PbpBuilders(team_finder: 'Callable[[BeautifulSoup], list[str]]', event_finder: 'Callable[[BeautifulSoup], list[Tag]]', event_time_finder: 'Callable[[Tag], Optional[str]]', event_score_finder: 'Callable[[Tag], Optional[str]]', game_event_finder: 'Callable[[Tag], Optional[str]]', event_team_finder: 'Callable[[Tag, bool], Optional[str]]', event_opponent_finder: 'Callable[[Tag, bool], Optional[str]]') -> None`

One version-era's HTML finder functions (``PlayByPlayParser

.base_builders`, `PlayByPlayParser.scala:37-51``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_finder` | `Callable[[BeautifulSoup], list[str]]` |  |  |
| `event_finder` | `Callable[[BeautifulSoup], list[Tag]]` |  |  |
| `event_time_finder` | `Callable[[Tag], Optional[str]]` |  |  |
| `event_score_finder` | `Callable[[Tag], Optional[str]]` |  |  |
| `game_event_finder` | `Callable[[Tag], Optional[str]]` |  |  |
| `event_team_finder` | `Callable[[Tag, bool], Optional[str]]` |  |  |
| `event_opponent_finder` | `Callable[[Tag, bool], Optional[str]]` |  |  |

### PeekableIterator {#PeekableIterator}

`PeekableIterator(iterable: 'Iterable[_T]') -> 'None'`

A stateful iterator with one element of look-ahead, the Python

stand-in for Scala's `scala.collection.Iterator`.

Reproduces the three `Iterator` operations `find_pbp_clump` /

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `iterable` | `Iterable[_T]` |  | Any iterable to wrap. |

**Methods**

#### PeekableIterator.find

`PeekableIterator.find(pred: 'Callable[[_T], bool]') -> 'Optional[_T]'`

First element satisfying `pred`, consuming up to and including

it (or exhausting the iterator and returning `None`) -- Scala
`Iterator.find`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pred` | `Callable[[_T], bool]` |  |  |

#### PeekableIterator.has_next

`PeekableIterator.has_next() -> 'bool'`

Whether another element is available, without consuming it

(Scala `Iterator.hasNext`).

#### PeekableIterator.to_list

`PeekableIterator.to_list() -> 'list[_T]'`

Drain the remaining elements into a list (Scala `Iterator.toList`).

### PlayerCodeId {#PlayerCodeId}

`PlayerCodeId(code: 'str', id: 'PlayerId', ncaa_id: 'Optional[str]' = None) -> None`

A player's within-team-season code paired with their full identity

(`LineupEvent.PlayerCodeId`, `LineupEvent.scala:185-189`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `code` | `str` |  | The player code, unique within the team/season only. |
| `id` | `PlayerId` |  | The player's globally-unique identity. |
| `ncaa_id` | `Optional[str]` | `None` | The player's NCAA-issued id, if known. |

### PlayerEvent {#PlayerEvent}

`PlayerEvent(player: 'PlayerCodeId', player_stats: 'LineupEventStats', date: 'datetime', location_type: 'LocationType', start_min: 'float', end_min: 'float', duration_mins: 'float', score_info: 'ScoreInfo', team: 'TeamSeasonId', opponent: 'TeamSeasonId', lineup_id: 'LineupId', players: 'list[PlayerCodeId]', players_in: 'list[PlayerCodeId]', players_out: 'list[PlayerCodeId]', raw_game_events: 'list[RawGameEvent]', team_stats: 'LineupEventStats', opponent_stats: 'LineupEventStats', player_count_error: 'Optional[int]' = None) -> None`

A lineup event's stats, narrowed to one player (`PlayerEvent`,

`models/ncaa/PlayerEvent.scala:48-70`). **Scope addition, Task 5c.4**
-- deferred by 5a since only `~sportsdataverse.mbb
.mbb_ncaa_lineup_enrich.create_player_events` (5c.4) returns it. Appended
here (not inserted among the 5a-reviewed classes above) to keep this an
additive-only change.

Same field shape as `LineupEvent` with two fields prepended
(`player`, `player_stats`) -- the Scala builds this via a
`shapeless.LabelledGeneric` HList splice of `PlayerEvent`'s own
`player`/`player_stats` onto every field of a `LineupEvent`
instance; this port has no generic-programming machinery, so
`~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.create_player_events`
constructs the dataclass directly instead.

**`SingleEventMeta` / `event_meta` / `game_id` are NOT ported.**
`PlayerEvent.scala`'s companion object nests a `SingleEventMeta` case
class, but the two fields that would carry it (`event_meta`,
`game_id`) are commented out in the Scala source itself
(`PlayerEvent.scala:67-69`) -- never part of the live case class, and
`create_player_events` never constructs a `SingleEventMeta`. Nothing
to defer; there is no live field to port.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player` | `PlayerCodeId` |  | The player this narrowed event describes. |
| `player_stats` | `LineupEventStats` |  | The player's own numerical stats for this lineup event. |
| `date` | `datetime` |  | The date of the game. |
| `location_type` | `LocationType` |  | Home/away/neutral (etc.) for this game. |
| `start_min` | `float` |  | The point in the game at which the lineup entered. |
| `end_min` | `float` |  | The point in the game at which the lineup changed. |
| `duration_mins` | `float` |  | The duration of the lineup. |
| `score_info` | `ScoreInfo` |  | The score differential context for this event. |
| `team` | `TeamSeasonId` |  | The team under analysis. |
| `opponent` | `TeamSeasonId` |  | The opposing team. |
| `lineup_id` | `LineupId` |  | A string that defines the set of players on the floor. |
| `players` | `list[PlayerCodeId]` |  | Mapping from player code to full identity, for this lineup. |
| `players_in` | `list[PlayerCodeId]` |  | Players who subbed in for this event. |
| `players_out` | `list[PlayerCodeId]` |  | Players who subbed out for this event. |
| `raw_game_events` | `list[RawGameEvent]` |  | The raw NCAA event strings for both teams. |
| `team_stats` | `LineupEventStats` |  | Numerical stats extracted for the lineup (team side). |
| `opponent_stats` | `LineupEventStats` |  | Numerical stats extracted for the lineup (opponent side). |
| `player_count_error` | `Optional[int]` | `None` | If the lineup is "impossible", the number of players actually seen (for analysis purposes). |

### PlayerShotInfo {#PlayerShotInfo}

`PlayerShotInfo(unknown_3pm: 'Optional[tuple[int, int, int, int, int]]' = None, early_3pa: 'Optional[tuple[int, int, int, int, int]]' = None, unast_3pm: 'Optional[tuple[int, int, int, int, int]]' = None, ast_3pm: 'Optional[tuple[int, int, int, int, int]]' = None) -> None`

Per-player shot-quality info, keyed by lineup slot

(`LineupEventStats.PlayerShotInfo`, `LineupEventStats.scala:98-103`).
Each tuple is a fixed-arity 5-slot (one per lineup spot), mirroring the
Scala `PlayerTuple[Int] = Tuple5[Int, Int, Int, Int, Int]` alias.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `unknown_3pm` | `Optional[tuple[int, int, int, int, int]]` | `None` | 3pt makes of unknown assist status, per slot. |
| `early_3pa` | `Optional[tuple[int, int, int, int, int]]` | `None` | Early-shot-clock 3pt attempts, per slot. |
| `unast_3pm` | `Optional[tuple[int, int, int, int, int]]` | `None` | Unassisted 3pt makes, per slot. |
| `ast_3pm` | `Optional[tuple[int, int, int, int, int]]` | `None` | Assisted 3pt makes, per slot. |

### PlayerValueConstants {#PlayerValueConstants}

`PlayerValueConstants(pace_baseline: 'float', bubble_recruit_rank: 'int', bundle_prefix: 'str') -> None`

Per-league constants for the player-value spine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pace_baseline` | `float` |  | League baseline possessions per game (per-100 scaling). |
| `bubble_recruit_rank` | `int` |  | National recruit rank of a "bubble" high-major rotation player (recruiting-model reference point). |
| `bundle_prefix` | `str` |  | Artifact filename prefix under `mbb/models` (`"mbb"` / `"wbb"`). |

### PossCalcFragment {#PossCalcFragment}

`PossCalcFragment(shots_made_or_missed: 'int' = 0, liveball_orbs: 'int' = 0, actual_deadball_orbs: 'int' = 0, ft_events: 'int' = 0, ignored_and_ones: 'int' = 0, bad_fouls: 'int' = 0, offsetting_bad_fouls: 'int' = 0, turnovers: 'int' = 0) -> None`

Running stats needed to calculate possessions for one lineup event,

one direction at a time (`PossessionUtils.PossCalcFragment`,
`PossessionUtils.scala:124-144`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots_made_or_missed` | `int` | `0` | Count of shot attempts (made or missed). |
| `liveball_orbs` | `int` | `0` | Count of live-ball offensive rebounds. |
| `actual_deadball_orbs` | `int` | `0` | Count of dead-ball offensive rebounds. |
| `ft_events` | `int` | `0` | Count of free-throw *sets* (capped-at-1 flag per set). |
| `ignored_and_ones` | `int` | `0` | Count of and-one free throws ignored for possession purposes (capped-at-1 flag). |
| `bad_fouls` | `int` | `0` | Count of technical/flagrant fouls counted against the defending side (capped-at-1 flag). |
| `offsetting_bad_fouls` | `int` | `0` | Count of technical/flagrant fouls that offset (net zero) rather than counting against either side (capped-at-1 flag). |
| `turnovers` | `int` | `0` | Count of turnovers. |

### PossState {#PossState}

`PossState(team_stats: 'PossCalcFragment', opponent_stats: 'PossCalcFragment', prev_clump: 'ConcurrentClump') -> None`

Running state threaded through `calculate_possessions_by_event`

(`PossessionUtils.PossState`, `PossessionUtils.scala:39-49`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_stats` | `PossCalcFragment` |  | Accumulated fragment for the team since the last lineup boundary. |
| `opponent_stats` | `PossCalcFragment` |  | Accumulated fragment for the opponent since the last lineup boundary. |
| `prev_clump` | `ConcurrentClump` |  | The previously-processed merged clump (used by `calculate_stats`'s and-one / deadball-rebound heuristics). |

### PossessionEvent {#PossessionEvent}

`PossessionEvent(dir: 'Direction') -> None`

Decomposes `RawGameEvent`\ s into attacking/defending sides

(`RawGameEvent.PossessionEvent`, `LineupEvent.scala:126-149`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dir` | `Direction` |  | Which team (`Direction.TEAM` / `Direction.OPPONENT`) is currently in possession. |

**Methods**

#### PossessionEvent.attacking_team

`PossessionEvent.attacking_team(ev: 'RawGameEvent') -> 'Optional[str]'`

The event string for the team in possession, or `None`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `RawGameEvent` |  | The raw game event to inspect. |

**Returns**

`ev.team` if `dir` is `Direction.TEAM`, `ev.opponent` if `Direction.OPPONENT`, else `None`.

#### PossessionEvent.defending_team

`PossessionEvent.defending_team(ev: 'RawGameEvent') -> 'Optional[str]'`

The event string for the team NOT in possession, or `None`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `RawGameEvent` |  | The raw game event to inspect. |

**Returns**

`ev.team` if `dir` is `Direction.OPPONENT`, `ev.opponent` if `Direction.TEAM`, else `None`.

### PossessionSplits {#PossessionSplits}

`PossessionSplits(home_off_poss: 'float', away_off_poss: 'float', neutral_off_poss: 'float', total_off_poss: 'float', home_def_poss: 'float', away_def_poss: 'float', neutral_def_poss: 'float', total_def_poss: 'float') -> None`

Home/away/neutral possession totals for one team (`ts:143-152`).

Off and def possessions are bucketed by the game's `location_type`
(missing -> `"Neutral"`). The HCA residual step reads the off/def
imbalance `(home - away) / total` off these totals.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off_poss` | `float` |  |  |
| `away_off_poss` | `float` |  |  |
| `neutral_off_poss` | `float` |  |  |
| `total_off_poss` | `float` |  |  |
| `home_def_poss` | `float` |  |  |
| `away_def_poss` | `float` |  |  |
| `neutral_def_poss` | `float` |  |  |
| `total_def_poss` | `float` |  |  |

### RapmConfig {#RapmConfig}

`RapmConfig(...)`

Port of `RapmConfig` (`RapmUtils.ts:175-179`).

### RapmPlayerContext {#RapmPlayerContext}

`RapmPlayerContext(...)`

Port of `RapmPlayerContext` (`RapmUtils.ts:147-173`).

See the module docstring for why `filtered_lineups` is a Python
callable rather than a materialized dict.

### RapmPreProcDiagnostics {#RapmPreProcDiagnostics}

`RapmPreProcDiagnostics(...)`

Port of `RapmPreProcDiagnostics` (`RapmUtils.ts:187-194`) -- the

multi-collinearity diagnostic `calc_collinearity_diag` returns.

### RapmPriorInfo {#RapmPriorInfo}

`RapmPriorInfo(...)`

Port of `RapmPriorInfo` (`RapmUtils.ts:124-133`).

### RapmProcessingInputs {#RapmProcessingInputs}

`RapmProcessingInputs(...)`

Port of `RapmProcessingInputs` (`RapmUtils.ts:196-203`).

See the module docstring's "Task 3.5 notes" for why `soln_matrix` and
`sd_rapm` are plain nested `list`s rather than `NDArray`s, and why
`sd_rapm` exists at all (a Python-only addition beyond upstream's own
return shape).

### RawGameEvent {#RawGameEvent}

`RawGameEvent(min: 'float', team: 'Optional[str]' = None, opponent: 'Optional[str]' = None) -> None`

A single NCAA play-by-play event line (`LineupEvent.RawGameEvent`,

`LineupEvent.scala:65-105`).

Exactly one of `team` / `opponent` is populated per event -- the raw
string is the literal `"date,time,event"` line from the NCAA website.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The game-clock minute (fractional) this event occurred at. |
| `team` | `Optional[str]` | `None` | The raw event string, if this event belongs to the team under analysis. |
| `opponent` | `Optional[str]` | `None` | The raw event string, if this event belongs to the opponent. |

### RosterEntry {#RosterEntry}

`RosterEntry(player_code_id: 'PlayerCodeId', number: 'str', pos: 'str', height: 'str', height_in: 'Optional[int]', year_class: 'str', gp: 'int', origin: 'Optional[str]', role: 'Optional[str]') -> None`

An entry in an NCAA team roster (`RosterEntry`, ``models/ncaa

/RosterEntry.scala:11-21`). **Scope addition, Task 5e.1** -- the first
model consumed by the HTML-parser layer (`mbb_ncaa_roster_parser.py``).
Appended here (not inserted among the 5a-reviewed classes above) to keep
this an additive-only change, matching `PlayerEvent`'s precedent.

The trailing `role` field (present in the Scala case class shape but
never populated by `RosterParser.parse_roster` -- every construction
site there, both the real-player and coach__` branches, passes the
literal `None` for it) is presumably set by a later phase's box-score
parser (`BoxscoreParser`, out of this task's scope); it is carried
here for shape fidelity even though Task 5e.1's only producer never
populates it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_code_id` | `PlayerCodeId` |  | The player's code + full identity (the roster-row equivalent of a box-score/PbP player reference). |
| `number` | `str` |  | The jersey number, as printed (may be non-numeric text). |
| `pos` | `str` |  | The listed position. |
| `height` | `str` |  | The listed height, in `"FT-IN"` text form (e.g. `"6-3"`). |
| `height_in` | `Optional[int]` |  | `height` parsed to total inches, if it matched `height_regex`. |
| `year_class` | `str` |  | The listed academic year (`"Fr"`/`"So"`/`"Jr"`/ `"Sr"`/etc.). |
| `gp` | `int` |  | Games played. |
| `origin` | `Optional[str]` |  | The player's hometown/prior-school text, if the source table has that column (v1 rosters only). |
| `role` | `Optional[str]` |  | Reserved for a later phase; always `None` from `parse_roster` (see above). |

### ScheduleBuilders {#ScheduleBuilders}

`ScheduleBuilders(team_name_finder: 'Callable[[BeautifulSoup], Optional[str]]', neutral_game_finder: 'Callable[[BeautifulSoup], list[str]]') -> None`

One version-era's HTML finder functions (``TeamScheduleParser

.base_builders`, `TeamScheduleParser.scala:36-39``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_name_finder` | `Callable[[BeautifulSoup], Optional[str]]` |  |  |
| `neutral_game_finder` | `Callable[[BeautifulSoup], list[str]]` |  |  |

### ScoreInfo {#ScoreInfo}

`ScoreInfo(start: 'Score', end: 'Score', start_diff: 'int', end_diff: 'int') -> None`

Score context at the start/end of a lineup event

(`LineupEvent.ScoreInfo`, `LineupEvent.scala:153-158`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start` | `Score` |  | Score at the start of the event. |
| `end` | `Score` |  | Score at the end of the event. |
| `start_diff` | `int` |  | Score differential (team - opponent) at the start. |
| `end_diff` | `int` |  | Score differential (team - opponent) at the end. |

### ShotClockStats {#ShotClockStats}

`ShotClockStats(total: 'int' = 0, early: 'Optional[int]' = None, mid: 'Optional[int]' = None, late: 'Optional[int]' = None, orb: 'Optional[int]' = None) -> None`

Counting stats broken down by shot-clock segment

(`LineupEventStats.ShotClockStats`, `LineupEventStats.scala:51-57`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `total` | `int` | `0` | Count across the entire shot clock. |
| `early` | `Optional[int]` | `None` | Count in the first 10s, if tracked. |
| `mid` | `Optional[int]` | `None` | Count in the middle 10s, if tracked. |
| `late` | `Optional[int]` | `None` | Count in the last 10s, if tracked. |
| `orb` | `Optional[int]` | `None` | Count in the first 10s following an offensive rebound, if tracked (else folded into `mid`/`late` as normal). |

### ShotEvent {#ShotEvent}

`ShotEvent(player: 'Optional[PlayerCodeId]', date: 'datetime', location_type: 'LocationType', team: 'TeamSeasonId', opponent: 'TeamSeasonId', is_off: 'bool', lineup_id: 'Optional[LineupId]', players: 'list[PlayerCodeId]', score: 'Score', min: 'float', loc: 'ShotLocation', geo: 'ShotGeo', dist: 'float', pts: 'int', value: 'int', ast_by: 'Optional[PlayerCodeId]', is_ast: 'Optional[bool]', is_trans: 'Optional[bool]', raw_event: 'Optional[str]') -> None`

Info about one shot taken during a game, all distances in feet

(`ShotEvent`, `models/ncaa/ShotEvent.scala:9-29`). **Scope addition,
Task 5e.5** -- the model produced by
`~sportsdataverse.mbb.mbb_ncaa_shot_parser.create_shot_event_data`.

**Fields `mbb_ncaa_shot_parser.create_shot_event_data` (this task)
actually populates:** `player` (best-effort -- tidy-resolved + coded
for the team under analysis, name-coded only for the opponent),
`date`/`location_type`/`team`/`opponent` (copied from the
box-score lineup), `is_off`, `score` (re-oriented for home/away/
neutral perspective), `min` (ascending game-clock time, after
`phase1_shot_event_enrichment`), `loc`/`dist` (transformed court
coordinates + Euclidean distance from the basket, after the
self-correcting flip pass), `geo` (synthetic lat/lon), `raw_event`
(the SVG `<title>` text, for debugging).

**Fields left as placeholders for a LATER phase (Task 5e.6,
`PlayByPlayUtils`/`ShotEnrichmentUtils`):** `lineup_id` (always
`None` here -- "discard if bad lineup" per the Scala comment, filled in
once the shot is matched against an actual on-floor lineup), `players`
(always `[]` here -- filled in from the matched lineup), `pts`
(**not the real point value** -- this task only sets it to `1`/`0`
for made/missed, matching the Scala's own `// (enrich in final phase)`
comment; the real 2pt/3pt value comes from `PlayByPlayUtils.shot_value`
in Task 5e.6), `value` (always `0` here, same "final phase" note),
`ast_by`/`is_ast`/`is_trans` (always `None` here -- assist/
transition attribution needs the play-by-play cross-reference Task 5e.6
builds).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player` | `Optional[PlayerCodeId]` |  | The shooting player's code + identity, if resolved (`None` is never actually produced by this task's parser, but the type allows for it per the Scala `Option`). |
| `date` | `datetime` |  | The date of the game. |
| `location_type` | `LocationType` |  | Home/away/neutral (etc.) for this game. |
| `team` | `TeamSeasonId` |  | The team under analysis. |
| `opponent` | `TeamSeasonId` |  | The opposing team. |
| `is_off` | `bool` |  | Whether the team under analysis is the one shooting. |
| `lineup_id` | `Optional[LineupId]` |  | The on-floor lineup id, if/when matched (see above). |
| `players` | `list[PlayerCodeId]` |  | The on-floor lineup's players, if/when matched (see above). |
| `score` | `Score` |  | The score at the time of the shot, team-oriented. |
| `min` | `float` |  | The ascending game-clock time (minutes) of the shot. |
| `loc` | `ShotLocation` |  | The shot's transformed court location, in feet. |
| `geo` | `ShotGeo` |  | The shot's synthetic lat/lon. |
| `dist` | `float` |  | The shot's distance from the basket, in feet. |
| `pts` | `int` |  | Made(`1`)/missed(`0`) flag from this task -- NOT the real point value (see above). |
| `value` | `int` |  | Always `0` from this task (see above). |
| `ast_by` | `Optional[PlayerCodeId]` |  | The assisting player, if/when matched (see above). |
| `is_ast` | `Optional[bool]` |  | Whether the shot was assisted, if/when matched (see above). |
| `is_trans` | `Optional[bool]` |  | Whether the shot was in transition, if/when matched (see above). |
| `raw_event` | `Optional[str]` |  | The raw SVG `<title>` text this shot was parsed from, for debugging (discarded before writing to disk upstream). |

### ShotEventBuilders {#ShotEventBuilders}

`ShotEventBuilders(team_finder: 'Callable[[BeautifulSoup], list[str]]', shot_event_finder: 'Callable[[BeautifulSoup], list[Tag]]', script_extractor: 'Callable[[str], Optional[str]]', title_extractor: 'Callable[[Tag], Optional[str]]', event_period_finder: 'Callable[[Tag], Optional[int]]', event_time_finder: 'Callable[[Tag], Optional[float]]', event_player_finder: 'Callable[[Tag], Optional[str]]', shot_location_finder: 'Callable[[Tag], Optional[tuple[float, float]]]', event_score_finder: 'Callable[[Tag], Optional[Score]]', shot_result_finder: 'Callable[[Tag], Optional[bool]]', shot_taking_team_finder: 'Callable[[Tag], Optional[str]]') -> None`

The HTML finder-function table (`ShotEventParser.base_builders`,

`:37-50`). v1-only -- SVG shot maps did not exist in the v0 page
format, so there is exactly one instance (`v1_builders`), unlike
the v0/v1 pairs in every other 5e parser.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_finder` | `Callable[[BeautifulSoup], list[str]]` |  |  |
| `shot_event_finder` | `Callable[[BeautifulSoup], list[Tag]]` |  |  |
| `script_extractor` | `Callable[[str], Optional[str]]` |  |  |
| `title_extractor` | `Callable[[Tag], Optional[str]]` |  |  |
| `event_period_finder` | `Callable[[Tag], Optional[int]]` |  |  |
| `event_time_finder` | `Callable[[Tag], Optional[float]]` |  |  |
| `event_player_finder` | `Callable[[Tag], Optional[str]]` |  |  |
| `shot_location_finder` | `Callable[[Tag], Optional[tuple[float, float]]]` |  |  |
| `event_score_finder` | `Callable[[Tag], Optional[Score]]` |  |  |
| `shot_result_finder` | `Callable[[Tag], Optional[bool]]` |  |  |
| `shot_taking_team_finder` | `Callable[[Tag], Optional[str]]` |  |  |

### ShotGeo {#ShotGeo}

`ShotGeo(lat: 'float', lon: 'float') -> None`

A shot's synthetic lat/lon, for geo-aware visualization tooling

(`ShotEvent.ShotGeo`, `models/ncaa/ShotEvent.scala:45`). **Scope
addition, Task 5e.5** -- flattened per `ShotLocation`'s note.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lat` | `float` |  | Synthetic latitude (feet-to-meters converted, offset from an arbitrary base point -- not a real-world location). |
| `lon` | `float` |  | Synthetic longitude, same convention as `lat`. |

### ShotLocation {#ShotLocation}

`ShotLocation(x: 'float', y: 'float') -> None`

A shot's court-relative coordinates, in feet (`ShotEvent.ShotLocation`,

`models/ncaa/ShotEvent.scala:48`). **Scope addition, Task 5e.5** --
flattened out of the Scala `ShotEvent` companion object per this
module's established nested-object-flattening precedent (see the module
docstring's "Scala idiom decisions": `ScoreInfo`/`PlayerCodeId` were
already flattened out of `LineupEvent`'s companion the same way).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `x` | `float` |  | Feet from the basket; positive is to the right of the basket (facing the goal), negative is to the left. |
| `y` | `float` |  | Feet from the basket along the baseline-perpendicular axis. |

### ShotMapDimensions {#ShotMapDimensions}

`ShotMapDimensions()`

SVG shot-map pixel<->feet conversion constants, taken from the

`svg#court` element (`ShotEventParser.ShotMapDimensions`,
`:568-582`). A plain class (not a dataclass) used purely as a
namespace, mirroring the Scala `object`'s "static singleton" role --
field names are kept snake_case to match the Scala vals verbatim,
letting the ported oracle tests reference e.g.
`ShotMapDimensions.court_length_x_px` 1:1.

### StrengthAdjustedResult {#StrengthAdjustedResult}

`StrengthAdjustedResult(averages: 'dict[str, FieldAverage]', teams: 'list[TeamStrengthAdjusted]') -> None`

The compute output of `build_strength_adjusted_stats`.

Mirrors `main()`'s `{ averages, teams }` object (`ts:656-662`) minus
the `lastUpdated`/`gender`/`year` serialization wrapper.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `averages` | `dict[str, FieldAverage]` |  |  |
| `teams` | `list[TeamStrengthAdjusted]` |  |  |

### StrongSurnameMatch {#StrongSurnameMatch}

`StrongSurnameMatch(box_name: 'str', score: 'int') -> None`

A surname fragment matched and the whole-name score cleared

`MIN_OVERALL_SCORE` (`NameFixer.StrongSurnameMatch`, `:646-647`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `score` | `int` |  | The whole-name similarity score. |

### SubInEvent {#SubInEvent}

`SubInEvent(min: 'float', score: 'Score', player_name: 'str') -> None`

A player subs into the game (`Model.SubInEvent`, `ExtractorUtils.scala:850-853`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |
| `player_name` | `str` |  | The raw or processed name of the player subbing in. |

**Methods**

#### SubInEvent.with_min

`SubInEvent.with_min(new_min: 'float') -> "'SubInEvent'"`

Return a copy with `min` replaced (`:852`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### SubOutEvent {#SubOutEvent}

`SubOutEvent(min: 'float', score: 'Score', player_name: 'str') -> None`

A player subs out of the game (`Model.SubOutEvent`, `ExtractorUtils.scala:854-857`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min` | `float` |  | The ascending game-clock minute of the event. |
| `score` | `Score` |  | The score at the time of the event. |
| `player_name` | `str` |  | The raw or processed name of the player subbing out. |

**Methods**

#### SubOutEvent.with_min

`SubOutEvent.with_min(new_min: 'float') -> "'SubOutEvent'"`

Return a copy with `min` replaced (`:856`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `new_min` | `float` |  |  |

### TeamId {#TeamId}

`TeamId(name: 'str') -> None`

CBB team identifier (`TeamId`, `TeamId.scala`, `AnyVal`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The unique team name. |

### TeamSeasonId {#TeamSeasonId}

`TeamSeasonId(team: 'TeamId', year: 'Year') -> None`

A team's season identifier (`TeamSeasonId`, `TeamSeasonId.scala`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamId` |  | The team playing the season. |
| `year` | `Year` |  | The year the season ends. |

### TeamStrengthAdjusted {#TeamStrengthAdjusted}

`TeamStrengthAdjusted(team_name: 'str', conf: 'str', raw: 'FieldSideMap', adj: 'FieldSideMap', adj_hca: 'FieldSideMap') -> None`

One team's raw / adjusted / HCA-adjusted rates (`ts:642-648`).

Each of `raw` / `adj` / `adj_hca` maps a stat field
(`efg`/`3p`/`2pmid`/`2prim`) to a `{"off": float, "def": float}`
dict. `adj` is the strength-of-schedule-adjusted value; `adj_hca` adds
the home-court term (`off + hca_off`, `def - hca_def`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_name` | `str` |  |  |
| `conf` | `str` |  |  |
| `raw` | `FieldSideMap` |  |  |
| `adj` | `FieldSideMap` |  |  |
| `adj_hca` | `FieldSideMap` |  |  |

### TidyPlayerContext {#TidyPlayerContext}

`TidyPlayerContext(box_lineup: 'LineupEvent', all_players_map: 'dict[str, str]', alt_all_players_map: 'dict[str, list[str]]', resolution_cache: 'dict[str, str]' = <factory>) -> None`

Precomputed box-score lookup tables + resolution cache for

`tidy_player` (`LineupErrorAnalysisUtils.TidyPlayerContext`,
`:31-36`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_lineup` | `LineupEvent` |  | The box-score lineup event this context resolves names against. |
| `all_players_map` | `dict[str, str]` |  | Player code -> full name, for every player in `box_lineup.players`. |
| `alt_all_players_map` | `dict[str, list[str]]` |  | Truncated player code (see truncate_code_1` / truncate_code_2`) -> the list of full names sharing that truncation -- used when the exact code doesn't match but a unique truncated one does. |
| `resolution_cache` | `dict[str, str]` | `<factory>` | Memoizes prior `tidy_player` resolutions. See the module docstring's "Behavioral quirk" note -- this is read by the raw input name but written by the corrected name, faithfully reproducing the upstream asymmetry. |

### ValidationError {#ValidationError}

`ValidationError(*values)`

The 3 ways a lineup can be declared invalid, in Scala declaration

(ordinal) order (`LineupErrorAnalysisUtils.ValidationError`, `:18-20`).
Member order is load-bearing -- see the module docstring's "Return
shape" note.

### WeakSurnameMatch {#WeakSurnameMatch}

`WeakSurnameMatch(box_name: 'str', score: 'int', info: 'str') -> None`

A surname fragment matched, but the whole-name score fell short of

`MIN_OVERALL_SCORE` (`NameFixer.WeakSurnameMatch`, `:644-645`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_name` | `str` |  | The box-score name compared against. |
| `score` | `int` |  | The whole-name similarity score. |
| `info` | `str` |  | Human-readable diagnostic (debug-only; see the fuzzy-match- parity note). |

### add_missing_players {#add_missing_players}

`add_missing_players(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'`

Back-fills a clump whose lineups carry TOO FEW players

(`LineupErrorAnalysisUtils.add_missing_players`, `:315-401`).

Fires only when the clump's first event has `<= 4` on-floor players
(`:324-325`: `players_in.size > 4` is a no-op). The candidate pool is
every box-score player NOT already on the first event's floor
(`:328`), **seeded** with a heuristic (`:352-357`): the `next_good`
lineup's sub-outs minus anyone appearing anywhere in the clump -- a good
lineup that opens by subbing out a player who was never actually on the
floor is a strong signal that player belongs to this under-filled clump.

Walking the clump chronologically (`:359-385`): a candidate who subs IN
is dropped from the pool (they're accounted for), and any remaining
candidate named in a team-side raw play (same `parse_any_play` ->
`tidy_player` -> `build_player_code` chain as
`find_missing_subs`) is collected into `players_to_add`. If
anything was collected, it is appended to **every** event's `players`
(raw list concat, no dedup -- `:388-391`, a verbatim port; an over-add
that pushes an event past 5 players lands it in the still-to-fix bucket
for the second `find_missing_subs` pass in
`analyze_and_fix_clumps` to trim back). The augmented events are
partitioned by `validate_lineup`. If nothing was collected, the
original clump is returned unchanged.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to attempt to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- the now-valid augmented events and a clump of the still-invalid ones (carrying the input's `next_good`); or `([], clump)` on a no-op / nothing-to-add outcome.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    add_missing_players,
)
fixed, still = add_missing_players(clump, box_lineup, valid_codes)
```

### add_stats_to_lineups {#add_stats_to_lineups}

`add_stats_to_lineups(lineup: 'LineupEvent') -> 'LineupEvent'`

Enrich a lineup with play-by-play stats for both team and opponent

(`add_stats_to_lineups`, `LineupUtils.scala:1441-1451`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup event to enrich (not mutated). |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` with `team_stats`/`opponent_stats` populated.

### adjust_efficiency {#adjust_efficiency}

`adjust_efficiency(game_eff: 'pl.DataFrame', *, league: 'str' = 'mens', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'`

Iterative opponent-adjusted efficiency -> AdjO / AdjD / AdjEM per team-season.

KenPom-style fixed point: initialise `adj_o = raw_o` / `adj_d = raw_d`,
then repeatedly recompute each team's rating from its games with the
opponent's *current* adjusted rating and a home-court adjustment removed,
until the largest change is below `tol`. Ratings are computed independently
per season (a team's opponent pool is within-season).

The per-game offensive update is
`off_eff - (adj_d_opp - avg) - loc_o` where `loc_o` is `+hfa/2` at
home, `-hfa/2` away, `0` neutral (defense is symmetric with the opposite
sign); `avg` is the league mean efficiency and `hfa` comes from
`~sportsdataverse.mbb.mbb_prediction_constants.get_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the HFA constant. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest rating change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_o, adj_d, adj_em, raw_o, raw_d, games`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, raw_game_efficiency
ratings = adjust_efficiency(raw_game_efficiency(sched, box))
```
