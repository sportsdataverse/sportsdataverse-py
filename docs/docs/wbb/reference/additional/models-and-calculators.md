---
title: "WBB — additional Python functions — Models and calculators: AssistEvent–calc_lineup"
sidebar_label: "Models and calculators: AssistEvent–calc_lineup"
sidebar_position: 8
description: "WBB — additional Python functions — Models and calculators: AssistEvent–calc_lineup — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — Models and calculators: AssistEvent–calc_lineup

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

**Methods**

#### LineupEventStats.empty

`LineupEventStats.empty() -> "'LineupEventStats'"`

A fresh all-defaults `LineupEventStats` (`:41`).

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

### PlayerId {#PlayerId}

`PlayerId(name: 'str') -> None`

CBB player identifier (`PlayerId`, `PlayerId.scala`, `AnyVal`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | The unique player name. |

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

**Methods**

#### RawGameEvent.for_opponent

`RawGameEvent.for_opponent(s: 'str', min: 'float') -> "'RawGameEvent'"`

Build an opponent-side event (Scala ``RawGameEvent.opponent(s,

min)`, `LineupEvent.scala:109-110` -- renamed per the "Scala
idiom decisions" module note to avoid colliding with the
`opponent`` field).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `s` | `str` |  |  |
| `min` | `float` |  |  |

#### RawGameEvent.for_team

`RawGameEvent.for_team(s: 'str', min: 'float') -> "'RawGameEvent'"`

Build a team-side event (Scala `RawGameEvent.team(s, min)`,

`LineupEvent.scala:107-108` -- renamed per the "Scala idiom
decisions" module note to avoid colliding with the `team` field).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `s` | `str` |  |  |
| `min` | `float` |  |  |

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

**Methods**

#### ScoreInfo.empty

`ScoreInfo.empty() -> "'ScoreInfo'"`

A fresh zeroed `ScoreInfo` (`ScoreInfo.empty`, `:161-166`).

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

### ShotQualityConstants {#ShotQualityConstants}

`ShotQualityConstants(arc_radius_by_season: "'dict[tuple[int, int], float]'" = <factory>, paint_radius_ft: 'float' = 15.0, rim_radius_ft: 'float' = 4.0, corner_x_ft: 'float' = 21.0, corner_y_ft: 'float' = 9.0, shrink_k_zone: 'float' = 100.0, shrink_k_talent: 'float' = 0.0) -> None`

Per-league rule + fitted constants for the shot-quality spine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `arc_radius_by_season` | `dict[tuple[int, int], float]` | `<factory>` | `{(first_season, last_season): radius_ft}` eras for the three-point arc. |
| `paint_radius_ft` | `float` | `15.0` | Outer edge of the paint/floater range (beyond the rim zone, inside this = `paint`). |
| `rim_radius_ft` | `float` | `4.0` | Radius of the `rim` zone. |
| `corner_x_ft` | `float` | `21.0` | `\|x\|` at/beyond this (with a small `y`) = corner 3. |
| `corner_y_ft` | `float` | `9.0` | Baseline band height for the corner-3 test. |
| `shrink_k_zone` | `float` | `100.0` | Empirical-Bayes cell-toward-zone-mean shrinkage (pseudo-attempts; hand-chosen default -- at the fixture cell sizes, n per zone x type cell >> k, so its effect is negligible). |
| `shrink_k_talent` | `float` | `0.0` | Shooter-talent shrinkage (fitted in Phase 3; 0.0 until fit). |

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

### adjust_off_rating_stats {#adjust_off_rating_stats}

`adjust_off_rating_stats(pts_correction_factor: 'float', poss_correction_factor: 'float', mutable_o_rtg: 'ORtgDiagnostics', maybe_raw_o_rtg: 'float | None') -> 'tuple[float, float] | None'`

Apply a missing-possession correction factor to an `ORtgDiagnostics` dict in place.

Faithful port of `RatingUtils.adjustOffRatingStats` (`RatingUtils.ts:993-1033`).
Genuinely public upstream (called from `LineupTableUtils.ts` after a
lineup-level pts/poss reconciliation), so this port is public too.
Recomputes the productivity fields via `build_productivity`
(reused, not re-derived).

**Landmine 4** (see module docstring): the `o_adj = avgEff / defSos or
1` recomputation here is unguarded against `defSos == 0` -- same
reachability analysis as landmine 3 (only reachable if the diagnostics
dict's original `build_o_rtg` call used `avg_efficiency == 0`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pts_correction_factor` | `float` |  | Points correction factor (e.g. team pts / sum of player pts, capped to `[0.95, 1.05]` by callers). |
| `poss_correction_factor` | `float` |  | Possession correction factor, same shape. |
| `mutable_o_rtg` | `ORtgDiagnostics` |  | The `ORtgDiagnostics` dict to mutate in place (`oRtg`, `Usage`, `adjORtg`, `adjORtgPlus`, `Usage_Bonus`, `SoS_Bonus`, `adjPtsFactor`, `adjPossFactor`, and (conditionally) `Raw_Usage` are all updated). |
| `maybe_raw_o_rtg` | `float \| None` |  | The un-overridden raw `oRtg` value (`rawORtg`'s `.value`, or `None` when no override was in play), used to compute the raw-side return. |

**Returns**

`(new_raw_o_rtg, raw_adj_o_rtg_plus)` when both `mutable_o_rtg["Raw_Usage"]` and `maybe_raw_o_rtg` are not `None`; otherwise `None` (.isNil` semantics -- an explicit `0` does NOT count as nil).

**Example**

```python
from sportsdataverse.mbb.mbb_ratings import build_o_rtg, adjust_off_rating_stats

_, _, raw_o_rtg, _, o_diags = build_o_rtg(player, {}, {}, 100.0, True, False)
maybe_raw = raw_o_rtg["value"] if raw_o_rtg else None
adjust_off_rating_stats(1.1, 0.9, o_diags, maybe_raw)
print(o_diags["oRtg"], o_diags["adjORtgPlus"])
```

### adjust_tempo {#adjust_tempo}

`adjust_tempo(game_eff: 'pl.DataFrame', *, league: 'str' = 'mens', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'`

Opponent-adjusted tempo (possessions/40) per team-season.

Same fixed point as `adjust_efficiency`, applied to game possessions
under the additive model `poss = tempo_i + tempo_j - avg`: a team's tempo
is recovered by removing its opponents' current adjusted tempo. `avg` is
the league baseline tempo from
`~sportsdataverse.mbb.mbb_prediction_constants.get_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_eff` | `DataFrame` |  | Output of `raw_game_efficiency`. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the tempo baseline. |
| `max_iter` | `int` | `100` | Maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | Convergence tolerance on the largest tempo change. |

**Returns**

One row per (season, team_id): `season, team_id, adj_tempo`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import adjust_tempo, raw_game_efficiency
tempo = adjust_tempo(raw_game_efficiency(sched, box))
```

### aggregate_player_seasons {#aggregate_player_seasons}

`aggregate_player_seasons(seasons: "'list[int]'", *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Canonical per-player-season counting frame from the boxscore release.

Sums the per-game player boxscores into one row per (player_id, season,
team_id) with the counting columns `player_per100_features` expects.
Shot-location splits come from the shots release (2025+): free throws
(`MadeFreeThrow`) are excluded, layup/dunk/tip = rim, and jump shots
split three vs mid by `score_value` (the release's `type_text` carries
no three-point marker; `score_value` is populated on misses too). For
seasons without shots data, three-point attempts come from the box and
all remaining attempts fold into `fga_mid`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to aggregate. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

One row per (player_id, season, team_id): `player_id:Utf8, season, team_id:Utf8, player, minutes` + the counting columns + `fga_rim, fga_mid, fga_three`. Empty input returns zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import (
    aggregate_player_seasons, player_per100_features,
)
feats = player_per100_features(aggregate_player_seasons([2025]))
```

### apply_weak_priors {#apply_weak_priors}

`apply_weak_priors(field: 'str', player_poss_pcts: 'list[float]', prior_info: 'RapmPriorInfo', debug_mode: 'bool' = False) -> 'Callable[[float, list[float]], list[float]]'`

Build a closure that nudges ridge-regressed RAPM back towards its weak prior.

Faithful port of `RapmUtils.applyWeakPriors` (`RapmUtils.ts:921-995`).
Ridge regression depresses estimates towards `0`; this "fills" the
team-total error (see `pick_ridge_regression`'s
`[IMPORTANT-EQUATION-01]` team-total reconciliation) back in using each
player's weak (KenPom-derived) prior as the fallback signal, capped so no
more than half the team-total error gets attributed via this path
(`max_multiplier = -0.5`) -- an alternate flat-translation path
(`use_alt_rating`) kicks in for `off_adj_ppp`/`def_adj_ppp` fields
when the capped path can't fully explain the error.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | The prior key to read off each `prior_info["players_weak"]` entry, e.g. `"off_adj_ppp"`. |
| `player_poss_pcts` | `list[float]` |  | Per-player possession-share weights (index-aligned with `prior_info["players_weak"]`), e.g. `pick_ridge_regression`'s own `pct_by_player[off_or_def]`. |
| `prior_info` | `RapmPriorInfo` |  | A `RapmPriorInfo` (only `["players_weak"]` is read). |
| `debug_mode` | `bool` | `False` | Kept for TS signature parity -- upstream gates a `console.log` behind this flag (`RapmUtils.ts:979-984`), which this port deliberately does not reproduce: every production call site pins it `False` (`offDefDebugMode.off`/`.def` are hardcoded `False` constants inside `pickRidgeRegression`), so it is dead in every current caller and would only ever emit console noise, not test-observable behavior. |

**Returns**

A closure `(error, base_results) -> adjusted_results` -- call it with the team-total efficiency error and the pre-adjustment RAPM vector to get the weak-prior-nudged result.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import apply_weak_priors

nudge = apply_weak_priors("off_adj_ppp", pct_by_player, ctx["prior_info"])
adjusted = nudge(adj_eff_err_pre_prior, results_pre_prior)
```

### as_of_ratings_split {#as_of_ratings_split}

`as_of_ratings_split(results: 'pl.DataFrame', cutoff_date: 'datetime.date') -> 'pl.DataFrame'`

Filter a results frame to games strictly before a cutoff date (leakage boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `results` | `DataFrame` |  | A `polars.DataFrame` with a `date` column. |
| `cutoff_date` | `date` |  | Games on or after this date are excluded. |

**Returns**

A `polars.DataFrame` containing only rows with `date < cutoff_date`.

**Example**

```python
import datetime as dt
from sportsdataverse._common.metrics import as_of_ratings_split
as_of_ratings_split(results, dt.date(2023, 9, 8))
```

### as_of_season_split {#as_of_season_split}

`as_of_season_split(df: 'pl.DataFrame', target_season: 'int') -> 'pl.DataFrame'`

Rows strictly before `target_season` -- the leakage boundary.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame with an integer `season` column. |
| `target_season` | `int` |  | The season being predicted; its rows (and later) drop. |

**Returns**

The subset with `season < target_season`.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import as_of_season_split
prior = as_of_season_split(df, 2026)
```

### bootstrap_ari {#bootstrap_ari}

`bootstrap_ari(fit_fn: "'Callable[[np.ndarray], tuple[np.ndarray, np.ndarray]]'", X: 'np.ndarray', n_boot: 'int' = 20, seed: 'int' = 0) -> 'float'`

Cluster stability: mean ARI between the full fit and bootstrap refits.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fit_fn` | `Callable[[ndarray], tuple[ndarray, ndarray]]` |  | `X -> (centers, labels)` (e.g. a seeded `kmeans_fit` partial). |
| `X` | `ndarray` |  | Feature matrix. |
| `n_boot` | `int` | `20` | Bootstrap resamples. |
| `seed` | `int` | `0` | RNG seed. |

**Returns**

Mean adjusted Rand index of the resample fits' assignments (of the FULL sample, via nearest refit center) vs the full-fit labels.

**Example**

```python
from functools import partial
score = bootstrap_ari(lambda Z: kmeans_fit(Z, 8, seed=0), Z, n_boot=20, seed=0)
```

### brier_score {#brier_score}

`brier_score(y_true: 'np.ndarray', p_pred: 'np.ndarray') -> 'float'`

Mean squared error between predicted probabilities and binary outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |

**Returns**

The Brier score (0.0 is a perfect forecast).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import brier_score
brier_score(np.array([1, 0]), np.array([0.9, 0.1]))
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

### build_wbb_season_wp {#build_wbb_season_wp}

`build_wbb_season_wp(season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

A WBB season's play-by-play with win-probability columns joined in.

Delegates to `sportsdataverse.mbb.mbb_win_prob.build_mbb_season_wp`
with `league="womens"` (WBB loaders + women's constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (e.g. `2024`); bounded by `load_wbb_pbp` release availability. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The season's `load_wbb_pbp` frame with `pregame_home_prob` + `home_win_prob` appended -- see the mbb core for the contract.

**Example**

```python
from sportsdataverse.wbb import build_wbb_season_wp
wp = build_wbb_season_wp(2024)
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

### calc_collinearity_diag {#calc_collinearity_diag}

`calc_collinearity_diag(weight_matrix: 'NDArray[np.float64]', ctx: 'RapmPlayerContext') -> 'RapmPreProcDiagnostics'`

Multi-collinearity diagnostic between the players in an off/def design matrix.

Faithful port of `RapmUtils.calcCollinearityDiag` (`RapmUtils.ts:1629-1760`).
Runs an SVD of `weight_matrix`, builds condition indices ("lineup
combos") from the ratio of the largest to each singular value, and a
variance-decomposition-proportions ("VDP") matrix identifying which
players load onto which collinear combo -- the classic Belsley-Kuh-Welsch
collinearity-diagnostics recipe (see the upstream comment's
[colldiag.m](https://github.com/brian-lau/colldiag/blob/master/colldiag.m)
citation). Also builds a plain Pearson player/player correlation matrix
(calc_player_correlations`) and folds it into a possession
-weighted `adaptive_correl_weights` summary per player.

**`numpy.linalg.svd(weight_matrix, full_matrices=False)` replaces
`svd-js`'s `SVD(weightMatrix, false)`.** Both are the standard
Golub-Kahan-Reinsch decomposition (`A = U @ diag(S) @ Vᵀ`); numpy's
`Vh` return value already *is* `Vᵀ` (what the TS code separately
computes via `transpose(matrix(v))`), so this port skips that
transpose. The TS code (and this port) never reads `u`/the first SVD
return -- only `q`/`S` (singular values) and `v`/`Vᵀ`. Singular
-vector **sign is immaterial here**: every place `V` is used
(`phiMatrix`/`phi_matrix`) squares each entry (`val * val`), and a
per-singular-value sign flip on `U`/`V` together is a valid SVD
regardless -- so any `U`/`V` sign convention difference between
`svd-js` and LAPACK (numpy's backend) cannot change this function's
output. **Singular-value ordering is likewise immaterial**: both this
port and the TS source explicitly re-sort `q` (ascending, carrying the
original index along) before using it, so whichever order either SVD
implementation returns values in, the final result only depends on the
*values themselves* (up to the explicit resort), not on numpy's native
descending convention vs whatever order `svd-js` happens to return.

**`correl_matrix`/`poss_correl_matrix` stay `numpy.ndarray`** (see
the module docstring's "Task 3.6 notes" for why this doesn't hit the
Task 3.5 "`ndarray` breaks deep `==`" concern).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `weight_matrix` | `NDArray[float64]` |  | An off/def design matrix, shape `(num_lineups, ctx["num_players"])` (e.g. `calc_player_weights`'s first return value, or a hand-built matrix for isolated testing). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`. `ctx["num_players"]` sizes every per-player structure; `ctx["col_to_player"]` keys `player_combos`. |

**Returns**

A `RapmPreProcDiagnostics`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_collinearity_diag, calc_player_weights

off_weights, _ = calc_player_weights(ctx)
diag = calc_collinearity_diag(off_weights, ctx)
print(diag["lineup_combos"][0])  # the worst-conditioned combo
```

### calc_lineup_outputs {#calc_lineup_outputs}

`calc_lineup_outputs(field: 'str', off_offset: 'float', def_offset: 'float', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None' = None, use_old_val_if_possible: 'tuple[bool, bool]' = (False, False)) -> 'list[NDArray[np.float64]]'`

Build the off/def target vectors the RAPM design matrices are fit against.

Faithful port of `RapmUtils.calcLineupOutputs` (`RapmUtils.ts:598-751`).
For each filtered lineup, computes a possession-weighted residual: the
lineup's own stat value, plus any global luck adjustment, minus the
accumulated "prior offset" contributed by every player on the lineup
(a strong-prior blend for kept players -- see get_strong_weight`
-- or a fixed baseline contribution for removed players).

Upstream keeps this as a plain `Array<Array<number>>` (*not* a mathjs
`Matrix`, unlike `calc_player_weights`'s `offWeights`/
`defWeights` -- `RapmUtils.test.ts`'s own `tidyResults` helper for
this function has a visibly different shape, see the classification map
in `tests/fixtures/hoop_explorer/README.md`). This port still
materializes both output vectors as `numpy.ndarray` for consistency
with `calc_player_weights` at the same dict -> array boundary --
Task 3.4's ridge-regression solve consumes both as arrays regardless of
the upstream distinction.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | The stat suffix to read off each lineup, e.g. `"adj_ppp"` (read as `{prefix}_{field}`, e.g. `"off_adj_ppp"`). |
| `off_offset` | `float` |  | The D1-average offensive value for `field` (the regression's starting/baseline value on the RHS). |
| `def_offset` | `float` |  | The D1-average defensive value for `field`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`, e.g. from `build_player_context`. |
| `adaptive_correl_weights` | `list[float] \| None` | `None` | Optional per-player adaptive-correlation weights (index-aligned with `ctx["col_to_player"]`), used as the strong-prior blend fallback when `ctx["prior_info"] ["strong_weight"] < 0` -- see get_strong_weight`. |
| `use_old_val_if_possible` | `tuple[bool, bool]` | `(False, False)` | `(use_old_val_for_off, use_old_val_for_def)` -- whether to prefer each lineup/team stat's luck-adjusted `old_value` over its raw `value` when present. This is the luck-adjustment hook Task 3.1's classification map flags as an **inherited coverage gap**: the vendored oracle fixture has `old_value == value` on every field (via `insertOldValues`), so neither jest nor this port's replay test ever observes this flag change the resulting numbers -- only that passing it doesn't crash. See the module docstring's "Task 3.3 coverage gap" note. |

**Returns**

`[off_outputs, def_outputs]` -- two 1-D `numpy.ndarray` target vectors, index-aligned with `ctx["filtered_lineups"]("off"/"def")` (plus one extra element each when `ctx["unbias_weight"] > 0`, an "unbiasing observation" target -- always unreached in production, same as `calc_player_weights`'s extra row).

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_lineup_outputs

off_outputs, def_outputs = calc_lineup_outputs(
    "adj_ppp", 100.0, 100.0, ctx
)
print(off_outputs.shape)  # (num_off_lineups,)

# Luck-adjusted variant (reads ``old_value`` where present)

off_luck, def_luck = calc_lineup_outputs(
    "adj_ppp", 100.0, 100.0, ctx, use_old_val_if_possible=(True, True)
)
```
