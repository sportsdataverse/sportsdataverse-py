---
title: "WBB — additional Python functions — Models and calculators: AssistEvent–in_game"
sidebar_label: "Models and calculators: AssistEvent–in_game"
sidebar_position: 6
description: "WBB — additional Python functions — Models and calculators: AssistEvent–in_game — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — Models and calculators: AssistEvent–in_game

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

### Year {#Year}

`Year(value: 'int') -> None`

CBB season, named by the year it ends (`Year`, `Year.scala`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `value` | `int` |  | The ending year of the season. |

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

### calc_player_weights {#calc_player_weights}

`calc_player_weights(ctx: 'RapmPlayerContext') -> 'list[NDArray[np.float64]]'`

Build the off/def player-weight (design) matrices for the RAPM solve.

Faithful port of `RapmUtils.calcPlayerWeights` (`RapmUtils.ts:544-595`).
One row per (filtered) lineup, one column per remaining player; each
filled cell is `sqrt(lineup_possessions / total_side_possessions)` --
the possession-weighted design-matrix entry the ridge regression (Task
3.4) solves against. This is the first function in the module where a
`dict`-shaped `RapmPlayerContext` gets materialized into a
`numpy.ndarray` -- see the module docstring's "dict -> `numpy.ndarray`
boundary" note.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`, e.g. from `build_player_context`. |

**Returns**

`[off_weights, def_weights]` -- two `numpy.ndarray` matrices of shape `(num_{off,def}_lineups [+1 if ctx["unbias_weight"] > 0], ctx["num_players"])`. The optional extra row (only emitted when `ctx["unbias_weight"] > 0` -- always `0.0` in production per `build_player_context`'s hardcoded local, but settable directly on the returned context dict, as the oracle test does) holds each column's `unbias_weight`-scaled sum-of-squares, an "unbiasing observation" row (`RapmUtils.ts:578-593`).

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_player_weights

off_weights, def_weights = calc_player_weights(ctx)
print(off_weights.shape)  # (num_off_lineups, num_players)
```

### calc_slow_pseudo_inverse {#calc_slow_pseudo_inverse}

`calc_slow_pseudo_inverse(player_weight_matrix: 'NDArray[np.float64]', ridge_lambda: 'float', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'`

Per-parameter variance terms for the ridge-regression standard errors.

Faithful port of the private `RapmUtils.calcSlowPseudoInverse`
(`RapmUtils.ts:1544-1557`): the same `(XᵀX + ridge_lambda·I)⁻¹` as
`slow_regression`'s `bottomInv`, but this function returns the
square root of its diagonal instead of the full solver matrix -- the
`paramErrs` term consumed by the standard-error formula (see
`calculate_sd_rapm`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, same shape as `slow_regression`'s. |
| `ridge_lambda` | `float` |  | The Tikhonov regularization strength (must match the `ridge_lambda` used to build the corresponding `slow_regression` solver, for the SEs to be meaningful). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` -- only `ctx["num_players"]` is read. |

**Returns**

A length-`num_players` array, `sqrt(diag((XᵀX + λI)⁻¹))`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calc_slow_pseudo_inverse

param_errs = calc_slow_pseudo_inverse(x, 1.0, ctx)
```

### calculate_aggregated_lineup_stats {#calculate_aggregated_lineup_stats}

`calculate_aggregated_lineup_stats(lineups: 'list[LineupStatSet] | None') -> 'LineupStatSet'`

Combine all lineups into a single team stat set.

Faithful port of `LineupUtils.calculateAggregatedLineupStats`
(`LineupUtils.ts:106`). Seeds an accumulator from
`StatModels.emptyLineup()` (`{"key": "empty", "doc_count": 0}`) plus
an `all_lineups` sub-accumulator of the same shape, then merges every
lineup via `weighted_avg`: lineups without a truthy `rapmRemove`
key merge into the main accumulator, while `rapmRemove` lineups merge
into `all_lineups` instead (their contribution is folded back in
afterward). Calls `complete_weighted_avg` to turn the main
accumulator's weighted sums into weighted averages, then -- because
`StatModels.emptyLineup()` always carries `key`/`doc_count` and so
is never considered "empty" by the upstream `lodash.isEmpty` check --
unconditionally re-merges the (now-averaged) team totals into
`all_lineups` and finishes that sub-accumulator too. Finally rebuilds
`off_net` / `off_raw_net` via `build_efficiency_margins`
(value-key always; old-value-key too when the team is in luck-adjusted
mode, i.e. `off_ppp.old_value` is present) -- but only on the top-level
result, matching upstream's "don't bother for all_lineups" comment.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `list[LineupStatSet] \| None` |  | The per-lineup `LineupStatSet` docs to fold together (e.g. the ES aggregation buckets under `responses[0].aggregations.lineups.buckets`). `None` or an empty list yields an all-zero/empty team stat set (mirrors the upstream `lineups \|\| []` guard). |

**Returns**

The aggregated team-total `LineupStatSet`, including a nested `all_lineups` key holding the `rapmRemove`-lineups-plus-team-total composite sub-aggregate.

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import calculate_aggregated_lineup_stats

buckets = raw_response["responses"][0]["aggregations"]["lineups"]["buckets"]
team_info = calculate_aggregated_lineup_stats(buckets)
print(team_info["off_ppp"]["value"], team_info["off_poss"]["value"])

# RAPM-exclusion flag

buckets[1]["rapmRemove"] = True  # divert into all_lineups instead
team_info = calculate_aggregated_lineup_stats(buckets)
```

### calculate_possessions {#calculate_possessions}

`calculate_possessions(lineup_events: 'Iterable[LineupEvent]') -> 'list[LineupEvent]'`

Top-level entry point: calculate team/opponent possessions for a

sequence of lineup events (`PossessionUtils.calculate_possessions`,
`PossessionUtils.scala:371-379`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `Iterable[LineupEvent]` |  | The lineups to enrich, in chronological order. |

**Returns**

The lineups, each enriched with possession counts.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_possessions import calculate_possessions

enriched = calculate_possessions(lineups)
enriched[0].team_stats.num_possessions
```

### calculate_possessions_by_event {#calculate_possessions_by_event}

`calculate_possessions_by_event(raw_events_as_clumps: 'Iterable[ConcurrentClump]') -> 'list[LineupEvent]'`

Drive the batch loop + per-clump scoring over an already-flattened

clump stream (`PossessionUtils.calculate_possessions_by_event`,
`PossessionUtils.scala:521-573`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `raw_events_as_clumps` | `Iterable[ConcurrentClump]` |  | The unbatched clump stream, e.g. from flat-mapping `lineup_as_raw_clumps` over several lineups. |

**Returns**

The lineups, each enriched with possession counts, in original order.

### calculate_predicted_out {#calculate_predicted_out}

`calculate_predicted_out(player_weight_matrix: 'NDArray[np.float64]', regressed_players: 'list[float]', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'`

Predict per-lineup outputs from fitted per-player RAPM values.

Faithful port of `RapmUtils.calculatePredictedOut` (`RapmUtils.ts:1559-1567`).
`ctx` is accepted for signature parity with the TS source but unused in
the body (ported verbatim -- upstream's own `ctx` param is likewise
dead in this function).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, shape `(num_lineups, num_players)`. |
| `regressed_players` | `list[float]` |  | The fitted per-player values (e.g. the final, strong-prior-blended RAPM from Task 3.5's `pickRidgeRegression`, or a raw `calculate_rapm` output), length `num_players`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (unused). |

**Returns**

The predicted per-lineup value, length `num_lineups` -- feed into `calculate_residual_error` alongside the actual lineup outputs.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_predicted_out

predicted = calculate_predicted_out(x, [0.875, 1.375], ctx)
```

### calculate_rapm {#calculate_rapm}

`calculate_rapm(regression_matrix: 'NDArray[np.float64]', player_outputs: 'list[float]') -> 'NDArray[np.float64]'`

Apply a regression solver matrix to a target-outputs vector.

Faithful port of `RapmUtils.calculateRapm` (`RapmUtils.ts:772-775`).
Note the TS signature carries no `ctx` parameter (unlike its solve-layer
siblings) -- ported verbatim, param-for-param.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `regression_matrix` | `NDArray[float64]` |  | The `(num_players, num_lineups)` solver from `slow_regression`. |
| `player_outputs` | `list[float]` |  | The per-lineup target vector, length `num_lineups` (e.g. `calc_lineup_outputs`'s `off_outputs`/`def_outputs`). |

**Returns**

The per-player RAPM estimate, length `num_players`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_rapm

rapm = calculate_rapm(solver, [1.0, 2.0, 3.0])
print(rapm.shape)  # (num_players,)
```

### calculate_residual_error {#calculate_residual_error}

`calculate_residual_error(player_outs: 'list[float]', regressed_outs: 'list[float]', ctx: 'RapmPlayerContext') -> 'float'`

Sum of squared residuals between actual and predicted lineup outputs.

Faithful port of `RapmUtils.calculateResidualError` (`RapmUtils.ts:1569-1579`).
`ctx` is accepted for signature parity but unused in the body (dead
upstream too).

**NaN/shape regime (landmine 7):** TS zips the two arrays via lodash
.zip` (pads the shorter side with `undefined`, so a length
mismatch silently contributes `NaN` to the running sum via
`undefined - number`) then reduces with plain `+`. This port instead
subtracts the two as `numpy` arrays: a length mismatch **raises**
`ValueError` (numpy broadcast rules), rather than the TS silent-NaN
behavior -- not reachable via either language's own call sites (both
arguments are always index-aligned to the same lineup count in
production), so this is a divergence in dead territory, not a fixed bug.
A `NaN` *value already present* inside either input (as opposed to a
length mismatch) propagates through the `numpy` subtraction/sum
exactly as it would through the JS arithmetic (both regimes:
numpy-propagate).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_outs` | `list[float]` |  | The actual per-lineup target values (e.g. `calc_lineup_outputs`'s output). |
| `regressed_outs` | `list[float]` |  | The predicted per-lineup values (e.g. `calculate_predicted_out`'s output). |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (unused). |

**Returns**

`sum((player_outs[i] - regressed_outs[i]) ** 2)` -- the `errSq` term consumed by `calculate_sd_rapm`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_residual_error

err_sq = calculate_residual_error([1.0, 2.0, 3.0], [0.875, 1.375, 2.25], ctx)
```

### calculate_sd_rapm {#calculate_sd_rapm}

`calculate_sd_rapm(param_errs: 'NDArray[np.float64]', err_sq: 'float', num_lineups: 'int', num_players: 'int') -> 'NDArray[np.float64]'`

Per-player RAPM standard errors.

Faithful port of the inline `sdRapm` computation in
`RapmUtils.pickRidgeRegression` (`RapmUtils.ts:1373-1390`, not itself
a named TS function -- promoted to a standalone, independently testable
helper here since Task 3.4's brief calls out the formula explicitly).
Cites [arXiv:1509.09169](https://arxiv.org/pdf/1509.09169.pdf).

**Two NaN/error regimes (landmines 8-9):**

8. `dof_inv = 1.0 / (num_lineups - num_players)` -- if
   `num_lineups == num_players` exactly, JS silently produces
   `Infinity` (float division by zero); this port instead **raises**
   `ZeroDivisionError` (Python float division by zero), matching this
   module's already-established landmine-2 convention (unguarded
   division, Python-raises vs JS-Infinity/NaN). Not reachable via the
   oracle fixtures (`num_off_lineups`/`num_def_lineups` always
   comfortably exceed `num_players` there).
9. `sqrt(sqrt(param_errs) * err_sq * dof_inv)` -- a negative
   `param_errs` entry (only possible if `XᵀX + λI` isn't actually
   positive-definite, e.g. `ridge_lambda < 0`) silently
   **numpy-propagates** to `NaN` (matching JS `Math.sqrt(negative)
   -> NaN`, with a `RuntimeWarning` rather than a raise) -- both
   language regimes agree here, unlike landmine 8.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `param_errs` | `NDArray[float64]` |  | Per-player variance terms from `calc_slow_pseudo_inverse`, length `num_players`. |
| `err_sq` | `float` |  | The residual sum of squares from `calculate_residual_error`. |
| `num_lineups` | `int` |  | `ctx["num_off_lineups"]` or `ctx["num_def_lineups"]` (whichever side `param_errs`/`err_sq` were computed for). |
| `num_players` | `int` |  | `ctx["num_players"]`. |

**Returns**

A length-`num_players` array of per-player RAPM standard errors.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import calculate_sd_rapm

sd_rapm = calculate_sd_rapm(param_errs, err_sq, num_lineups=3, num_players=2)
```

### calculate_stats {#calculate_stats}

`calculate_stats(clump: 'ConcurrentClump', prev: 'ConcurrentClump', dir: 'Direction') -> 'PossCalcFragment'`

Calculate one direction's possession-fragment for one merged clump

(`PossessionUtils.calculate_stats`, `PossessionUtils.scala:170-369`).

See the upstream source's inline worked examples (and-one detection,
technical/flagrant offsetting, the deadball-rebound heuristic) for the
hand-annotated NCAA play-by-play snippets that motivate each step; this
port reproduces every step in the same order.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `ConcurrentClump` |  | The merged clump to score. |
| `prev` | `ConcurrentClump` |  | The previously-processed merged clump (feeds the and-one and deadball-rebound heuristics -- see below). |
| `dir` | `Direction` |  | Which side (`Direction.TEAM`/`Direction.OPPONENT`) is "attacking" for this calculation. Named to match the Scala (shadows the `dir` builtin -- consistent with this port's existing precedent of naming params after their Scala originals, e.g. `RawGameEvent.for_team`'s `min`). |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.PossCalcFragment` for this clump/direction.

### calibration_table {#calibration_table}

`calibration_table(y_true: 'np.ndarray', p_pred: 'np.ndarray', n_bins: 'int' = 10) -> 'pl.DataFrame'`

Bucket predicted probabilities into bins and compare to actual outcome rates.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `n_bins` | `int` | `10` | Number of equal-width probability bins. |

**Returns**

A `polars.DataFrame` with columns `bin_mid`, `mean_pred`, `mean_actual`, `n` (one row per non-empty bin).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import calibration_table
calibration_table(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
```

### fit_shrinkage_k {#fit_shrinkage_k}

`fit_shrinkage_k(scored: 'pl.DataFrame', *, seed: 'int' = 0) -> 'float'`

Fit the talent shrinkage `k` split-half (see module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output. |
| `seed` | `int` | `0` | Split seed (deterministic fit). |

**Returns**

The `k` in `[1, 5000]` minimizing `talent_split_mse`.

**Example**

```python
from sportsdataverse.mbb.mbb_shooter_talent import fit_shrinkage_k
k = fit_shrinkage_k(scored)
```

### get_constants {#get_constants}

`get_constants(league: 'str') -> 'ShotQualityConstants'`

League constants bundle for the shot-quality spine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"mens"` or `"womens"`. |

**Returns**

The frozen `ShotQualityConstants` for that league.

**Example**

```python
from sportsdataverse.mbb.mbb_shot_quality_constants import get_constants
get_constants("mens").rim_radius_ft
```

### get_player_value_constants {#get_player_value_constants}

`get_player_value_constants(league: 'str') -> 'PlayerValueConstants'`

Return the `PlayerValueConstants` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"mens"` or `"womens"`. |

**Returns**

The league's `PlayerValueConstants`.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import get_player_value_constants
get_player_value_constants("mens").bundle_prefix
```

### in_game_features {#in_game_features}

`in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'`

Per-play in-game win-probability features from a `load_mbb_pbp` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame with `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id` (event team) and `home_team_id` (the `load_mbb_pbp` schema). |
| `pregame_home_prob` | `float` |  | The pregame home win probability (e.g. from `win_prob_from_margin`), encoded as a constant logit column. Clipped to `[1e-6, 1 - 1e-6]` so a saturated CDF (exact 0/1) cannot crash the logit. |

**Returns**

One row per input play: `score_diff` (home - away), `sec_left` (clipped at 0 -- overtime plays count as 0 seconds left), `sqrt_sec_left`, `pregame_logit`, `home_has_ball` (`Int8`; dead-ball / unknown-team plays are 0).

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import in_game_features
from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
feats = in_game_features(pbp, 0.62)
```
