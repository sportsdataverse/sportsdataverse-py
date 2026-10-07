---
title: "WBB — additional Python functions — stats.ncaa.org: enrich_stats–shot_js"
sidebar_label: "stats.ncaa.org: enrich_stats–shot_js"
sidebar_position: 3
description: "WBB — additional Python functions — stats.ncaa.org: enrich_stats–shot_js — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — stats.ncaa.org: enrich_stats–shot_js

### enrich_stats {#enrich_stats}

`enrich_stats(lineup: 'LineupEvent', event_parser: 'PossessionEvent', stats: 'LineupEventStats', player_filter_coder: 'Optional[PlayerFilterCoder]' = None, player_index: 'int' = -1) -> 'LineupEventStats'`

Fold a lineup's raw events into a counting-stat tree (``protected def

enrich_stats`, `LineupUtils.scala:115-162``). Reuses the Task 5a.3
concurrent-clump batching (`~sportsdataverse.mbb.mbb_ncaa_possessions
.lineup_as_raw_clumps` + `~sportsdataverse.mbb.mbb_ncaa_possessions
.concurrent_event_handler`) rather than duplicating it -- both were
already public/exported from Task 5a.3.

`stats` is deep-copied once up front (see the module docstring's
"Scala idiom decisions"), so this function never mutates the caller's
`stats` argument -- safe to call repeatedly against the same starting
literal (e.g. a shared "empty stats" fixture).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup whose `raw_game_events` to fold over. |
| `event_parser` | `PossessionEvent` |  | Selects which side (team/opponent) is "attacking". |
| `stats` | `LineupEventStats` |  | The starting stat tree (not mutated -- see above). |
| `player_filter_coder` | `Optional[PlayerFilterCoder]` | `None` | Optional `name -> (is_this_player, code)` predicate/coder, for per-player scoping (Task 5c.4). |
| `player_index` | `int` | `-1` | Lineup-slot index for `~sportsdataverse.mbb .mbb_ncaa_models.PlayerShotInfo` tuples (Task 5c.4; `-1` for team-level calls, the only value exercised before then). |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEventStats` with every matching event folded in.

### ensure_ev_uniqueness {#ensure_ev_uniqueness}

`ensure_ev_uniqueness(clump: 'ConcurrentClump') -> 'ConcurrentClump'`

Nudge each event's `min` by a tiny per-index delta so truly

concurrent (identical-`min`) events within a clump don't collapse
under `==` (`ensure_ev_uniqueness`, `LineupUtils.scala:105-111`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `ConcurrentClump` |  | The clump whose events to nudge. |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_possessions.ConcurrentClump` with each event's `min` incremented by `1e-6 * index`.

### extract_player_from_ev {#extract_player_from_ev}

`extract_player_from_ev(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', tidy_ctx: 'TidyPlayerContext') -> 'Optional[PlayerCodeId]'`

Resolve the player named in `pbp_event` to a

`~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId`
(`ShotEnrichmentUtils.extract_player_from_ev`,
`PlayByPlayUtils.scala:613-635`).

For a shot by the team under analysis (`shot.is_off`) the name is
tidied against the box score before coding (so a mis-spelled play-by-play
name resolves to the roster identity); for an opponent shot it is coded
verbatim with no team context.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched (only `is_off` is read). |
| `pbp_event` | `MiscGameEvent` |  | The play-by-play event naming the player. |
| `tidy_ctx` | `TidyPlayerContext` |  | The name-resolution context for this game. |

**Returns**

The resolved `PlayerCodeId`, or `None` if the event string names no player (`~sportsdataverse.mbb.mbb_ncaa_events.parse_any_play` found nothing).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import extract_player_from_ev
pc = extract_player_from_ev(shot, pbp_event, tidy_ctx)
```

### field_keys {#field_keys}

`field_keys(field: 'str') -> 'dict[str, str]'`

Off/def stat-key names for a field (`fieldKeys`, `ts:77-79`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `field` | `str` |  | A stat field (`"efg"` / `"3p"` / `"2pmid"` / `"2prim"`). |

**Returns**

`{"off": f"off_{field}", "def": f"def_{field}"}`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import field_keys

keys = field_keys("3p")
print(keys["off"], keys["def"])  # off_3p def_3p
```

### filter_matching_own {#filter_matching_own}

`filter_matching_own(tags: 'list[Tag]', regex: 'str') -> 'list[Tag]'`

JSoup `:matchesOwn(regex)` applied to an already-computed candidate

list, rather than a fresh `root.select(selector)` call (Task 5e.2
addition; see the module docstring's note on composing this with
`attr_regex_filter`).

Same own-text-only semantics as `select_matching_own` -- JSoup's
`Element.ownText()` walks only the element's direct `TextNode`
children, not text nested inside child elements.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `tags` | `list[Tag]` |  | Candidate tags to filter (typically the result of an earlier `.select()`/`attr_regex_filter` call). |
| `regex` | `str` |  | The pattern each candidate's own (whitespace-collapsed) text must `re.search`-match. |

**Returns**

The subset of `tags` whose own text contains a `regex` match, in the input list's order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import attr_regex_filter, filter_matching_own, parse_html
soup = parse_html('<td style="font-size:36px">92</td><td style="color:red">x</td>')
candidates = attr_regex_filter(soup.find_all("td"), "style", r"font-size:36px")
filter_matching_own(candidates, r"[0-9]+")  # [<td style="font-size:36px">92</td>]
```

### find_lineup {#find_lineup}

`find_lineup(shot: 'ShotEvent', curr_pbp: 'Optional[MiscGameEvent]', curr_lineups: 'list[LineupEvent]', lineup_it: "'PeekableIterator[LineupEvent]'") -> 'tuple[Optional[LineupEvent], list[LineupEvent]]'`

Find the lineup (stint) event on the floor for `shot`

(`ShotEnrichmentUtils.find_lineup`, `PlayByPlayUtils.scala:352-517`).

A recursive state machine over three lists: `curr_lineup` (the current
candidate), `fallback_lineups` (time-matching lineups whose raw events
did not contain `curr_pbp` -- kept as fallbacks), and `stashed_lineups`
(lineups pulled from the iterator but not yet stepped into, available for
future shots). The branch cases (labelled 2.1-2.4 in the Scala):

* **2.1** -- no time-matching lineup left: return the fallbacks.
* **2.2** -- the next lineup starts *after* the shot: no match, stash it.
* **2.3** -- strictly inside a lineup with no prior fallbacks: take it.
* **2.4** -- shot is exactly at a lineup boundary (or we are already
  choosing among multiple fallbacks): take this lineup iff its raw game
  events contain `curr_pbp`'s event string (`curr_pbp is None` takes
  it unconditionally); otherwise keep it as a fallback and recurse.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot to place (only `min` / `is_off` are read). |
| `curr_pbp` | `Optional[MiscGameEvent]` |  | The already-matched play-by-play event for this shot, used to disambiguate boundary lineups; `None` disables that check. |
| `curr_lineups` | `list[LineupEvent]` |  | Lineups pulled from the iterator on a previous call and still available (the current one first). |
| `lineup_it` | `PeekableIterator[LineupEvent]` |  | The shared lineup iterator (consumed in place). |

**Returns**

`(matched_lineup_or_None, lineups_to_retry_next_time)` -- the second element always includes the matched lineup (so out-of-order shots sharing it still resolve) plus any leftover stash.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
    PeekableIterator,
    find_lineup,
)
matched, retry = find_lineup(shot, None, [lineup], PeekableIterator([]))
```

### find_missing_subs {#find_missing_subs}

`find_missing_subs(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'`

Trims a clump whose lineups carry TOO MANY players by identifying the

"ghost" player(s) a missing sub-out left behind
(`LineupErrorAnalysisUtils.find_missing_subs`, `:406-514`).

Fires only when the clump's first event has `>= 6` on-floor players
(`:415-416`: `candidates.size < 6` is a no-op). `expected_size_diff`
(`:419`) is `first_event_player_count - 5` -- the number of ghosts the
trim should end up removing.

**Phase 1 -- shrink the candidate pool** (`:437-478`). Starting from the
first event's players, walk the clump chronologically. At each event a
candidate is *confirmed present* (and dropped from the pool) if it subs
out (`ev.players_out`, **skipped for the first event** -- `:445`,
literal port of `clump.evs.headOption.contains(ev)` as value equality
`ev == clump.evs[0]`; for a well-formed clump of distinct events this is
exactly `index == 0`) or is named in one of the event's team-side raw
plays (`parse_any_play` -> `~sportsdataverse.mbb.mbb_ncaa_names
.tidy_player` -> `~sportsdataverse.mbb.mbb_ncaa_stints
.build_player_code`, `:448-456`; unlike `validate_lineup` this
does NOT skip the literal `"team"` token -- ported verbatim).
`matching_index` is the **FIRST** event index at which the pool size
first equals `expected_size_diff` (`:475`); once set it freezes -- all
later events are skipped in phase 1 (`:439-441`).

**Accept gate** (`:479-480`): the final pool must be non-empty and no
larger than `expected_size_diff`. If `matching_index` never fired
(the pool jumped past `expected_size_diff` in a single step, or never
shrank to it), the gate still accepts iff the residual pool is a non-empty
subset of size `<= expected_size_diff` -- in which case phase 3 routes
**every** event through the "before match" branch (`index > None` is
always false). On failure the **original** clump is returned unchanged.

**Phase 3 -- rebuild the events** (`:482-503`, a `scanLeft` ported as
a manual accumulate loop that drops the seed). For events at/before
`matching_index` the ghost pool is simply removed from `players`
(`filterNot`). For events strictly **after** `matching_index`
(`index > matching_index` -- the matched event itself is "before")
`players` is rebuilt from the previous *tidied* event via
`~sportsdataverse.mbb.mbb_ncaa_stints.build_new_player_list` (the
`scanLeft` threads the previously-emitted event; its seed is `None`,
but the first event can never be an "after match" event, so the
`getOrElse(ev)` fallback is only ever a formality -- ported faithfully
all the same). The rebuilt events are partitioned by
`validate_lineup`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to attempt to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- the now-valid rebuilt events and a clump of the still-invalid ones (carrying the input's `next_good`); or `([], clump)` on a no-op / rejected fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    find_missing_subs,
)
fixed, still = find_missing_subs(clump, box_lineup, valid_codes)
```

### find_pbp_clump {#find_pbp_clump}

`find_pbp_clump(shot_time: 'float', pbp_it: "'PeekableIterator[PlayByPlayEvent]'", curr_pbp_clump: 'list[MiscGameEvent]', maybe_next_pbp_event: 'Optional[MiscGameEvent]') -> 'tuple[list[MiscGameEvent], Optional[MiscGameEvent]]'`

Gather every play-by-play shot/assist event sharing `shot_time`

(`ShotEnrichmentUtils.find_pbp_clump`, `PlayByPlayUtils.scala:556-608`).

If `curr_pbp_clump` (carried over from the previous shot) already holds
events at `shot_time` they are returned as-is; otherwise the iterator is
walked forward, discarding earlier events, accumulating the equal-time
ones, and stopping (returning it as `maybe_next_pbp_event`) at the first
later event.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot_time` | `float` |  | The shot's game-clock minute to gather events for. |
| `pbp_it` | `PeekableIterator[PlayByPlayEvent]` |  | The shared play-by-play iterator (consumed in place). |
| `curr_pbp_clump` | `list[MiscGameEvent]` |  | Events left over from the previous shot's clump. |
| `maybe_next_pbp_event` | `Optional[MiscGameEvent]` |  | The look-ahead event stashed by the previous call, if any. |

**Returns**

`(clump, maybe_next)` -- the equal-time events, plus the first strictly-later event (or `None` at end of stream).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
    PeekableIterator,
    find_pbp_clump,
)
clump, nxt = find_pbp_clump(5.0, PeekableIterator([]), [], None)
# ([], None)
```

### fix_combos {#fix_combos}

`fix_combos(first: 'str', last: 'str', code_start: 'Optional[str]' = None) -> 'list[tuple[str, Optional[str]]]'`

Pair each of `combos`' three name variants with a shared

player-code override (`DataQualityIssues.fix_combos`,
`DataQualityIssues.scala:340-346`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first name. |
| `last` | `str` |  | The player's last name. |
| `code_start` | `Optional[str]` | `None` | The forced player-code prefix for every variant, or `None` to leave the default `build_player_code` truncation behavior in place. |

**Returns**

Three `(name_variant, code_start)` pairs.

### fix_possible_score_swap_bug {#fix_possible_score_swap_bug}

`fix_possible_score_swap_bug(lineup: 'list[LineupEvent]', box_lineup: 'LineupEvent') -> 'list[LineupEvent]'`

Undo a rare NCAA data bug where the scores get transposed

(`fix_possible_score_swap_bug`, `LineupUtils.scala:51-90`).

If the last lineup's ending score is the exact transpose of the box
score's ending score, every lineup's `score_info` is un-transposed and
`pts`/`plus_minus` are swapped/negated between `team_stats` and
`opponent_stats` -- nothing else in the stat trees changes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `list[LineupEvent]` |  | The lineups to (maybe) fix, in chronological order. |
| `box_lineup` | `LineupEvent` |  | The trusted box-score lineup to compare the final score against. |

**Returns**

`lineup` unchanged if the scores aren't transposed (or `lineup` is empty); otherwise a new list with every entry's score/pts/ plus_minus corrected.

### get_ascending_time {#get_ascending_time}

`get_ascending_time(event: 'ShotEvent', period: 'int', is_women_game: 'bool') -> 'float'`

Converts the descending in-period clock time to an ascending

game-elapsed time (`ShotEventParser.get_ascending_time`, `:531-537`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `ShotEvent` |  | The shot event (only `~sportsdataverse.mbb .mbb_ncaa_models.ShotEvent.min`, the raw descending clock minute, is read). |
| `period` | `int` |  | The 1-indexed period the shot was taken in. |
| `is_women_game` | `bool` |  | Whether to use women's-quarters (10min) or men's- halves (20min, then 5min OTs) period lengths. |

**Returns**

The ascending game-elapsed time, in minutes.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import get_ascending_time
get_ascending_time(shot_with_min_4, period=1, is_women_game=False)  # 16.0
```

### get_box_lineup {#get_box_lineup}

`get_box_lineup(filename: 'str', in_html: 'str', team_id: 'TeamId', format_version: 'int', external_roster: 'tuple[list[str], list[RosterEntry]]' = ([], []), neutral_game_dates: 'AbstractSet[str]' = frozenset(), home_team: 'Optional[str]' = None, away_team: 'Optional[str]' = None) -> 'Union[LineupEvent, list[ParseError]]'`

Gets the boxscore lineup from the HTML page (``BoxscoreParser

.get_box_lineup`, `:122-222``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name -- used both for error reporting and to extract the period via `parse_period_from_filename` (e.g. `"test_p2.html"`). |
| `in_html` | `str` |  | The raw box-score-page HTML. |
| `team_id` | `TeamId` |  | The team this box score is being parsed for. |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout (see the module docstring's selector-translation notes). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` | `([], [])` | `(other_players, roster_players)` -- either just names, or a full roster, to validate/fuzzy-correct box names against (see `inject_validated_players`). Also seeds `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent.players_out` on the interim lineup (`roster_players`, each's `code` replaced by its jersey `number`). |
| `neutral_game_dates` | `AbstractSet[str]` | `frozenset()` | Date strings (the first whitespace-separated token of the raw date-cell text) known to be neutral-site games -- overrides the default home/away inference. |
| `home_team` | `Optional[str]` | `None` | The game's home team when the caller already knows it, forwarded to team-name resolution so a box page that names only one side (a non-D-I opponent has no header) still resolves. |
| `away_team` | `Optional[str]` | `None` | The game's away team, same purpose. Both are required together or neither is used. |

**Returns**

A `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` whose `players` is the validated box-score lineup (natural HTML order -- see the module docstring's "not sorted" note), or a `list[ParseError]` if any parsing step failed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    html = f.read()
result = get_box_lineup("test_p1.html", html, TeamId("TeamA"), format_version=0)
```

### get_config {#get_config}

`get_config() -> 'NcaaFetchConfig'`

Return the live `NcaaFetchConfig` singleton.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import get_config
cfg = get_config()
print(cfg.cache_dir, cfg.timeout)
```

### get_game_weight {#get_game_weight}

`get_game_weight(opp: 'OpponentGame', field: 'str', side: 'str') -> 'float'`

Weight for one game/field/side (`getGameWeight`, `ts:119-140`).

The field-specific shot volume (FGA for `efg`, 3PA for `3p`,
`2pmid_attempts` / `2prim_attempts` for the mid/rim fields); when that
is `0` (no shots of that type), **falls back to** `off_poss` /
`def_poss` so the game still carries weight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `opp` | `OpponentGame` |  | One opponent game dict. |
| `field` | `str` |  | A stat field. |
| `side` | `str` |  | `"off"` or `"def"`. |

**Returns**

The (non-negative) game weight.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_game_weight

game = {"off_3p_attempts": 0, "off_poss": 70}
print(get_game_weight(game, "3p", "off"))  # 70.0 (poss fallback)
```

### get_neutral_games {#get_neutral_games}

`get_neutral_games(filename: 'str', in_html: 'str', format_version: 'int') -> 'Union[tuple[TeamId, set[str]], list[ParseError]]'`

Extracts the set of neutral/away-marked game dates from a saved NCAA

team-schedule page (`TeamScheduleParser.get_neutral_games`,
`TeamScheduleParser.scala:63-94`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw team-schedule-page HTML. |
| `format_version` | `int` |  | `0` for the legacy `fieldset`/`legend` layout, `1` for the 2018+ `div.card-header`/`div.card-body` layout. |

**Returns**

`(team, neutral_game_dates)` -- the team parsed from the page's image `alt` attribute, and every `"MM/DD/YYYY"` date string found on an `"@Opponent"`-marked row -- or a single-element `list[ParseError]` if the HTML fails to parse, or the team name can't be located.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_neutral_games

with open("tests/fixtures/ncaa/test_schedule.html", encoding="utf-8") as f:
    html = f.read()
result = get_neutral_games("test_schedule.html", html, format_version=0)
if isinstance(result, list):
    raise RuntimeError(result)  # list[ParseError]
team, neutral_dates = result
```

### get_per_game_raw {#get_per_game_raw}

`get_per_game_raw(opp: 'OpponentGame', field: 'str', side: 'str') -> 'Optional[float]'`

Per-game raw shooting rate from one opponent row (`getPerGameRaw`, `ts:82-116`).

`efg` is `(2pmid_made + 2prim_made + 1.5 * 3p_made) / (2pmid_att +
2prim_att + 3p_att)`; `3p` / `2pmid` / `2prim` are `made /
attempts`. Every counter read is nullish (missing -> 0); the sole guard
is on total attempts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `opp` | `OpponentGame` |  | One opponent game dict. |
| `field` | `str` |  | A stat field; an unknown field returns `None`. |
| `side` | `str` |  | `"off"` or `"def"` (selects the `off_`/`def_` prefix). |

**Returns**

The rate as a float, or `None` when the relevant attempts total is `<= 0` (game skipped by the weighted means -- **not** a 0-rate).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_per_game_raw

game = {"off_3p_made": 4, "off_3p_attempts": 10}
print(get_per_game_raw(game, "3p", "off"))  # 0.4
```

### get_sorted_pbp_events {#get_sorted_pbp_events}

`get_sorted_pbp_events(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent', format_version: 'int') -> 'Union[list[PlayByPlayEvent], list[ParseError]]'`

Handy util to return the play-by-play events in chronological order,

used in a few other places (`PlayByPlayParser.get_sorted_pbp_events`,
`:221-239`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw play-by-play-page HTML. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup (supplies `team`/`year`). |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout. |

**Returns**

The play-by-play events in chronological (earliest-to-latest) order, or a `list[ParseError]` on failure. `enrich=True` is used internally to get the correct ascending timestamps, and its reversal is undone here (`.reverse`) to restore chronological order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import get_sorted_pbp_events

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    box_html = f.read()
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
    pbp_html = f.read()
events = get_sorted_pbp_events("test.html", pbp_html, box_lineup, format_version=0)
```

### get_team_raw_from_per_game {#get_team_raw_from_per_game}

`get_team_raw_from_per_game(team: 'TeamDetail', field: 'str') -> 'SideValues'`

A team's field rate as the weighted mean of its per-game raws (`getTeamRawFromPerGame`, `ts:224-250`).

Same accumulation as `compute_league_averages_from_per_game` but
scoped to one team's games; empty -> `0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | A `team_details` team dict. |
| `field` | `str` |  | A stat field. |

**Returns**

`{"off": float, "def": float}`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import get_team_raw_from_per_game

team = {"opponents": [{"off_3p_made": 4, "off_3p_attempts": 10}]}
print(get_team_raw_from_per_game(team, "3p")["off"])  # 0.4
```

### get_team_triples {#get_team_triples}

`get_team_triples(filename: 'str', in_html: 'str', old_format: 'bool' = False) -> 'Union[list[tuple[TeamId, str, ConferenceId]], list[ParseError]]'`

Extracts `(team, NCAA id, conference)` triples from a saved NCAA

team-list/attendance page (`TeamIdParser.get_team_triples`,
`TeamIdParser.scala:69-91`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw team-list-page HTML. |
| `old_format` | `bool` | `False` | `True` for pages where the team name and conference are in separate `<td>`s; `False` (default) for pages where the conference is embedded in the team-name cell as `"Team (Conf)"`. |

**Returns**

One `(TeamId, ncaa_id, ConferenceId)` triple per row that has both a resolvable id and name/conference (rows missing either are silently skipped, matching the Scala's `case _ => Nil`), or a single-element `list[ParseError]` if the HTML itself fails to parse.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_team_triples

with open("tests/fixtures/ncaa/test_attendance_list.html", encoding="utf-8") as f:
    html = f.read()
result = get_team_triples("test_attendance_list.html", html, old_format=True)
```

### get_unified_ncaa_id {#get_unified_ncaa_id}

`get_unified_ncaa_id(filename: 'str', in_html: 'str') -> 'Union[Optional[str], list[ParseError]]'`

Gets a player's lowest cross-season NCAA id from a saved player page

(`RosterParser.get_unified_ncaa_id`, `RosterParser.scala:136-152`).

Always uses the v1 selector table -- this bonus lookup only exists on
2018+-era pages.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw player-page HTML. |

**Returns**

The numerically-lowest NCAA id found, `None` if the page has no `tr[id^=player_season_]` rows, or a single-element `list[ParseError]` if the HTML couldn't be parsed at all.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_roster_parser import get_unified_ncaa_id
get_unified_ncaa_id("player.html", player_page_html)
```

### handle_common_sub_bug {#handle_common_sub_bug}

`handle_common_sub_bug(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'`

Fixes the "2-in-1-out then a compensating 1-out" substitution bug

(`LineupErrorAnalysisUtils.handle_common_sub_bug`, `:269-298`).

Handles a **single-event** bad clump whose next known-good lineup carries
a lone sub-out that the clump's event should have applied but didn't (e.g.
`IN: X, Y, Z; OUT: A, B` in the bad event, then `OUT: C` in the good
one). Fires only when all three guard conditions hold
(`:275-278`):

* the bad event has more players subbing IN than OUT
  (`len(players_in) > len(players_out)`),
* the good event has **no** sub-ins (`len(good.players_in) == 0` --
  otherwise there's no way to tell which of its sub-outs to borrow), and
* the good event has at least one sub-out (`len(good.players_out) > 0`).

The fix (`:279-283`) removes the good event's sub-outs from the bad
event's on-floor `players` (value-equality `not in` -- the Scala's
`filterNot(good.players_out.toSet)`) and appends them to the bad
event's `players_out` (order-preserving distinct` -- the Scala's
`(bad.players_out ++ good.players_out).distinct`). The fix is **accepted
only if** the result then passes `validate_lineup` (`:284-294`):
on success the fixed event is returned as the sole `fixed` lineup and
the still-to-fix clump is emptied; on failure the *fixed* event (not the
original) is returned as the still-to-fix clump, keeping the same
`next_good` so a later pass can try again.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to attempt to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- `fixed_lineups` is `[fixed]` on an accepted fix else `[]`; `still_to_fix` is an empty clump on accept, the (unchanged) input clump on a guard miss, or the single-event *fixed*-but-still-invalid clump on a rejected fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    handle_common_sub_bug,
)
fixed, still = handle_common_sub_bug(clump, box_lineup, valid_codes)
```

### inject_starting_lineup_into_box {#inject_starting_lineup_into_box}

`inject_starting_lineup_into_box(sorted_pbp_events: 'list[PlayByPlayEvent]', box_lineup: 'LineupEvent', external_roster: 'tuple[list[str], list[RosterEntry]]', format_version: 'int') -> 'LineupEvent'`

Infer the starting five and reorder the box-score roster so they lead

(`PlayByPlayUtils.inject_starting_lineup_into_box`,
`PlayByPlayUtils.scala:684-845`).

The v1 (2018+) NCAA box score dropped the ordered list of starters, so we
reconstruct it from the play-by-play sub sequencing. A player is a starter
if, walking the events forward, they are seen *before* their first sub-in
-- either subbed *out* (before ever being subbed in), or *named in a
team-side play* that isn't concurrent with a sub. Anyone subbed *in* before
ever being seen is excluded. The reconstruction stops once five starters
are found.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_pbp_events` | `list[PlayByPlayEvent]` |  | The full play-by-play event stream, ascending time. |
| `box_lineup` | `LineupEvent` |  | The box-score lineup event (its `players` is the full roster to reorder). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` |  | Unused here -- carried for signature parity with the Scala (its pipeline caller passes it). See the module note. |
| `format_version` | `int` |  | Unused here -- carried for signature parity. See the module note. |

**Returns**

A copy of `box_lineup` with `players` reordered so the inferred starters lead. If fewer than five starters could be inferred (a "40-trillion" player who was never subbed nor mentioned), the roster is ordered starters -> possible-starters -> definitely-not-starters as the best available guess.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import inject_starting_lineup_into_box
fixed = inject_starting_lineup_into_box(pbp_events, box_lineup, ([], []), 1)
```

### inject_validated_players {#inject_validated_players}

`inject_validated_players(ordered_lineup_from_box: 'list[str]', box_minus_players: 'LineupEvent', external_roster: 'tuple[list[str], list[RosterEntry]]') -> 'list[str]'`

Validates box players against the roster (if available) and any

other available box scores (`BoxscoreParser.inject_validated_players`,
`:233-279`).

See the module docstring's "un-threaded `tidy_ctx`" note -- every
fuzzy-resolution call inside the loop uses the SAME original context,
never the updated one a call returns (ported verbatim, including this
apparent Scala oversight).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ordered_lineup_from_box` | `list[str]` |  | The raw player-name strings scraped straight off the box-score page (already v0-normalized if the source was v1, by `get_box_lineup`'s caller). |
| `box_minus_players` | `LineupEvent` |  | The in-progress `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` (used only for its `team` field, both to scope the fuzzy-match context and to key `~sportsdataverse.mbb.mbb_ncaa_data_quality.players_missing_from_boxscore`). |
| `external_roster` | `tuple[list[str], list[RosterEntry]]` |  | `(other_players, roster_players)` -- extra known player names, and a full team roster (if available) to validate against / fuzzy-correct box names onto. |

**Returns**

`ordered_lineup_from_box` with any name not found in `roster_players` fuzzy-corrected onto the closest roster name (if a roster was supplied at all), followed by any roster/other/ known-missing players not already present in that corrected list (see the module docstring's "Extra-players Set ordering" note for this trailing group's order).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import inject_validated_players
inject_validated_players(["Player One"], box_lineup, ([], []))
```

### is_cached {#is_cached}

`is_cached(path: 'str', *, cache_dir: 'Optional[Path]' = None) -> 'bool'`

Return whether *path* already has a cache file on disk.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  |  |
| `cache_dir` | `Optional[Path]` | `None` |  |

### is_end_of_game_fouling_vs_fastbreak {#is_end_of_game_fouling_vs_fastbreak}

`is_end_of_game_fouling_vs_fastbreak(curr_clump: 'ConcurrentClump', event_parser: 'PossessionEvent') -> 'bool'`

Check for intentional fouling to prolong the game, specifically so it

can be excluded from being counted as a fast break
(`is_end_of_game_fouling_vs_fastbreak`, `LineupUtils.scala:603-656`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr_clump` | `ConcurrentClump` |  | The clump to classify. |
| `event_parser` | `PossessionEvent` |  | Selects which side of each event is "attacking". |

**Returns**

`True` iff the FIRST attacking-side FT-made/FT-missed event in `curr_clump.evs` is both near the end of a period AND has the attacking team ahead by `(0, 10]` points; `False` if no such event exists.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_end_of_game_fouling_vs_fastbreak

is_end_of_game_fouling_vs_fastbreak(curr_clump, event_parser)
```

### is_gen2 {#is_gen2}

`is_gen2(ev: 'RawGameEvent') -> 'bool'`

Detect the new/"gen2" NCAA event format (`EventUtils.is_gen2`,

`EventUtils.scala:12-14`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `RawGameEvent` |  | The raw game event to inspect. |

**Returns**

`True` if `ev.info` contains a comma-space (`", "`), the gen2 format's field separator; `False` for the old/legacy format.

### is_scramble {#is_scramble}

`is_scramble(curr_clump: 'ConcurrentClump', prev_clumps: 'list[ConcurrentClump]', event_parser: 'PossessionEvent', player_version: 'bool') -> 'tuple[Callable[[RawGameEvent], bool], str]'`

Figure out if (each event of) the current clump is part of a

"scramble scenario" following an ORB (`is_scramble`, `LineupUtils
.scala:222-597`).

Returns a `(predicate, debug_tag)` tuple -- **the tuple shape is
load-bearing**: the oracle asserts the debug tag string directly
(`"N/A"`/`"0a"`/`"1aa"`/`"1ab"`/`"1b"`/`"2aa"`/`"2ab"`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `curr_clump` | `ConcurrentClump` |  | The clump to classify. |
| `prev_clumps` | `list[ConcurrentClump]` |  | Prior merged clumps, most-recent-first. |
| `event_parser` | `PossessionEvent` |  | Selects which side of each event is "attacking". |
| `player_version` | `bool` |  | Unused -- see the module docstring's `is_scramble` port notes (the Scala's debug-print gate this flag controls is permanently `false` regardless of its value). |

**Returns**

`(predicate, debug_tag)` where `predicate(ev)` reports whether `ev` is part of a scramble.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_scramble

predicate, tag = is_scramble(curr_clump, prev_clumps, event_parser, player_version=False)
[predicate(ev) for ev in curr_clump.evs]
```

### is_team_shooting_left_to_start {#is_team_shooting_left_to_start}

`is_team_shooting_left_to_start(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]') -> 'tuple[bool, int]'`

Infers which side of the SVG court the team under analysis shoots

towards in the first period, from its own made/missed shot locations
(`ShotEventParser.is_team_shooting_left_to_start`, `:540-555`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs, pre-geometry-transform. |

**Returns**

`(team_shooting_left_in_first_period, first_period)` -- the first element of `sorted_very_raw_events`, if any, determines `first_period`; the majority side (by count) of the team's own (`is_off`) shots within that period determines the direction.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import is_team_shooting_left_to_start
is_team_shooting_left_to_start([(1, shot_a), (1, shot_b)])
```

### is_women_game {#is_women_game}

`is_women_game(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]') -> 'bool'`

Infers men's vs. women's game from timing evidence

(`ShotEventParser.is_women_game`, `:558-566`). **Shot-parser-specific
variant** -- distinct from the play-by-play parser's own
`is_women_game` (Task 5e.3), which uses PbP event timing instead of
shot timing; the plan's recon flags both as "its OWN is_women_game
variant" per module.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs. |

**Returns**

`True` if at least 4 periods were seen AND no shot was taken with more than 10 minutes showing on the (descending) clock in the very first event (women's quarters are 10 minutes; a shot at >10:00 remaining could only happen in a longer men's period).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import is_women_game
is_women_game([(1, shot), (2, shot), (3, shot), (4, shot)])  # True
```

### jsoup_text {#jsoup_text}

`jsoup_text(el: 'Optional[Tag]') -> 'str'`

JSoup `Element.text()`: all descendant text, whitespace-collapsed.

JSoup's `.text()` joins every text node under `el` (including
descendants) and collapses runs of whitespace (spaces, tabs, newlines)
into single spaces, trimming the ends. bs4's `.get_text()` does the
joining but not the collapsing, so captured HTML's indentation/newlines
would otherwise leak into every extracted value.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `el` | `Optional[Tag]` |  | The element to extract text from, or `None`. |

**Returns**

The whitespace-collapsed text, or `""` if `el` is `None`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html
soup = parse_html("<td>\n  Akin,\tDaniel  </td>")
jsoup_text(soup.find("td"))  # "Akin, Daniel"
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

### matching_player {#matching_player}

`matching_player(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', tidy_ctx: 'TidyPlayerContext', code_match: 'bool') -> 'bool'`

Whether the player in `pbp_event` matches `shot`'s shooter

(`ShotEnrichmentUtils.matching_player`, `PlayByPlayUtils.scala:638-652`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched. |
| `pbp_event` | `MiscGameEvent` |  | The candidate play-by-play event. |
| `tidy_ctx` | `TidyPlayerContext` |  | The name-resolution context. |
| `code_match` | `bool` |  | If `True`, compare on player *code* only (looser -- lets a name that resolves to the wrong identity but the right code match); if `False`, require full `~sportsdataverse.mbb .mbb_ncaa_models.PlayerCodeId` equality. |

**Returns**

`True` if the resolved player matches `shot.player` under the selected comparison, else `False`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import matching_player
matching_player(shot, pbp_event, tidy_ctx, code_match=False)
```

### misspellings {#misspellings}

`misspellings(team: 'Optional[TeamId]') -> 'dict[str, str]'`

Team-scoped misspelling map, falling back to the generic map

(`DataQualityIssues.misspellings`, `DataQualityIssues.scala:165-322`
-- see the module docstring's "fallback semantics" note for why this is
a precomputed merge, not a runtime two-level lookup).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `Optional[TeamId]` |  | The team to look up team-specific corrections for. `None` (like any team absent from the table) falls back to `generic_misspellings`. |

**Returns**

A fresh dict -- the team's misspelling map merged with `generic_misspellings`, or a copy of `generic_misspellings` if `team` has no specific entries.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import misspellings
from sportsdataverse.mbb.mbb_ncaa_models import TeamId

misspellings(TeamId("NJIT"))["Lewal, Levi"]  # 'Lawal, Levi'
misspellings(TeamId("Some Unlisted Team"))  # {} (generic fallback)
misspellings(None)  # {} (generic fallback)
```

### ncaa_wbb_box_scores {#ncaa_wbb_box_scores}

`ncaa_wbb_box_scores(game_ids: 'Union[str, int, Iterable[Union[str, int]]]', *, multi_games: 'bool' = False, fetcher: 'Optional[Any]' = None, return_as_pandas: 'bool' = False) -> "Union['pl.DataFrame', 'pd.DataFrame']"`

Scrape WBB per-player box scores (wbigballR `get_box_scores`/`scrape_box`).

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_box_stats.ncaa_mbb_box_scores` — see
it for the column contract, the tolerant header renames, and the fixed
`multi_games` aggregation (R's groups by a `Pos` column the current
markup no longer ships).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Union[str, int, Iterable[Union[str, int]]]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `multi_games` | `bool` | `False` | Aggregate per player across all games (fixed grouping on player/clean_name/team). |
| `fetcher` | `Optional[Any]` | `None` | Optional injected fetcher exposing `fetch_game_individual_stats` (tests/offline). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player box rows (or per-player aggregates with `multi_games`).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_box_stats import ncaa_wbb_box_scores
box = ncaa_wbb_box_scores(["5722355"])
print(box.shape)
```

### ncaa_wbb_date_games {#ncaa_wbb_date_games}

`ncaa_wbb_date_games(date: 'Optional[str]' = None, *, conference: 'str' = 'All', conference_id: 'Optional[int]' = None, fetcher: "Optional['NcaaFetcher']" = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Discover every NCAA WBB game played on a date (wbigballR `get_date_games`).

Same engine as
`sportsdataverse.mbb.mbb_ncaa_scoreboard.ncaa_mbb_date_games` with
the WBB `season_divisions` table bound (see the module docstring for
the 2010-11..2025-26 coverage caveat).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | `"MM/DD/YYYY"`. Defaults to yesterday (R default). |
| `conference` | `str` | `'All'` | Conference name filter (e.g. `"SEC"`, `"Summit League"`); case/punctuation-insensitive. Default `"All"` (every conference). Unknown names raise. |
| `conference_id` | `Optional[int]` | `None` | Explicit stats.ncaa.org conference id; overrides *conference* when given. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch. NcaaFetcher` (tests pass an offline fake). `None` uses `NcaaFetcher.with_browser()`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game — see the MBB sibling for the full `SCOREBOARD_SCHEMA` column contract.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_scoreboard import ncaa_wbb_date_games
games = ncaa_wbb_date_games("12/05/2024")
print(games.shape)
```

### ncaa_wbb_join_pbp_shots {#ncaa_wbb_join_pbp_shots}

`ncaa_wbb_join_pbp_shots(pbp: 'pl.DataFrame', shots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Attach WBB chart shots onto the pbp frame (pure delegation).

See `sportsdataverse.mbb.mbb_ncaa_shots.ncaa_mbb_join_pbp_shots`
for the matching rules (FG-only, within-second same-result sequence) and
the joined 40-column contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | 35-column snake_case pbp frame (`ncaa_wbb_play_by_play`). |
| `shots` | `DataFrame` |  | Shots frame from `ncaa_wbb_shot_locations`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The pbp frame with shot columns attached (unmatched rows NA-filled).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_shots import ncaa_wbb_join_pbp_shots
joined = ncaa_wbb_join_pbp_shots(pbp, shots)
print(joined.shape)
```

### ncaa_wbb_player_stats {#ncaa_wbb_player_stats}

`ncaa_wbb_player_stats(pbp: 'pl.DataFrame', *, multi_games: 'bool' = False, simple: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Aggregate WBB play-by-play into per-player box stats (wbigballR `get_player_stats`).

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_stats_agg.ncaa_mbb_player_stats` —
see it for the algorithm and column contracts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`ncaa_wbb_game_pbp` output). |
| `multi_games` | `bool` | `False` | Aggregate across games per (player, team) — the season-stat surface. |
| `simple` | `bool` | `False` | Return the reduced surface without the transition / assisted / putback / block-location splits. |
| `fix_tip_in` | `bool` | `True` | Count the real `"Tip In"` vocabulary (default); False reproduces R's `"Tip-In"` bug for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player+team (+game when `multi_games=False`).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_stats_agg import ncaa_wbb_player_stats
stats = ncaa_wbb_player_stats(pbp)
print(stats.shape)
```

### ncaa_wbb_possessions {#ncaa_wbb_possessions}

`ncaa_wbb_possessions(pbp: 'pl.DataFrame', *, simple: 'bool' = False, fix_cross_game_leak: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Aggregate WBB play-by-play into one row per possession (wbigballR `get_possessions`).

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_possession_seg.ncaa_mbb_possessions`
— see it for the algorithm, the 28/17-column contracts, and the fixed-vs-
faithful flag convention.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`ncaa_wbb_game_pbp` output). |
| `simple` | `bool` | `False` | Return only the 17-column possession/points frame. |
| `fix_cross_game_leak` | `bool` | `True` | When True (default, and the CORRECT behavior), window the `start_event_type` lag with `.over("game_id")` so a game's first possession does not inherit the previous game's last event. When False, reproduce R's ungrouped `dplyr::lag` (`all_functions.R:3698`). Parity tests pass False. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per possession.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_possession_seg import ncaa_wbb_possessions
poss = ncaa_wbb_possessions(pbp)
print(poss.shape)

# Faithful (R-buggy) start-event lag

poss = ncaa_wbb_possessions(pbp, fix_cross_game_leak=False)
```

### ncaa_wbb_shot_locations {#ncaa_wbb_shot_locations}

`ncaa_wbb_shot_locations(game_ids: 'Sequence[object]', *, fetcher: 'Optional[Any]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape WBB shot locations for one or more games.

Same driver as
`sportsdataverse.mbb.mbb_ncaa_shots.ncaa_mbb_shot_locations` with
the quarters `period_model` bound — see the mbb sibling for the parse
algorithm and the `~sportsdataverse.mbb.mbb_ncaa_shots.SHOTS_SCHEMA`
contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[object]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `fetcher` | `Optional[Any]` | `None` | Optional injected fetcher exposing `fetch_game_box` (tests/offline). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

All games' shots row-bound (zero-row schema frame when none found).

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_shots import ncaa_wbb_shot_locations
shots = ncaa_wbb_shot_locations(["5722355"])
print(shots.shape)
```

### ncaa_wbb_team_roster {#ncaa_wbb_team_roster}

`ncaa_wbb_team_roster(team_id: 'Optional[int]' = None, *, team: 'Optional[str]' = None, season: 'Optional[str]' = None, fetcher: "Optional['NcaaFetcher']" = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape a women's team roster from stats.ncaa.org.

Port of wbigballR `get_team_roster` with name resolution fixed to the
WBB crosswalk (see the module docstring). The roster parser itself is
league-agnostic; algorithm detail:
`sportsdataverse.mbb.mbb_ncaa_schedule.ncaa_mbb_team_roster`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` | stats.ncaa.org team id (changes every season). |
| `team` | `Optional[str]` | `None` | School name, e.g. `"South Carolina"`. |
| `season` | `Optional[str]` | `None` | Season string, e.g. `"2024-25"`; required with `team`. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch. NcaaFetcher`; defaults to a fresh browser-transport fetcher. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player — see `~sportsdataverse.mbb.mbb_ncaa_schedule.parse_ncaa_bb_team_roster` for the column contract.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_schedule import ncaa_wbb_team_roster
df = ncaa_wbb_team_roster(team="South Carolina", season="2024-25")
print(df.select("jersey", "player", "ht_inches").head())
```

### ncaa_wbb_team_schedule {#ncaa_wbb_team_schedule}

`ncaa_wbb_team_schedule(team_id: 'Optional[int]' = None, *, team: 'Optional[str]' = None, season: 'Optional[str]' = None, fetcher: "Optional['NcaaFetcher']" = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Scrape a women's team's season schedule from stats.ncaa.org.

Port of wbigballR `get_team_schedule` with name resolution fixed to the
WBB crosswalk (see the module docstring). Algorithm detail:
`sportsdataverse.mbb.mbb_ncaa_schedule.ncaa_mbb_team_schedule`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Optional[int]` | `None` | stats.ncaa.org team id (changes every season). |
| `team` | `Optional[str]` | `None` | School name, e.g. `"South Carolina"`. |
| `season` | `Optional[str]` | `None` | Season string, e.g. `"2024-25"`; required with `team`. |
| `fetcher` | `Optional['NcaaFetcher']` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch. NcaaFetcher`; defaults to a fresh browser-transport fetcher. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per scheduled game — see `~sportsdataverse.mbb.mbb_ncaa_schedule.parse_ncaa_bb_team_schedule` for the column contract.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_schedule import ncaa_wbb_team_schedule
df = ncaa_wbb_team_schedule(team="South Carolina", season="2024-25")
print(df.shape)
```

### ncaa_wbb_team_stats {#ncaa_wbb_team_stats}

`ncaa_wbb_team_stats(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Aggregate WBB play-by-play into per-team game stats (wbigballR `get_team_stats`).

Pure delegation to
`sportsdataverse.mbb.mbb_ncaa_stats_agg.ncaa_mbb_team_stats` — see
it for the algorithm and column contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`ncaa_wbb_game_pbp` output). |
| `include_transition` | `bool` | `False` | Append the trans`/half` split surface. |
| `fix_tip_in` | `bool` | `True` | Count the real `"Tip In"` vocabulary (default); False reproduces R's `"Tip-In"` bug for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team per game.

**Example**

```python
from sportsdataverse.wbb.wbb_ncaa_stats_agg import ncaa_wbb_team_stats
team = ncaa_wbb_team_stats(pbp)
print(team.shape)
```

### phase1_shot_event_enrichment {#phase1_shot_event_enrichment}

`phase1_shot_event_enrichment(sorted_very_raw_events: 'list[tuple[int, ShotEvent]]', second_half_override: 'Optional[set[int]]' = None) -> 'list[ShotEvent]'`

The court-geometry enrichment pass: ascending time, coordinate

transform + geo synthesis, and the self-correcting side-flip re-run
(`ShotEventParser.phase1_shot_event_enrichment`, `:415-528`).

For each shot: compute the ascending game time, decide (from
`is_team_shooting_left_to_start` + which half the period falls in)
whether the shot's side needs flipping, run `transform_shot_location`
to get both the believed-correct and alternative (mirrored) locations,
keep whichever is closer to the basket (a >1.2x distance advantage for
the "alternative" wins, or ANY shot taken with <0.1 min left on the
clock always keeps the original -- a half-court heave near the buzzer
is plausible, so the tie-break favors trusting the raw geometry there),
then synthesize a lat/lon.

After all shots are processed, if any period had >=6 shots AND more than
75% of them came back implausibly long-distance (>50ft), the whole pass
re-runs ONCE with those periods' orientation flipped (the self-correcting
part) -- `second_half_override` is `None` on the initial call and a
non-`None` set on the one allowed retry, preventing infinite recursion.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_very_raw_events` | `list[tuple[int, ShotEvent]]` |  | The chronologically-sorted (period, shot) pairs from `parse_shot_html`, pre-geometry-transform. |
| `second_half_override` | `Optional[set[int]]` | `None` | The set of periods whose `second_half_switch` orientation should be inverted (the self-correction re-run's input); `None` on the first call. |

**Returns**

The fully court-geometry-enriched shots, in the same order as `sorted_very_raw_events`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import phase1_shot_event_enrichment
shots = phase1_shot_event_enrichment([(1, very_raw_shot)])
```

### playwright_transport {#playwright_transport}

`playwright_transport(*, headless_new: 'bool' = True, challenge_wait_ms: 'int' = 8000, nav_timeout_ms: 'int' = 45000, user_agent: 'Optional[str]' = None, solve_attempts: 'int' = 3, relaunch_backoff: 'float' = 2.0) -> "'_PlaywrightTransport'"`

Build the **suggested** stats.ncaa.org game-detail scraping transport.

Drives a real Chromium via Playwright in Chrome's new-headless mode
(`--headless=new`) to clear the Akamai `bm-verify` challenge that
`curl_cffi` cannot, then serves raw server HTML for the 5a-5e parsers.
Playwright is a **lazy optional import** (not a hard dependency); a clear
`ImportError` fires on first use if it is missing.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headless_new` | `bool` | `True` | Use `--headless=new` (real-GPU render, no window) -- the default and the proven-working mode. `False` runs old headless (`headless_shell`), which Akamai flags -- avoid. |
| `challenge_wait_ms` | `int` | `8000` | Milliseconds to let the bm-verify sensor run after the first navigation. |
| `nav_timeout_ms` | `int` | `45000` | Per-navigation timeout. |
| `user_agent` | `Optional[str]` | `None` | Override the Chrome UA string. |
| `solve_attempts` | `int` | `3` |  |
| `relaunch_backoff` | `float` | `2.0` |  |

**Returns**

A stateful, callable `FetchTransport` reusing one browser for the session. Close it when done (it is a context manager, has `close()`, and registers an `atexit` safety net).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher
with NcaaFetcher.with_browser() as fetcher:
    pbp = fetcher.fetch_game_pbp("1613299")               # raw PBP HTML
    box = fetcher.fetch_game_individual_stats("1613299")  # raw box HTML
# -> feed to get_box_lineup / create_lineup_data (mbb_ncaa_*_parser)
```

### remove_diacritics {#remove_diacritics}

`remove_diacritics(fragment: 'str') -> 'str'`

Strip diacritical marks, e.g. `"Juhász"` -> `"Juhasz"`

(`ExtractorUtils.scala:38-43`: NFD normalization then removal of the
combining-diacritical-marks block).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fragment` | `str` |  | Any string (a full player name or a name fragment). |

**Returns**

The string with combining marks removed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import remove_diacritics
print(remove_diacritics("Dorka Juhász"))  # "Dorka Juhasz"
```

### reorder_and_reverse {#reorder_and_reverse}

`reorder_and_reverse(reversed_partial_events: 'Iterable[PlayByPlayEvent]') -> 'list[PlayByPlayEvent]'`

Orders same-minute play-by-play events so subs never enclose the plays

they logically precede/follow (`ExtractorUtils.scala:435-599`).

Groups consecutive events sharing the same `min` into a block (the
input arrives in descending/reverse-chronological order, so blocks are
discovered and internally accumulated in reverse too), then -- for any
block containing a sub -- reorders it via `inner_sort`: events
referencing a subbed-OUT player (or scoring no higher than the sub) land
in a pre-sub group, the subs themselves come next (in ascending-score
order), and events referencing a subbed-IN player (or scoring higher
than the sub) land in a trailing post-sub group. Free-throw attempts
sharing the sub's inferred "direction" (team vs. opponent, inferred from
the nearest preceding shot/FT/foul) are pulled into the pre-sub group
unless the shooter is one of the players being subbed in. Blocks with no
sub are returned unchanged apart from the initial score-based sort.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `reversed_partial_events` | `Iterable[PlayByPlayEvent]` |  | Events for one lineup event, in reverse-chronological (descending-time) order -- the natural order encountered walking play-by-play text bottom-up. |

**Returns**

The same events, forward-chronological (ascending time), with each same-minute block internally reordered so no sub encloses a play it logically shouldn't.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import Score
from sportsdataverse.mbb.mbb_ncaa_stints import (
    OtherTeamEvent,
    SubInEvent,
    reorder_and_reverse,
)
events = [
    SubInEvent(0.4, Score(0, 0), "player1"),
    OtherTeamEvent(0.4, Score(0, 0), "rebound"),
]
reorder_and_reverse(events)
# [OtherTeamEvent(...), SubInEvent(...)]
```

### reset_config {#reset_config}

`reset_config() -> 'NcaaFetchConfig'`

Reset the active config to its env-var-derived defaults.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import update_config, reset_config
update_config(timeout=5)
reset_config()
```

### right_kind_of_shot {#right_kind_of_shot}

`right_kind_of_shot(shot: 'ShotEvent', pbp_event: 'MiscGameEvent', strict: 'bool') -> 'bool'`

Whether `pbp_event`'s shot type is compatible with `shot`'s

distance and make/miss (`ShotEnrichmentUtils.right_kind_of_shot`,
`PlayByPlayUtils.scala:659-679`).

The distance-in-the-data is approximate, so exact 2-vs-3 discrimination is
impossible; this only rules out the *obvious* mismatches (a clearly-short
shot matched to a 3, or vice versa) and always requires make/miss
agreement.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot` | `ShotEvent` |  | The shot being enriched (`pts`/`dist` read). |
| `pbp_event` | `MiscGameEvent` |  | The candidate play-by-play event. |
| `strict` | `bool` |  | If `True`, also apply the distance gate; if `False`, only the make/miss agreement is required. |

**Returns**

`True` if the event could plausibly be this shot.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import right_kind_of_shot
right_kind_of_shot(shot, pbp_event, strict=True)
```

### run_iterative_adjustment_with_hca {#run_iterative_adjustment_with_hca}

`run_iterative_adjustment_with_hca(teams: 'Sequence[TeamDetail]', team_by_name: 'dict[str, TeamDetail]', fields: 'Sequence[str]', league_averages: 'LeagueAverages', poss_splits: 'dict[str, PossessionSplits]', *, max_iterations: 'int' = 100, tolerance: 'float' = 1e-06) -> 'IterationResult'`

KenPom-style SoS + HCA fixed-point solver (`runIterativeAdjustmentWithHCA`, `ts:306-527`).

Each iteration (Jacobi -- all teams read the *previous* iteration's
adjustments, then commit together):

1. Per team/field, adjust every game
   `adj_game = raw_game * (league / (opp_adj +/- hca))` and take the
   weighted mean; a field with no valid games keeps its current value.
2. Re-estimate per-field HCA from home/away possession-imbalance residuals
   `hca = sum((raw - pred) * |imbalance|) / sum(|imbalance|)` over teams
   with `|imbalance| >= IMBALANCE_MIN`.

Stops when the max per-team/field change drops below `tolerance` or after
`max_iterations` sweeps (the HCA re-estimate still runs on the final
sweep). The cross-guard on the per-game branch, the asymmetric residual
prediction, and the cross-named opponent strengths are all preserved -- see
the module docstring's landmine list.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `Sequence[TeamDetail]` |  | The teams to solve over. |
| `team_by_name` | `dict[str, TeamDetail]` |  | `{team_name: team_detail}` for opponent lookup. |
| `fields` | `Sequence[str]` |  | The stat fields to solve. |
| `league_averages` | `LeagueAverages` |  | Output of `compute_league_averages_from_per_game`. |
| `poss_splits` | `dict[str, PossessionSplits]` |  | `{team_name:` `PossessionSplits` `}`. |
| `max_iterations` | `int` | `100` | Iteration cap (default `MAX_ITERATIONS`; pin to `1` to inspect a single sweep). |
| `tolerance` | `float` | `1e-06` | Convergence tolerance (default `TOLERANCE`). |

**Returns**

An `IterationResult` (`adj_values`, `hca_per_field`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import (
    STRENGTH_ADJUSTED_FIELDS,
    compute_league_averages_from_per_game,
    compute_possession_splits,
    run_iterative_adjustment_with_hca,
)

by_name = {t["team_name"]: t for t in teams}
league = compute_league_averages_from_per_game(teams)
splits = {t["team_name"]: compute_possession_splits(t) for t in teams}
result = run_iterative_adjustment_with_hca(
    teams, by_name, STRENGTH_ADJUSTED_FIELDS, league, splits,
)
print(result.hca_per_field["3p"]["hca_off"])
```

### select_contains {#select_contains}

`select_contains(root: 'Tag', selector: 'str', text: 'str') -> 'list[Tag]'`

JSoup `root.select(sel + ":contains(text)")`: candidates whose full

text (own + every descendant's) case-insensitively CONTAINS `text` as
a plain substring -- **not** a regex (Task 5e.2 addition; see the module
docstring's "Critical divergence" note).

JSoup's `:contains()` is documented case-insensitive substring
containment; soupsieve's `:-soup-contains()` (the non-deprecated
spelling of its `:contains()`) is case-SENSITIVE, with no
case-insensitive variant of its own. Reproducing JSoup's actual
semantics therefore needs this helper rather than `:-soup-contains()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector for the structural part of the match (everything before `:contains`). |
| `text` | `str` |  | The plain substring each candidate's collapsed text must case-insensitively contain. |

**Returns**

Every `selector` match whose `jsoup_text` case-insensitively contains `text`, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_contains
soup = parse_html("<td>game date:</td><td>Location:</td>")
select_contains(soup, "td", "Game Date:")  # [<td>game date:</td>]
```

### select_matching {#select_matching}

`select_matching(root: 'Tag', selector: 'str', regex: 'str') -> 'list[Tag]'`

JSoup `root.select(sel + ":matches(regex)")`: candidates whose full

text (own + every descendant's) matches `regex`.

Soupsieve has no `:matches()` pseudo-class equivalent, so this runs the
plain structural `selector` first, then filters by `re.search`
over each candidate's `jsoup_text` (own text plus descendants',
matching JSoup's `:matches()` semantics -- as opposed to
`select_matching_own`'s own-text-only `:matchesOwn()`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector. |
| `regex` | `str` |  | The pattern each candidate's collapsed text must `re.search`-match. |

**Returns**

Every `selector` match whose `jsoup_text` contains a `regex` match, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_matching
soup = parse_html("<div><p>Home Team</p><p>Away Team</p></div>")
select_matching(soup, "p", r"^Home")  # [<p>Home Team</p>]
```

### select_matching_own {#select_matching_own}

`select_matching_own(root: 'Tag', selector: 'str', regex: 'str') -> 'list[Tag]'`

JSoup `root.select(sel + ":matchesOwn(regex)")`: candidates whose

OWN text only (excluding descendant elements' text) matches `regex`.

JSoup's `Element.ownText()` walks only the element's direct
`TextNode` children, not text nested inside child elements -- the
same distinction bs4 draws between a tag's direct
`bs4.NavigableString` children and its full `.get_text()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `Tag` |  | The element to search within. |
| `selector` | `str` |  | A plain (soupsieve-legal) CSS selector. |
| `regex` | `str` |  | The pattern each candidate's own (whitespace-collapsed) text must `re.search`-match. |

**Returns**

Every `selector` match whose own text contains a `regex` match, in document order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_matching_own
soup = parse_html('<div class="card-header">Coach <b>Info</b></div>')
select_matching_own(soup, "div.card-header", r"^Coach")
# [<div class="card-header">Coach <b>Info</b></div>]
```

### shot_js_to_html {#shot_js_to_html}

`shot_js_to_html(js: 'str') -> 'list[Tag]'`

Converts client-side `addShot(...)` JS calls into parseable

`circle.shot` HTML, for pages where the shot map is built on the fly
rather than baked into the initial HTML (`ShotEventParser
.shot_js_to_html`, `:266-283`). See the module docstring's "Scala
idiom decision" note -- the Scala's `builders`/`browser` parameters
are dropped here since the Scala body never actually uses them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `js` | `str` |  | The concatenated `<script>` text containing one or more `addShot(x, y, ..., 'title', ...)` calls, one per line. |

**Returns**

The `circle.shot` elements reconstructed from every matching line (non-matching lines, e.g. the `addShot` function definition line itself, are silently skipped).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import shot_js_to_html
js = "addShot(27.0, 77.0, 392, false, 1, 'title text', 'class', false);"
circles = shot_js_to_html(js)
```
