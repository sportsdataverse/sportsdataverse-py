---
title: "MBB — additional Python functions — stats.ncaa.org: enrich_stats–remove_html"
sidebar_label: "stats.ncaa.org: enrich_stats–remove_html"
sidebar_position: 4
description: "MBB — additional Python functions — stats.ncaa.org: enrich_stats–remove_html — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — stats.ncaa.org: enrich_stats–remove_html

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

### enrich_sub_error {#enrich_sub_error}

`enrich_sub_error(location: 'str', base_id: 'str', error: 'ParseError') -> 'list[ParseError]'`

Adds top-level location information to a single sub-error, returning a

list for consistency (`ParseUtils.enrich_sub_error`, `ParseUtils.scala:91-93`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `location` | `str` |  | The module in which the (now top-level) error occurred. |
| `base_id` | `str` |  | An id fragment prepended (bracket-wrapped, if non-empty) to `error`'s existing `id`. |
| `error` | `ParseError` |  | The child-parser error to enrich. |

**Returns**

`enrich_sub_errors` applied to a single-element `[error]` list.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import build_sub_error, enrich_sub_error

child_err = build_sub_error("game_score", error="Could not find score")
enrich_sub_error("ncaa.parse_playbyplay", "", child_err)
```

### enrich_sub_errors {#enrich_sub_errors}

`enrich_sub_errors(location: 'str', base_id: 'str', errors: 'list[ParseError]') -> 'list[ParseError]'`

Adds top-level location information to a list of sub-errors generated

by a child parser (`ParseUtils.enrich_sub_errors`, `ParseUtils.scala:87-89`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `location` | `str` |  | The module in which the (now top-level) error occurred. |
| `base_id` | `str` |  | An id fragment prepended (bracket-wrapped, if non-empty) to each error's existing `id`. |
| `errors` | `list[ParseError]` |  | The child-parser errors to enrich (their `location` is **replaced**, not merged -- matching the Scala's `ParseError(location, ..., error.messages)` construction, which discards the child's own `location`). |

**Returns**

A new list of `ParseError`, one per input error, each with `location` set to `location` and `id` set to `build_error_id(base_id) + error.id`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import build_sub_error, enrich_sub_errors

child_err = build_sub_error("game_time", error="Could not find time")
enrich_sub_errors("ncaa.parse_playbyplay", "", [child_err])
```

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

### lineup_stats_bucket {#lineup_stats_bucket}

`lineup_stats_bucket(ev: 'LineupEvent', *, avg_eff: 'float' = 100.0, opponent_baselines: 'Optional[dict[str, float]]' = None, doc_count: 'int' = 1) -> 'LineupStatSet'`

Assemble one lineup's full 254-field `{value}` bucket.

`lineup_stats_bucket` is the Python entry point for stage 2 of the port (see the
module docstring) -- the faithful composition of this module's factories in the order
`commonLineupAggregations.ts` (572-line ES aggregation) issues them: `sum` (
sum_fields`) -> merge the play-type `pts`/`poss` bucket_script (
play_type_pts_poss`) -> mint every other rate bucket_script (
all_rate_fields`) -> the SOS-adjusted-efficiency bucket_script (
adj_fields`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ev` | `LineupEvent` |  | One already-summed lineup event (`team_stats`/`opponent_stats` populated by stage 1, `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.enrich_lineup`). |
| `avg_eff` | `float` | `100.0` | League-average efficiency passed through to adj_fields`. |
| `opponent_baselines` | `Optional[dict[str, float]]` | `None` | SOS baseline lookup passed through to adj_fields`; only `None` (no baselines) is implemented. |
| `doc_count` | `int` | `1` | The ES `doc_count` for this bucket (number of raw events folded in). |

**Returns**

The full bucket: every `total_*`/rate/adj field wrapped in `{"value": <float>}`, plus the structural keys `key`, `players_array`, `doc_count` (bare, unwrapped).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_aggregation import lineup_stats_bucket

bucket = lineup_stats_bucket(enriched_event, doc_count=7)
bucket["off_ppp"]["value"]
```

### lineup_stats_buckets {#lineup_stats_buckets}

`lineup_stats_buckets(evs: 'list[LineupEvent]', *, avg_eff: 'float' = 100.0, opponent_baselines: 'Optional[dict[str, float]]' = None) -> 'list[LineupStatSet]'`

Group events by lineup, fold each group's stats, and mint one bucket per lineup.

Python entry point for the ES `terms` aggregation over `key` (grouping by
bucket_key`) that feeds each lineup's docs into
`commonLineupAggregations.ts`'s `sum` aggs -- see
`cbb-on-off-analyzer/src/utils/es-queries/commonLineupAggregations.ts`. This
is the list-form producer the `LineupStatSet` consumers (`mbb_lineup_stats`)
read from; `lineup_stats_bucket` handles a single already-folded event.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `evs` | `list[LineupEvent]` |  | Raw per-possession-chunk lineup events (`team_stats`/`opponent_stats` populated by stage 1, `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich .enrich_lineup`), one lineup's floor time possibly split across many events. |
| `avg_eff` | `float` | `100.0` | League-average efficiency passed through to each bucket. |
| `opponent_baselines` | `Optional[dict[str, float]]` | `None` | SOS baseline lookup passed through to each bucket; only `None` (no baselines) is implemented (see adj_fields`). |

**Returns**

One `LineupStatSet` per distinct lineup (bucket_key`), in first-seen order, with `doc_count` set to that lineup's event count.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_aggregation import lineup_stats_buckets

buckets = lineup_stats_buckets(enriched_events)
buckets[0]["off_poss"]["value"]
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

### name_in_v0_box_format {#name_in_v0_box_format}

`name_in_v0_box_format(v1_name: 'str') -> 'str'`

Switch a v1-box-format name (`"first_name names"`) to v0-box format

(`"names, first_name"`) (`ExtractorUtils.scala:59-81`).

Handles a v0-PbP-style all-caps input (`"SURNAME,NAME"`, still seen in
older files even in v1-format seasons) by first flipping it to guaranteed
v1 shape, then splits on the first space to get `first`/`last`. A
`last` starting with `"("` is treated as a nickname parenthetical
(e.g. `"Russell (Deuce) Dean"`) and re-split via
COMPLEX_V0_CASE_RE` -- **ported verbatim including its literal
quirk**: the regex's second capture group keeps the leading space before
the trailing surname (e.g. yields `" Dean, Russell (Deuce)"`, not
`"Dean, Russell Deuce"` as the Scala source comment's stated *intent*
describes) and the parenthesis characters are not stripped. This is
upstream behavior, not a Python-side bug -- the Scala's own pattern-match
reproduces exactly this, so faithful porting keeps it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `v1_name` | `str` |  | The player name as it appears in a v1 (2018+) NCAA roster or box-score row. |

**Returns**

The name in v0 (`"names, first_name"`) format, or `v1_name` (via the guaranteed-v1-format intermediate) unchanged if it has no space to split on.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import name_in_v0_box_format
name_in_v0_box_format("Daniel Akin")  # "Akin, Daniel"
name_in_v0_box_format("AKIN,DANIEL")  # "AKIN, DANIEL" (old PbP form, flipped then re-split)
```

### ncaa_mbb_box_scores {#ncaa_mbb_box_scores}

`ncaa_mbb_box_scores(game_ids: 'Union[str, int, Iterable[Union[str, int]]]', *, multi_games: 'bool' = False, fetcher: 'Optional[_SupportsFetchIndividualStats]' = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Box scores for one or more NCAA games (bigballR `get_box_scores` port).

Multi-game driver over `parse_ncaa_bb_box`
(`bigballR/R/all_functions.R:3603-3678`): drops null ids, isolates
per-game errors (failed ids are reported and skipped), binds rows, and
optionally aggregates across games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Union[str, int, Iterable[Union[str, int]]]` |  | One id or an iterable of NCAA contest ids. |
| `multi_games` | `bool` | `False` | When `True`, aggregate one row per `(player, clean_name, team)` -- counters summed, `g` = games played, rates recomputed from the sums (R's `multi.games`; grouping adapted per module docstring). |
| `fetcher` | `Optional[_SupportsFetchIndividualStats]` | `None` | Optional injected fetcher exposing `fetch_game_individual_stats` (for offline replay/tests). Defaults to a fresh `NcaaFetcher.with_browser()` context per call -- stats.ncaa.org sits behind an Akamai challenge that the plain transport cannot clear. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame (or pandas with `return_as_pandas=True`): per-game rows in the `parse_ncaa_bb_box` contract, or the aggregated `multi_games` contract.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_box_stats import ncaa_mbb_box_scores
df = ncaa_mbb_box_scores(["6470186", "6479639"])
print(df.shape)

# Season aggregate for a scraped id list

agg = ncaa_mbb_box_scores(ids, multi_games=True)

# Offline with an injected fetcher

df = ncaa_mbb_box_scores("6470186", fetcher=my_fetcher)
```

### ncaa_mbb_date_games {#ncaa_mbb_date_games}

`ncaa_mbb_date_games(date: 'Optional[str]' = None, *, conference: 'str' = 'All', conference_id: 'Optional[int]' = None, fetcher: "'Optional[NcaaFetcher]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Discover every NCAA MBB game played on a date (bigballR `get_date_games`).

Fetches `stats.ncaa.org/season_divisions/{sid}/scoreboards` for the
date's season and returns one row per game with the `/contests/{id}`
game id needed by the play-by-play / box-score scrapers. Port of bigballR
`get_date_games` (all_functions.R:1119-1427).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | `"MM/DD/YYYY"`. Defaults to yesterday (R default). |
| `conference` | `str` | `'All'` | Conference name filter (e.g. `"ACC"`, `"Big Ten"`, `"Metro"` / `"MAAC"`); case/punctuation-insensitive, and both the current stats.ncaa.org label and bigballR's abbreviation work. Default `"All"` (every conference). Unknown names raise. |
| `conference_id` | `Optional[int]` | `None` | Explicit stats.ncaa.org conference id; overrides *conference* when given (R's `conference.ID`). |
| `fetcher` | `Optional[NcaaFetcher]` | `None` | Injectable `~sportsdataverse.mbb.mbb_ncaa_fetch .NcaaFetcher` (tests pass an offline fake). `None` uses `NcaaFetcher.with_browser()` — the page is JS-rendered behind Akamai bm-verify, so the browser transport is the live default. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game with columns `date, start_time, home, away, box_id, game_id, home_score, away_score, attendance, neutral_site, home_wins, home_losses, away_wins, away_losses` (`SCOREBOARD_SCHEMA`). Scores stay Utf8 — they hold `"Canceled"` / `"Ppd"` for unplayed games; `game_id` is null for games without a box score.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_scoreboard import ncaa_mbb_date_games
games = ncaa_mbb_date_games("11/11/2025")
print(games.shape)

# Useful parameter combination

acc_pd = ncaa_mbb_date_games("02/01/2025", conference="ACC",
                             return_as_pandas=True)

# Pipeline next step (one line)

games.filter(pl.col("game_id").is_not_null())["game_id"].to_list()
```

### ncaa_mbb_join_pbp_shots {#ncaa_mbb_join_pbp_shots}

`ncaa_mbb_join_pbp_shots(pbp: 'pl.DataFrame', shots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Attach shot-chart coordinates to play-by-play rows (bigballR

`join_pbp_shots`, `get_shot_locations.R:93-135`).

FG attempts (`shot_value` 2/3) are matched to chart shots on
`(game_id, game_seconds, event_result == shot_result, shot_no)` where
`shot_no` is the within-second same-result sequence number on BOTH
sides — free throws and non-shot rows are deliberately excluded from
matching (the chart plots FGs only) and pass through NA-filled. Row
count and per-game row order are preserved; the explicit `shot_dist`
carry-through is fork-skew fix #11 (wbigballR's unexported copy drops
it). Works identically for the WBB extension — feed it quarter-model
pbp + shots frames.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | The 35-column snake_case pbp contract frame (see `~sportsdataverse.mbb.mbb_ncaa_game_pbp.PBP_SCHEMA`). |
| `shots` | `DataFrame` |  | A `SHOTS_SCHEMA` frame (from `parse_ncaa_bb_shots` / `ncaa_mbb_shot_locations`) covering exactly the same game ids. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The input pbp frame + `team`, `player`, `x`, `y`, `shot_dist` (null on non-FG rows and unmatched FG rows), sorted by (game_id, original per-game row order) exactly as R's `arrange(row, .by_group = TRUE)`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_game_pbp import ncaa_mbb_play_by_play
from sportsdataverse.mbb.mbb_ncaa_shots import (
    ncaa_mbb_join_pbp_shots,
    ncaa_mbb_shot_locations,
)
pbp = ncaa_mbb_play_by_play(["6470186"])
shots = ncaa_mbb_shot_locations(["6470186"])
joined = ncaa_mbb_join_pbp_shots(pbp, shots)

# Pipeline next step (one line)

joined.filter(pl.col("x").is_not_null()).head()
```

### ncaa_mbb_player_stats {#ncaa_mbb_player_stats}

`ncaa_mbb_player_stats(pbp: 'pl.DataFrame', *, multi_games: 'bool' = False, simple: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-player box stats.

Port of bigballR `get_player_stats` (`all_functions.R:2810-3177`) +
its `get_mins` helper (`:3240-3263`). Counting stats are summarised
per (game, team, player), assists counted from `player_2`, minutes and
offensive possessions derived from the ten on-court columns, and rates
(FG%, TS%, eFG%, rim/mid splits, ...) computed from the counters and
rounded to 3 decimals with R's `round` semantics. With
`multi_games=True` the per-game rows are summed per (player, team),
every rate is recomputed from the summed counters (never averaged), and
`GP`/`GS` are appended.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract. May span multiple games. |
| `multi_games` | `bool` | `False` | When True, aggregate across games per (player, team) — the season-stat surface. When False (default, R parity), treat each game separately and keep the game id columns. |
| `simple` | `bool` | `False` | When True, return the reduced 33-column (multi) / 35-column (per-game) surface without the transition / assisted / putback / block-location splits. |
| `fix_tip_in` | `bool` | `True` | When True (default), rim and putback stats count the scrape engine's real `"Tip In"` vocabulary. When False, reproduce R's literal `"Tip-In"` test (`all_functions.R:2827`) — tip-ins silently excluded — for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`): one row per player+team (+game when `multi_games=False`). Columns follow `PLAYER_STATS_COLUMNS` / `PLAYER_STATS_SIMPLE_COLUMNS` / `PLAYER_GAME_STATS_COLUMNS` / `PLAYER_GAME_STATS_SIMPLE_COLUMNS`. Rows sorted by the group keys (byte order, matching dplyr's C-locale group order). Empty input yields an empty frame with the documented schema.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_player_stats
season = ncaa_mbb_player_stats(pbp, multi_games=True)
print(season.shape)

# Reduced surface, pandas out

df_pd = ncaa_mbb_player_stats(pbp, multi_games=True, simple=True, return_as_pandas=True)

# Pipeline next step (one line)

season.filter(pl.col("mins") > 50).sort("pts", descending=True).head()
```

### ncaa_mbb_possessions {#ncaa_mbb_possessions}

`ncaa_mbb_possessions(pbp: 'pl.DataFrame', *, simple: 'bool' = False, fix_cross_game_leak: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into one row per possession.

Port of bigballR `get_possessions` (`all_functions.R:3686-3745`).
Groups by the possession keys stamped upstream by the scrape engine
(`poss_num`, `poss_team`, the ten on-court lineup columns, plus game
identity), drops possessions with any missing on-court player, and — in
the full variant — sorts each row's home/away lineup alphabetically so a
given lineup always occupies the same columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`parse_ncaa_bb_game_pbp` output). May span multiple games; rows must be in scrape order. |
| `simple` | `bool` | `False` | When True, return only the 17-column possession/points frame (`all_functions.R:3687-3694`) with lineups in on-court order. When False (default), return the full 28-column frame with per-possession context columns and alpha-sorted lineups. |
| `fix_cross_game_leak` | `bool` | `True` | When True (default, and the CORRECT behavior), window the `start_event_type` lag with `.over("game_id")` so a game's first possession has a null start event instead of inheriting the PREVIOUS game's last event. When False, reproduce R's ungrouped `dplyr::lag` (`all_functions.R:3698`) and its cross-game leak. Parity tests pass False. Ignored when `simple=True` (that variant emits no `start_event_type`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per possession — 28 columns per `POSSESSION_SEG_SCHEMA` (full) or 17 per `POSSESSIONS_SIMPLE_SCHEMA` (simple). Empty input yields an empty frame carrying the documented schema.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_possession_seg import ncaa_mbb_possessions
poss = ncaa_mbb_possessions(pbp)
print(poss.shape)

# Simple points-per-possession variant

poss_pd = ncaa_mbb_possessions(pbp, simple=True, return_as_pandas=True)

# Pipeline next step (one line)

poss.group_by("poss_team").agg(pl.col("pts").mean())
```

### ncaa_mbb_shot_locations {#ncaa_mbb_shot_locations}

`ncaa_mbb_shot_locations(game_ids: "'Sequence[object]'", *, fetcher: 'Optional[_SupportsFetchGameBox]' = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, Any]'"`

Scrape MBB shot locations for one or more games (bigballR

`get_shot_locations`, `get_shot_locations.R:3-89`).

Fetches each game's `stats.ncaa.org/contests/{id}/box_score` page and
parses the embedded shot-chart JS through `parse_ncaa_bb_shots`.
NA ids are dropped up front (R `:5`); per-game "shots found" messages
go to the module logger (R `message`, `:69-70`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[object]` |  | NCAA contest ids; `None`/NaN entries are dropped. |
| `fetcher` | `Optional[_SupportsFetchGameBox]` | `None` | Optional injected fetcher exposing `fetch_game_box` (for tests/offline use). Defaults to a fresh `NcaaFetcher.with_browser()` context per call. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

All games' shots row-bound (zero-row `SHOTS_SCHEMA` frame when no ids survive or no charts are found).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shots import ncaa_mbb_shot_locations
df = ncaa_mbb_shot_locations(["6470186", "6479639"])
print(df.shape)

# Offline with an injected fetcher

df = ncaa_mbb_shot_locations(["6470186"], fetcher=my_fetcher)

# Pipeline next step (one line)

df.group_by("team").agg(pl.col("shot_dist").mean()).head()
```

### ncaa_mbb_team_stats {#ncaa_mbb_team_stats}

`ncaa_mbb_team_stats(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-team game stats.

Port of bigballR `get_team_stats` (`all_functions.R:2530-2538`): the
ten on-court columns are blanked so every row shares one "lineup", then
`get_lineups` (`ncaa_mbb_lineups`) runs per game and the lineup
key columns are dropped — yielding two rows (one per team) per game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract. May span multiple games. |
| `include_transition` | `bool` | `False` | When True, append the trans`/half` split surface plus `o_trans_pct`/`d_trans_pct`. |
| `fix_tip_in` | `bool` | `True` | When True (default), rim stats count the scrape engine's real `"Tip In"` vocabulary; `False` reproduces R's literal `"Tip-In"` bug for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per team per game — `TEAM_STATS_COLUMNS` (73) or `TEAM_STATS_TRANSITION_COLUMNS` with `include_transition=True`. Games ordered by the Utf8 `game_id` byte sort (R's do() sorts a numeric ID — identical for equal-width ids), teams within a game byte-sorted. Empty input yields an empty frame with the documented schema.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_team_stats
teams = ncaa_mbb_team_stats(pbp)
print(teams.shape)

# Transition splits, pandas out

df_pd = ncaa_mbb_team_stats(pbp, include_transition=True, return_as_pandas=True)

# Pipeline next step (one line)

teams.sort("netrtg", descending=True).head()
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

### remove_html_encoding {#remove_html_encoding}

`remove_html_encoding(html_str: 'str') -> 'str'`

Undo a handful of literal HTML entity escapes (``ExtractorUtils

.remove_html_encoding`, `ExtractorUtils.scala:25-33`). **Scope
addition, Task 5e.5** -- the first consumer is
`mbb_ncaa_shot_parser.parse_shot_html` (the `player` name / shooting
team name extracted from an SVG shot's `<title>` text).

In practice bs4/lxml already decode standard HTML entities (`&#39;`,
`&quot;`, `&amp;``) while parsing text nodes, so this is usually a
no-op by the time it runs on already-parsed text -- ported anyway for
exact behavioral parity with any double-escaped input the upstream
Scala guards against (JSoup has the same auto-decoding behavior, so the
Scala original is equally a defensive no-op in the common case).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `html_str` | `str` |  | Any string, typically already-parsed element text. |

**Returns**

`html_str` with `&#39;`/`&quot;`/`&amp;` replaced by their literal characters, only if `"&"` appears at all (short-circuit matching the Scala's `if (html_str.indexOf("&") >= 0)` guard).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import remove_html_encoding
remove_html_encoding("De&#39;Shayne")  # "De'Shayne"
remove_html_encoding("Plain Name")  # "Plain Name" (unchanged)
```
