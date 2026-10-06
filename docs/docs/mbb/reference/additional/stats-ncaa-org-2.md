---
title: "MBB — additional Python functions — stats.ncaa.org: get_per–validate_lineup"
sidebar_label: "stats.ncaa.org: get_per–validate_lineup"
sidebar_position: 3
description: "MBB — additional Python functions — stats.ncaa.org: get_per–validate_lineup — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — stats.ncaa.org: get_per–validate_lineup

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

### same_school {#same_school}

`same_school(a: 'str', b: 'str') -> 'bool'`

Whether two team-name spellings denote the same school.

Exact match, or both names inside one `team_name_equivalents` class.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `str` |  | One team-name spelling. |
| `b` | `str` |  | The other team-name spelling. |

**Returns**

`True` if the two names refer to the same school.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import same_school
same_school("New Orleans", "LSU New Orleans")  # True
same_school("Miami (FL)", "Miami (OH)")        # False
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

### start_time_from_period {#start_time_from_period}

`start_time_from_period(period: 'int', is_women_game: 'bool') -> 'float'`

The game-clock time (minutes elapsed) a period starts at

(`ExtractorUtils.scala:272-281`).

Women's games play four 10-minute quarters then 5-minute overtimes; men's
games play two 20-minute halves then 5-minute overtimes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `period` | `int` |  | The 1-indexed period number (1/2 = halves for men, 1-4 = quarters for women, 5+ = overtimes for both). |
| `is_women_game` | `bool` |  | Whether to use the women's (quarters) or men's (halves) period schedule. |

**Returns**

The game-clock minute the period begins at.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import start_time_from_period
start_time_from_period(2, is_women_game=False)  # 20.0 (men's 2nd half)
start_time_from_period(1, is_women_game=True)  # 0.0 (women's 1st quarter)
start_time_from_period(6, is_women_game=False)  # 45.0 (men's 2nd OT)
```

### sum_event_stats {#sum_event_stats}

`sum_event_stats(lhs: 'LineupEventStats', rhs: 'LineupEventStats') -> 'LineupEventStats'`

Field-wise add two :class:`~sportsdataverse.mbb.mbb_ncaa_models

.LineupEventStats` (`protected def sum_event_stats`, `LineupUtils.scala
:1534-1622`, debug-only -- the Scala's own docstring says "just used for
debug"). The Scala builds this via `shapeless.Generic` field-zipping;
this port is an explicit field-by-field call since Python has no
equivalent generic-programming machinery.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lhs` | `LineupEventStats` |  | The left-hand stat tree. |
| `rhs` | `LineupEventStats` |  | The right-hand stat tree. |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEventStats` with every field summed (see the module's private sum_*` helpers for the `Optional`/nested-field summing rules).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import sum_event_stats
from sportsdataverse.mbb.mbb_ncaa_models import LineupEventStats

sum_event_stats(LineupEventStats.empty(), LineupEventStats.empty()).num_events
```

### sum_shot_infos {#sum_shot_infos}

`sum_shot_infos(shot_infos: 'list[PlayerShotInfo]') -> 'Optional[PlayerShotInfo]'`

Field-wise sum a list of :class:`~sportsdataverse.mbb.mbb_ncaa_models

.PlayerShotInfo`\ s (`sum_shot_infos`, `LineupUtils.scala:1625-1655`,
debug-only).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shot_infos` | `list[PlayerShotInfo]` |  | The list to combine, in order. |

**Returns**

`None` if `shot_infos` is empty; the single element if there's exactly one; otherwise a left-fold of pairwise field-wise sums (`reduceOption`).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import sum_shot_infos
from sportsdataverse.mbb.mbb_ncaa_models import PlayerShotInfo

sum_shot_infos([PlayerShotInfo(ast_3pm=(1, 0, 0, 0, 0)), PlayerShotInfo(ast_3pm=(0, 1, 0, 0, 0))])
```

### td_at {#td_at}

`td_at(row: 'Tag', n: 'int') -> 'Optional[Tag]'`

JSoup `row >?> element("td:eq(n)")`: the `n`-th `<td>` child.

Soupsieve has no `:eq()` positional pseudo-class (unlike JSoup), so
this is a plain 0-indexed lookup into `row.find_all("td")`, guarded
against an out-of-range index (JSoup's `>?>` returns `None` rather
than raising when the selector matches nothing).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `row` | `Tag` |  | The row (or other container) element to search. |
| `n` | `int` |  | The 0-indexed `<td>` position. |

**Returns**

The `n`-th `<td>` descendant, or `None` if `row` has fewer than `n + 1` of them.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import parse_html, td_at
soup = parse_html("<tr><td>A</td><td>B</td></tr>")
row = soup.find("tr")
td_at(row, 1).get_text()  # "B"
td_at(row, 5)  # None
```

### transform_shot_location {#transform_shot_location}

`transform_shot_location(x: 'float', y: 'float', second_half_switch: 'bool', team_shooting_left_in_first_period: 'bool', is_offensive: 'bool') -> 'tuple[float, float, float, float]'`

Transforms a raw SVG pixel location into feet from the basket, always

oriented as if shooting towards the left goal (`ShotEventParser
.transform_shot_location`, `:588-620`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `x` | `float` |  | Raw SVG `cx` pixel coordinate. |
| `y` | `float` |  | Raw SVG `cy` pixel coordinate. |
| `second_half_switch` | `bool` |  | Whether this shot is in the "other" half of the game from `team_shooting_left_in_first_period` (each `False` factor below flips which side is treated as "left"). |
| `team_shooting_left_in_first_period` | `bool` |  | Whether the team under analysis shot towards the left goal in the first period (see `is_team_shooting_left_to_start`). |
| `is_offensive` | `bool` |  | Whether the team under analysis is shooting (an opponent shot flips the expected side again). |

**Returns**

`(x, y, alt_x, alt_y)` in feet -- the believed-correct location, then the alternative (mirror-image) location, both relative to the goal the shot is (believed to be) attacking.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_shot_parser import transform_shot_location
transform_shot_location(310.2, 235, False, False, True)
```

### update_config {#update_config}

`update_config(**kwargs: 'object') -> 'NcaaFetchConfig'`

Update the active config in place.

**Returns**

The (mutated) global config object.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import update_config
update_config(proxy_url="http://user:pass@1.2.3.4:8080")
```

### validate_box_score {#validate_box_score}

`validate_box_score(team: 'TeamId', lineup: 'list[str]') -> 'Union[list[PlayerCodeId], ParseError]'`

Checks there are no duplicates in the lineup (``BoxscoreParser

.validate_box_score`, `:388-404``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamId` |  | The team the lineup belongs to (feeds `~sportsdataverse.mbb.mbb_ncaa_stints.build_player_code`'s team-scoped misspelling corrections). |
| `lineup` | `list[str]` |  | The raw player-name strings, in whatever order they were assembled by `inject_validated_players`. |

**Returns**

`lineup`, mapped to `~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId` (same order, no sort -- see the module docstring's "not sorted" note). When two teammates collide on the `{first-two-letters}{Surname}` scheme -- siblings, in practice -- **only the colliding players** are re-coded to `{First}{Last}` by disambiguate_sibling_codes`; every other player keeps the Scala-faithful code. This is a DELIBERATE divergence from `ExtractorUtils.scala`, which rejects the game: since a team's roster is the same all season, one sibling pair cost the team its ENTIRE season of lineups. A `~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError` is returned only when widening cannot separate them, i.e. two players with the SAME full name -- genuinely ambiguous, so still an error. Callers must not re-derive a code from a name after this point: `build_player_code` would undo the widening and silently drop one twin. Use `~sportsdataverse.mbb.mbb_ncaa_names.code_from_box`, which resolves against this roster.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import validate_box_score
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
validate_box_score(TeamId("Team"), ["Player One", "Player Two"])
```

### validate_lineup {#validate_lineup}

`validate_lineup(lineup_event: 'LineupEvent', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'list[ValidationError]'`

Flags a lineup stint as internally inconsistent, via 3 independent

checks (`LineupErrorAnalysisUtils.validate_lineup`, `:181-218`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_event` | `LineupEvent` |  | The lineup stint to validate. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (`players` is the full roster) -- used both to build the name-resolution context (see `~sportsdataverse.mbb.mbb_ncaa_names.build_tidy_player_context`) and, indirectly, as the source of `players_out` for jersey-number resolution inside `~sportsdataverse.mbb .mbb_ncaa_names.tidy_player`. |
| `valid_player_codes` | `set[str]` |  | Every player code that's actually on the box score / roster for this team-season. |

**Returns**

The failing `ValidationError`\ s, in declaration order (see the module docstring's "Return shape" note) -- empty if `lineup_event` is clean. * `ValidationError.WRONG_NUMBER_OF_PLAYERS` -- `lineup_event` doesn't have exactly 5 players on the floor. * `ValidationError.UNKNOWN_PLAYERS` -- some player on the floor isn't in `valid_player_codes`. * `ValidationError.INACTIVE_PLAYERS` -- some player mentioned in `lineup_event`'s own (team-side) raw game events resolves to a code not in `valid_player_codes` (i.e. isn't on the floor, per the lineup being validated).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import validate_lineup
errors = validate_lineup(lineup_event, box_lineup, {"MiMitchell", "BbBob"})
assert not errors  # a clean lineup returns []
```
