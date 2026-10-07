---
title: "MBB — additional Python functions — stats.ncaa.org: reorder_and–validate_lineup"
sidebar_label: "stats.ncaa.org: reorder_and–validate_lineup"
sidebar_position: 5
description: "MBB — additional Python functions — stats.ncaa.org: reorder_and–validate_lineup — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — stats.ncaa.org: reorder_and–validate_lineup

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

**Returns**

The live singleton, now holding the env-var-derived defaults again.

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
