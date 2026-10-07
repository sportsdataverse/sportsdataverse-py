---
title: "WBB — additional Python functions — stats.ncaa.org: sum_shot–validate_lineup"
sidebar_label: "stats.ncaa.org: sum_shot–validate_lineup"
sidebar_position: 4
description: "WBB — additional Python functions — stats.ncaa.org: sum_shot–validate_lineup — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — stats.ncaa.org: sum_shot–validate_lineup

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
