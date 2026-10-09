# Package — additional Python functions — Validation

> Package — additional Python functions — Validation — function reference in sdv-py, the SportsDataverse Python package.

### validate_game {#validate_game}

`validate_game(frame: 'pl.DataFrame', league: 'str', *, header: 'dict | None' = None, source: 'str' = 'espn', summary: 'dict | None' = None, box: 'dict | None' = None) -> 'GameReport'`

Validate one processed game against the packaged invariant rules.

Pure and offline: nothing is fetched, nothing is written, the frame is not
mutated. A rule whose columns the frame lacks is skipped rather than failed,
so a slim frame validates the rules it can support.

For a source whose producer names the same quantities differently,
`SOURCE_COLUMNS` supplies the ESPN-shaped aliases on a view of the
frame, and `NOT_APPLICABLE` names the rules that source cannot
support at all -- those are reported in `not_applicable` rather than
skipped silently.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `frame` | `DataFrame` |  | one game's processed plays, in processor row order -- the `plays_frame` attribute of `NFLPlayProcess` / `CFBPlayProcess`. |
| `league` | `str` |  | `"nfl"` or `"cfb"`. |
| `header` | `dict \| None` | `None` | the game's ESPN-shaped `header` dict. Only used to resolve `game_id` / `season` when the frame carries neither. |
| `source` | `str` | `'espn'` | the source the game came from (`"espn"`, `"shield"`, `"cbs"`, `"yahoo"`, `"fox"`, `"ncaa"`). Rules that only judge ESPN's own feed are skipped for an adapted source. |
| `summary` | `dict \| None` | `None` | the full ESPN-shaped summary, when available. Enables the header-score, final-WP, dropped-play, drive-count and ESPN box rules. |
| `box` | `dict \| None` | `None` | the processor's `advBoxScore` dict. Enables the team box and team EPA aggregations. |

**Returns**

`ok` is True when no rule fired at `error` severity.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
from sportsdataverse.validation import validate_game

proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
game = proc.run_processing_pipeline()
report = validate_game(proc.plays_frame, "nfl", summary=proc.json, box=game.get("advBoxScore"))
report.ok, sorted(report.counts_by_rule)
```
