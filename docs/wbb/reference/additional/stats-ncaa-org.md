# WBB — additional Python functions — stats.ncaa.org: BadLineupClump–enrich_shot

> WBB — additional Python functions — stats.ncaa.org: BadLineupClump–enrich_shot — function reference in sdv-py, the SportsDataverse Python package.

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

#### NcaaFetcher.with_browser

`NcaaFetcher.with_browser(config: 'Optional[NcaaFetchConfig]' = None, *, proxy_pool: "Optional['list[str]']" = None, **browser_opts: 'object') -> "'NcaaFetcher'"`

Build a fetcher wired to the **suggested** browser transport.

The go-to constructor for scraping game-detail pages
(play-by-play / individual-stats / box-score): it attaches a

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config` | `Optional[NcaaFetchConfig]` | `None` |  |
| `proxy_pool` | `Optional['list[str]']` | `None` |  |

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

**Methods**

#### ParseError.single

`ParseError.single(location: 'str', id: 'str', message: 'str') -> "'ParseError'"`

Single-message convenience constructor (`ParseError.apply`,

`ParseError.scala:19-21` -- the companion object's single-`message`
overload). Python has no method overloading, so this is a
`classmethod` rather than a second constructor signature.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `location` | `str` |  | The module in which the error occurred. |
| `id` | `str` |  | The module-specific id for which the error occurred. |
| `message` | `str` |  | A single human-readable description of the error. |

**Returns**

A `ParseError` with `messages=[message]`.

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

### ScheduleBuilders {#ScheduleBuilders}

`ScheduleBuilders(team_name_finder: 'Callable[[BeautifulSoup], Optional[str]]', neutral_game_finder: 'Callable[[BeautifulSoup], list[str]]') -> None`

One version-era's HTML finder functions (``TeamScheduleParser

.base_builders`, `TeamScheduleParser.scala:36-39``).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_name_finder` | `Callable[[BeautifulSoup], Optional[str]]` |  |  |
| `neutral_game_finder` | `Callable[[BeautifulSoup], list[str]]` |  |  |

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

### ValidationError {#ValidationError}

`ValidationError(*values)`

The 3 ways a lineup can be declared invalid, in Scala declaration

(ordinal) order (`LineupErrorAnalysisUtils.ValidationError`, `:18-20`).
Member order is load-bearing -- see the module docstring's "Return
shape" note.

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

### alias_combos {#alias_combos}

`alias_combos(first: 'str', last: 'str', to_name: 'str') -> 'dict[str, str]'`

Pair each of `combos`' three name variants with a shared alias

target (`DataQualityIssues.alias_combos`, `DataQualityIssues.scala:351-356`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first (mis-recorded) first name. |
| `last` | `str` |  | The player's (mis-recorded) last name. |
| `to_name` | `str` |  | The canonical `"Lastname, Firstname"` this player should resolve to. |

**Returns**

A dict mapping each of the three name variants to `to_name`.

### analyze_and_fix_clumps {#analyze_and_fix_clumps}

`analyze_and_fix_clumps(clump: 'BadLineupClump', box_lineup: 'LineupEvent', valid_player_codes: 'set[str]') -> 'tuple[list[LineupEvent], BadLineupClump]'`

Runs the full self-healing fixer pipeline over one bad-lineup clump

(`LineupErrorAnalysisUtils.analyze_and_fix_clumps`, `:556-610`).

The strict, order-dependent sequence (each stage threads
`(fixed_so_far + newly_fixed, still_to_fix)`):

1. `handle_common_sub_bug`,
2. `find_missing_subs`,
3. `add_missing_players`,
4. `find_missing_subs` **again** -- the Scala's own comment
   (`:587-588`) explains: "Try this again since add_missing_players can
   go too far". Step 3 back-fills onto every event and can push some past
   5 players; the second trim pass removes the over-add.

Finally every accumulated `fixed` lineup gets a fresh `lineup_id` via
`~sportsdataverse.mbb.mbb_ncaa_stints.build_lineup_id` (`:597-605`)
-- the fixers changed the on-floor `players`, so the id computed during
stint construction is stale. The Scala's `debug`-gated
`analyze_unfixed_clumps` call (`:593-596`) is dropped (see the module
docstring's "Debug-only" note); it only prints.

The Scala wraps the whole pipeline in `Some(clump).map { ... }
.getOrElse((Nil, clump))`, but `Some(_)` is never empty so the
`getOrElse` is dead -- omitted here.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clump` | `BadLineupClump` |  | The bad-lineup clump to repair. |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (roster + name context). |
| `valid_player_codes` | `set[str]` |  | Every player code on the box score / roster. |

**Returns**

`(fixed_lineups, still_to_fix)` -- every repaired lineup (with a recomputed `lineup_id`) and whatever clump the pipeline could not fix.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    analyze_and_fix_clumps,
)
fixed, still = analyze_and_fix_clumps(clump, box_lineup, valid_codes)
for lineup in fixed:
    print(lineup.lineup_id.value)
```

### attr_regex_filter {#attr_regex_filter}

`attr_regex_filter(tags: 'list[Tag]', attr: 'str', regex: 'str') -> 'list[Tag]'`

JSoup `[attr~=regex]`: candidates whose `attr` value matches

`regex`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `tags` | `list[Tag]` |  | Candidate tags to filter (typically the result of an earlier `.select()`/`.find_all()` call). |
| `attr` | `str` |  | The attribute name to test. |
| `regex` | `str` |  | The pattern the attribute value must `re.search`-match. |

**Returns**

The subset of `tags` that have `attr` set and whose value matches `regex`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_html import attr_regex_filter, parse_html
soup = parse_html('<div width="45%"></div><div width="10%"></div>')
attr_regex_filter(soup.find_all("div"), "width", r"^\[?4?5")
```

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

### cached_path {#cached_path}

`cached_path(path: 'str', *, cache_dir: 'Optional[Path]' = None) -> 'Path'`

Return the on-disk cache file path for *path*, without touching it.

Layout: `{cache_dir}/stats.ncaa.org/{dirs...}/{last}.html`, where the
URL path's `/`-separated segments become nested directories and a
query string is folded into the final filename as {safe_query}.html`
(unsafe characters replaced with `). Two different query strings for
the same base path therefore always produce two distinct cache files.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` |  | The stats.ncaa.org URL path (with its query string, if any). |
| `cache_dir` | `Optional[Path]` | `None` | The cache root; the active config's `cache_dir` when None. |

**Returns**

Where the page's HTML is (or would be) cached. The file need not exist.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_fetch import cached_path
cached_path("contests/4690813/play_by_play")
# .../stats.ncaa.org/contests/4690813/play_by_play.html
cached_path("contests/4690813/box_score?period_no=2")
# .../stats.ncaa.org/contests/4690813/box_score__period_no=2.html
```

### categorize_bad_lineups {#categorize_bad_lineups}

`categorize_bad_lineups(lineup_events: 'list[LineupEvent]') -> 'dict[int, tuple[int, int]]'`

Aggregates bad lineup events for display, by clump-leader player count

(`LineupErrorAnalysisUtils.categorize_bad_lineups`, `:617-633`,
display-only -- the Scala doc comment says "can live without tests").

Re-clumps `lineup_events` (each paired with `next_good=None` --
`clump_bad_lineups`'s grouping predicate never inspects
`next_good`, so this re-clumping is faithful to the Scala's own
`lineup_events.map(e => (e, None))`), then groups the resulting clumps
by `len(clump.evs[0].players)` (the FIRST event's player count -- `5`
means a lineup with a bad *player*, not a bad *count*).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `list[LineupEvent]` |  | The bad lineup events to categorize, in chronological order. |

**Returns**

Player count -> `(num_clumps, total_possessions)`, where `total_possessions` sums `team_stats.num_possessions` across every event in every clump in that group.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import categorize_bad_lineups
categorize_bad_lineups([bad_ev])  # {5: (1, bad_ev.team_stats.num_possessions)}
```

### clump_bad_lineups {#clump_bad_lineups}

`clump_bad_lineups(lineup_events: 'list[tuple[LineupEvent, Optional[LineupEvent]]]') -> 'list[BadLineupClump]'`

Groups consecutive bad lineup events into `BadLineupClump`\ s

(`LineupErrorAnalysisUtils.clump_bad_lineups`, `:229-263`).

The Scala original is a bespoke `foldLeft` (NOT the generic
`Clumper` utility used elsewhere in the codebase) that prepends onto
two nested lists -- the per-clump `evs` and the top-level clump list
-- and reverses both at the end. This port walks the input once and
appends directly (to the current clump's `evs`, or a new clump to the
result list), which produces the identical chronological order as the
Scala's prepend-then-double-reverse without needing an explicit reverse
step: mirroring a "prepend to the front, reverse at the end" fold as a
plain "append to the back" loop is behavior-preserving precisely because
reversing a prepend-built list restores insertion order.

The current clump extends to cover the next `(lineup, next_good)` pair
iff ALL 5 conditions hold, compared against the clump's LAST-ADDED event
(`last`, not its first event) (`:242-249`):

1. `lineup.team == last.team`
2. `lineup.opponent == last.opponent`
3. `lineup.start_min == last.end_min` (no time gap)
4. `len(lineup.players) == len(last.players)`
5. `len(lineup.players_in) == len(lineup.players_out)` -- this checks
   the INCOMING lineup's own in/out balance, not a comparison against
   `last` (an unbalanced sub is a bad sign in isolation, per the
   Scala's own comment at `:247`).

`TeamSeasonId` (`lineup.team` / `.opponent`) is a plain (non-frozen)
dataclass, so `==` is a field-wise value comparison out of the box --
no `PlayerCodeId`-unhashability workaround is needed here, since this
predicate only compares team identities and player-list lengths, never a
set of `PlayerCodeId`.

Each time a clump is extended, `next_good` is REPLACED with the
incoming pair's own second element (`:251`) -- the final clump's
`next_good` is always the LAST-extended event's `next`, discarding
whatever `next_good` an earlier extension set.

Starting a new clump uses the incoming pair's own `next` too (`:234`,
`:253`) -- a fresh clump's `next_good` is never inherited from the
clump before it.

The Scala's third `foldLeft` case (`:255-259`, matching a head clump
whose `evs` is empty) is dead code in practice -- every
`BadLineupClump` this function ever constructs starts with exactly one
event and is only ever appended to, so `evs` can never be empty. Omitted
here with this comment in place of an unreachable branch.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_events` | `list[tuple[LineupEvent, Optional[LineupEvent]]]` |  | `(lineup_event, next_good_or_None)` pairs, in chronological order. |

**Returns**

The clumps, in chronological order, each with `evs` in chronological order.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stint_validation import clump_bad_lineups
clumps = clump_bad_lineups([(bad_ev, good_ev)])
clumps[0].evs  # [bad_ev]
```

### combos {#combos}

`combos(first: 'str', last: 'str') -> 'list[str]'`

Generate the three name-string variants NCAA sources use for one

player (`DataQualityIssues.combos`, `DataQualityIssues.scala:330-337`).

The Scala signature takes a single `(String, String)` tuple, but every
call site (including the `fix_combos`/`alias_combos` helpers below
and the upstream `DataQualityIssuesTests` oracle) invokes it with two
positional arguments via Scala's tuple auto-conversion -- ported here as
a plain two-argument function since Python has no such conversion.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `first` | `str` |  | The player's first name. |
| `last` | `str` |  | The player's last name. |

**Returns**

`[f"{last}, {first}", f"{first} {last}", f"{last.upper()},{first.upper()}"]` -- new-box, new-PbP, and old-box/legacy-PbP formats respectively.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_data_quality import combos

combos("Makhi", "Mitchell")
# ['Mitchell, Makhi', 'Makhi Mitchell', 'MITCHELL,MAKHI']
```

### compute_league_averages_from_per_game {#compute_league_averages_from_per_game}

`compute_league_averages_from_per_game(teams: 'Sequence[TeamDetail]', fields: 'Sequence[str]' = ('efg', '3p', '2pmid', '2prim')) -> 'LeagueAverages'`

Possession-weighted league means per field (`computeLeagueAveragesFromPerGame`, `ts:189-221`).

For each field, the weighted mean of every team's per-game raw rate over
all their games; only games with a non-`None` raw and a positive weight
contribute. An empty accumulator yields `0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `Sequence[TeamDetail]` |  | All teams. |
| `fields` | `Sequence[str]` | `('efg', '3p', '2pmid', '2prim')` | The stat fields to average (default `STRENGTH_ADJUSTED_FIELDS`). |

**Returns**

`{field: {"league_off": float, "league_def": float}}`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_league_averages_from_per_game

teams = [{"team_name": "A", "opponents": [{"off_3p_made": 5, "off_3p_attempts": 10}]}]
print(compute_league_averages_from_per_game(teams, ["3p"])["3p"]["league_off"])  # 0.5
```

### compute_opponent_strengths {#compute_opponent_strengths}

`compute_opponent_strengths(team: 'TeamDetail', team_by_name: 'dict[str, TeamDetail]', fields: 'Sequence[str]', adj_values: 'AdjValues') -> 'dict[str, SideValues]'`

Schedule-weighted opponent strength per field (`computeOpponentStrengths`, `ts:253-299`).

**Cross-named on purpose:** `avg_opp_def` is weighted by the *offensive*
game weights and reads each opponent's `def` adjustment; `avg_opp_off`
is weighted by *defensive* weights and reads the opponent's `off`. Each
opponent value is its current adjusted value, falling back to its raw
per-game value when no adjustment exists yet. Games whose opponent is not
in `team_by_name` (or whose off+def weights are both `<= 0`) are
skipped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | The team whose schedule is being summarized. |
| `team_by_name` | `dict[str, TeamDetail]` |  | `{team_name: team_detail}` for opponent lookup. |
| `fields` | `Sequence[str]` |  | The stat fields to compute. |
| `adj_values` | `AdjValues` |  | Current `{team_name: field: {"off","def"}}` adjustments. |

**Returns**

`{field: {"avg_opp_def": float, "avg_opp_off": float}}`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_opponent_strengths

team = {"team_name": "A", "opponents": [{"oppo_name": "B", "off_3p_attempts": 10}]}
by_name = {"A": team, "B": {"team_name": "B"}}
adj = {"B": {"3p": {"off": 0.5, "def": 0.3}}}
print(compute_opponent_strengths(team, by_name, ["3p"], adj)["3p"]["avg_opp_def"])  # 0.3
```

### compute_possession_splits {#compute_possession_splits}

`compute_possession_splits(team: 'TeamDetail') -> 'PossessionSplits'`

Home/away/neutral possession totals for a team (`computePossessionSplits`, `ts:154-186`).

Each opponent game's `off_poss`/`def_poss` (missing -> 0) is bucketed
by `location_type` (missing or any non `"Home"`/`"Away"` value ->
the neutral bucket).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team` | `TeamDetail` |  | A `team_details` team dict. |

**Returns**

A `PossessionSplits`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_strength import compute_possession_splits

team = {"opponents": [{"off_poss": 70, "def_poss": 68, "location_type": "Home"}]}
print(compute_possession_splits(team).home_off_poss)  # 70.0
```

### create_lineup_data {#create_lineup_data}

`create_lineup_data(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent', format_version: 'int') -> 'Union[tuple[list[LineupEvent], list[LineupEvent]], list[ParseError]]'`

Combines the different methods to build a set of lineup events

(`PlayByPlayParser.create_lineup_data`, `:153-217`) -- the
orchestrator that chains the ENTIRE Phase 5a-5d surface:

1. `parse_game_events` -- HTML -> reversed
   `~sportsdataverse.mbb.mbb_ncaa_stints.PlayByPlayEvent`\ s.
2. `~sportsdataverse.mbb.mbb_ncaa_stints.build_partial_lineup_list`
   -- events -> chronological lineup stints.
3. `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.fix_possible_score_swap_bug`
   -- undoes a rare NCAA score-transposition bug.
4. `~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.enrich_lineup`
   (mapped over every stint) -- populates `pts`/`plus_minus`/stat
   trees.
5. `~sportsdataverse.mbb.mbb_ncaa_possessions.calculate_possessions`
   -- per-stint possession counts.
6. Zip each stint with its successor (`None` for the last), then
   `~sportsdataverse.mbb.mbb_ncaa_stint_validation.validate_lineup`
   partitions the `(stint, next)` pairs into good (empty error list)
   and bad.
7. `~sportsdataverse.mbb.mbb_ncaa_stint_validation.clump_bad_lineups`
   groups consecutive bad stints, then
   `~sportsdataverse.mbb.mbb_ncaa_stint_validation.analyze_and_fix_clumps`
   tries to self-heal each clump.
8. Concatenate: good stints + every clump's fixed stints -> `good`;
   every clump's still-unfixed stints -> `bad`, each stamped with
   `player_count_error=len(players)` as the VERY LAST step.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw play-by-play-page HTML. |
| `box_lineup` | `LineupEvent` |  | The team's validated box-score lineup (`~sportsdataverse.mbb.mbb_ncaa_boxscore_parser.get_box_lineup`'s result) -- supplies the full roster (for validation), the team/year (for parsing), and the trusted final score (for the swap-bug fix). |
| `format_version` | `int` |  | `0` for the legacy layout, `1` for the 2018+ layout. |

**Returns**

`(good_lineups, bad_lineups)` on success, or a `list[ParseError]` if `parse_game_events` failed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import create_lineup_data

with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
    box_html = f.read()
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
    pbp_html = f.read()
result = create_lineup_data("test.html", pbp_html, box_lineup, format_version=0)

# Pipeline next step (one line)

    good, bad = result
    sum(ev.duration_mins for ev in good + bad)
```

### create_player_events {#create_player_events}

`create_player_events(lineup_event_maybe_bad: 'LineupEvent', box_lineup: 'LineupEvent') -> 'list[PlayerEvent]'`

Split a lineup event into one :class:`~sportsdataverse.mbb

.mbb_ncaa_models.PlayerEvent` per player on the floor
(`create_player_events`, `LineupUtils.scala:1454-1529`).

First re-tidies `lineup_event_maybe_bad`'s `players`/`players_in`/
`players_out` against `box_lineup` (via player_tidier`),
dropping any player who doesn't actually resolve to a box-score player --
this recovers from "impossible" lineups. Then, for each surviving player
(in lineup-slot order, 0-4), builds their own `enrich_stats` call
with a per-player `player_filter_coder` + that player's slot index (the
only caller in this module that ever passes a non-default
`player_index`, wiring increment_player_3p_shot_info`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup_event_maybe_bad` | `LineupEvent` |  | The lineup event to split (its player lists may reference names not actually in `box_lineup`). |
| `box_lineup` | `LineupEvent` |  | The trusted box-score lineup for this game (name resolution + team-scoping context). |

**Returns**

One `~sportsdataverse.mbb.mbb_ncaa_models.PlayerEvent` per (tidied) player in `lineup_event_maybe_bad.players`, same order. Kept even if a player has zero matching raw events -- needed downstream for usage/possession math.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import create_player_events

player_events = create_player_events(lineup, box_lineup)
player_events[0].player_stats.fg_3p.made.total
```

### create_shot_event_data {#create_shot_event_data}

`create_shot_event_data(filename: 'str', in_html: 'str', box_lineup: 'LineupEvent') -> 'Union[list[ShotEvent], list[ParseError]]'`

Parses a game page's SVG shot map into a list of :class:`~sportsdataverse

.mbb.mbb_ncaa_models.ShotEvent` (`ShotEventParser.create_shot_event_data`,
`:175-259`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `filename` | `str` |  | The source file name, used only for error reporting. |
| `in_html` | `str` |  | The raw game-page HTML (containing the SVG shot map, either baked in as `circle.shot` elements or built client-side via an `addShot(...)` JS call -- see `shot_js_to_html`). |
| `box_lineup` | `LineupEvent` |  | The team's box-score lineup event (supplies `team`/ `year`/`location_type` and the tidy-name lookup context). |

**Returns**

Every shot found, sorted chronologically and court-geometry enriched, or a `list[ParseError]` if the HTML couldn't be parsed, the team names couldn't be matched, no shot events were found (even after the JS fallback), or any one circle failed to parse (the first such failure's error(s) only -- Scala's `.sequence` over `List[Either[...]]` is fail-fast, not accumulating).

**Example**

```python
from pathlib import Path
from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
from sportsdataverse.mbb.mbb_ncaa_models import TeamId
from sportsdataverse.mbb.mbb_ncaa_shot_parser import create_shot_event_data

box_html = Path("tests/fixtures/ncaa/test_lineup.html").read_text(encoding="utf-8")
box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=1)
shots = create_shot_event_data("test_p1.html", box_html, box_lineup)
```

### duration_from_period {#duration_from_period}

`duration_from_period(period: 'int', is_women_game: 'bool') -> 'float'`

The game duration (minutes elapsed) once `period` has completed

(`ExtractorUtils.scala:286-287`: `start_time_from_period(period + 1,
...)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `period` | `int` |  | The 1-indexed period number. |
| `is_women_game` | `bool` |  | Whether to use the women's or men's period schedule. |

**Returns**

The game-clock minute at the end of `period`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_stints import duration_from_period
duration_from_period(2, is_women_game=False)  # 40.0 (end of men's regulation)
duration_from_period(4, is_women_game=True)  # 40.0 (end of women's regulation)
```

### enrich_and_reverse_game_events {#enrich_and_reverse_game_events}

`enrich_and_reverse_game_events(in_events: 'list[PlayByPlayEvent]') -> 'list[PlayByPlayEvent]'`

Inserts game-break events and turns descending per-row times into

ascending game-clock minutes, returning the whole list latest-to-earliest
(`PlayByPlayParser.enrich_and_reverse_game_events`, `:297-370`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `in_events` | `list[PlayByPlayEvent]` |  | The raw parsed events, earliest to latest, with each `.min` still a per-period DESCENDING clock reading. |

**Returns**

`in_events` with `~sportsdataverse.mbb.mbb_ncaa_stints .GameBreakEvent`\ s inserted at every period boundary, every `.min` converted to an ASCENDING whole-game reading, and a trailing (once reversed, LEADING) `~sportsdataverse.mbb .mbb_ncaa_stints.GameEndEvent` -- the whole list in LATEST-TO-EARLIEST order (the caller is expected to `reversed(...)` it back when chronological order is wanted, exactly like `get_sorted_pbp_events` does).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import Score
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import enrich_and_reverse_game_events
from sportsdataverse.mbb.mbb_ncaa_stints import OtherTeamEvent

events = [OtherTeamEvent(18.0, Score(1, 1), "tipoff")]
reversed_enriched = enrich_and_reverse_game_events(events)
reversed_enriched[0].__class__.__name__  # 'GameEndEvent'
```

### enrich_lineup {#enrich_lineup}

`enrich_lineup(lineup: 'LineupEvent') -> 'LineupEvent'`

Populate `pts`/`plus_minus` from the score delta, then run the

full stat-tree enrichment (`enrich_lineup`, `LineupUtils.scala:29-46`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineup` | `LineupEvent` |  | The lineup event to enrich (not mutated -- see the module docstring's "Scala idiom decisions"). |

**Returns**

A new `~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` with `team_stats`/`opponent_stats` fully populated.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import enrich_lineup

enriched = enrich_lineup(lineup)
enriched.team_stats.pts
```

### enrich_shot_events_with_pbp {#enrich_shot_events_with_pbp}

`enrich_shot_events_with_pbp(sorted_shot_events: 'list[ShotEvent]', sorted_pbp_events: 'list[PlayByPlayEvent]', lineup_events: 'list[LineupEvent]', bad_lineup_events: 'list[LineupEvent]', box_lineup: 'LineupEvent') -> 'list[ShotEvent]'`

Enrich each shot with its play-by-play event + on-floor lineup

(`PlayByPlayUtils.enrich_shot_events_with_pbp`,
`PlayByPlayUtils.scala:28-278`).

Folds over the (time-sorted) shots, threading two iterators (play-by-play
and lineup) and a small amount of carry-over state. For each shot it:

1. gathers the play-by-play events at the shot's time (`find_pbp_clump`),
   keeping only the ones on the shot's side (team if `is_off`);
2. picks the matching shot event via a strict -> loose -> first-of-N
   cascade (`right_kind_of_shot` then `matching_player`);
3. locates the on-floor lineup (`find_lineup`), falling back to
   `bad_lineup_events` if the good lineups yield nothing (a bad-lineup
   match is used for `players` but its id is suppressed);
4. attributes an assist (a same-time non-self assist event) and transition
   flag (`"fastbreak"` in the event string), and fills in `lineup_id` /
   `players` / `pts` / `value` / `ast_by` / `is_ast` / `is_trans`
   -- exactly the fields Task 5e.5's parser left as placeholders.

Shots with no matching play-by-play clump, no matching shot event, or no
matching lineup are dropped (the Scala logs a `WARN` and discards; the
logging is dropped per the module note, the discard preserved).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sorted_shot_events` | `list[ShotEvent]` |  | Shots in ascending game-clock order. |
| `sorted_pbp_events` | `list[PlayByPlayEvent]` |  | The full play-by-play event stream, ascending. |
| `lineup_events` | `list[LineupEvent]` |  | The good (validation-passing) stint events. |
| `bad_lineup_events` | `list[LineupEvent]` |  | The validation-flagged stint events, used only as a last resort (their ids are never attributed). |
| `box_lineup` | `LineupEvent` |  | The roster lineup event (drives name resolution). |

**Returns**

The enriched, still-time-sorted list of shots (a subset of the input -- unmatchable shots are dropped).

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import enrich_shot_events_with_pbp
enriched = enrich_shot_events_with_pbp(
    shots, pbp, good_lineups, bad_lineups, box_lineup
)
```
