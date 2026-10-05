---
title: "CFB — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 6
description: "CFB — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Other

### espn_cfb_game_rosters {#espn_cfb_game_rosters}

`espn_cfb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_cfb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_cfb_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

**Example**

```python
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
print(rosters.shape)

# Pandas round-trip

rosters_pd = espn_cfb_game_rosters(game_id=401628334, return_as_pandas=True)
rosters_pd.head()

# Pipeline next step (filter to game starters)

import polars as pl
starters = espn_cfb_game_rosters(game_id=401628334).filter(
    pl.col("starter") == True
)
```

### espn_cfb_play_participants {#espn_cfb_play_participants}

`espn_cfb_play_participants(game_id: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, resolve_missing: 'bool' = True, resolve_missing_max: 'int' = 50, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull ESPN per-play participants for a college-football game.

The college-football entry point of the shared
`sportsdataverse.football.play_participants.espn_play_participants`;
see it for the column contract (`{type}_player_name` / `{type}_player_id`
scalars plus the `{type}_player_names` / `{type}_player_ids` lists per
participant type ESPN ships).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `raw` | `bool` | `False` | If True, returns the raw list of play-items dicts. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |
| `resolve_missing` | `bool` | `True` | Fetch athletes the sidecar omits from their `$ref`. |
| `resolve_missing_max` | `int` | `50` | Cap on those per-athlete requests (default 50). |

**Returns**

Polars (or pandas) DataFrame, one row per play; the raw play dicts when `raw=True`.

**Example**

```python
from sportsdataverse.cfb import espn_cfb_play_participants
participants = espn_cfb_play_participants(game_id=401628334)
print(participants.shape)
```

### load_cfb_betting_lines {#load_cfb_betting_lines}

`load_cfb_betting_lines(return_as_pandas=False) -> 'pl.DataFrame'`

Load college football betting lines information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing betting lines available for the available seasons.

| col_name | type | description |
|---|---|---|
| `id` | double | 247Sports referencing id for the recruit. |
| `game_id` | integer | ESPN game identifier. |
| `season` | double | Season (4-digit year). |
| `game_desc` | character | Human-readable description of the game, typically including team names and context. |
| `date_time` | character | Date and time of the game to which the betting line applies, as a string. |
| `market_type` | character | Geographic market type (e.g. `National`). |
| `abbr` | character | Selection/side this odds row applies to — a team abbreviation for spread and moneyline markets, or 'over'/'under' for total markets (the data is long-format, one row per book per selection per market_type). |
| `lines` | double | Numeric line for this row's market — the per-side point spread for spread markets or the over/under total points for total markets; null for moneyline rows. |
| `odds` | integer | American-odds price for this selection — the juice/vig on spread and total rows, or the moneyline price itself on moneyline rows. |
| `opening_lines` | double | Opening numeric line for this row's market (per-side spread or over/under total points) before line movement; null for moneyline rows. |
| `opening_odds` | integer | Opening American-odds price for this selection before line movement (vig on spread/total rows, moneyline price on moneyline rows). |
| `book` | character | Name of the sportsbook or oddsmaker that provided the betting line. |
| `season_type` | character | ESPN season type (2 = regular, 3 = postseason). |
| `week` | integer | Game week of the season. |
| `home_team_id` | integer | ESPN home team id (parsed from `home_team_ref`). |
| `away_team_id` | integer | ESPN away team id (parsed from `away_team_ref`). |

**Example**

```python
from sportsdataverse.cfb import load_cfb_betting_lines
lines = load_cfb_betting_lines()
print(lines.shape)

# Pandas round-trip

lines_pd = load_cfb_betting_lines(return_as_pandas=True)
lines_pd.head()

# Pipeline next step (filter to one provider in 2023)

import polars as pl
consensus_2023 = load_cfb_betting_lines().filter(
    (pl.col("season") == 2023) & (pl.col("provider") == "consensus")
)
```

### load_cfb_rosters_crosswalk {#load_cfb_rosters_crosswalk}

`load_cfb_rosters_crosswalk(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load the current ESPN x Fox CFB rosters crosswalk (single snapshot).

Unlike the per-season `load_cfb_teams_crosswalk` / `load_cfb_schedule_crosswalk`
loaders, this one is **season-less**: ESPN's and Fox's team-roster endpoints
only expose the *current* roster, so the published artifact is a single
snapshot rather than a historical per-season series. It is built by
`cfbfastR-cfb-data`'s `scripts/build_cfb_crosswalk.py` (which fans the
per-team `sportsdataverse.cfb.cfb_rosters_crosswalk` builder out over
the current season's ESPN<->Fox team-id pairs) and refreshed on that repo's
cadence.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

one row per matched player, carrying `espn_team_id` / `fox_team_id` provenance plus each provider's athlete id, name, jersey, position, and the `match_method` / `matched_sources` flags.

| col_name | type | description |
|---|---|---|
| `espn_team_id` | integer | ESPN team id (canonical key). |
| `fox_team_id` | character | Fox Bifrost team id (NA if unmatched). |
| `person_key` | character | Normalized player-name join key: 'Last, First' flipped, lowercased, ASCII-folded, punctuation stripped and runs of initials merged, so 'C.J.' and 'CJ' both give 'cj' (e.g. 'josh brown'). |
| `espn_athlete_id` | integer | ESPN athlete id. |
| `fox_athlete_id` | character | Fox athlete id (NA if unmatched). |
| `yahoo_athlete_id` | character | Present but unpopulated in the published data (all null): the asset is built with providers=('espn', 'fox'), so no Yahoo ids are joined. |
| `name` | character | Position name (e.g. `Quarterback`). |
| `espn_jersey` | character | ESPN jersey number. |
| `fox_jersey` | character | Fox jersey number (NA if unmatched). |
| `espn_position` | character | ESPN position abbreviation. |
| `fox_position` | character | Position abbreviation from the Fox Sports roster (e.g. 'QB', 'OL', 'DB'); null when the player has no Fox roster match or Fox lists no position. |
| `yahoo_position` | character | Present but unpopulated in the published data (all null): the asset is built with providers=('espn', 'fox'), so no Yahoo positions are joined. |
| `match_method` | character | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `matched_sources` | character | Plus-joined provenance tag naming which rosters listed the player: 'espn+fox', 'espn' or 'fox'. The published asset is built without Yahoo, so 'yahoo' never appears. |

**Example**

```python
from sportsdataverse.cfb import load_cfb_rosters_crosswalk
xwalk = load_cfb_rosters_crosswalk()
print(xwalk.shape)

# Pandas round-trip

xwalk_pd = load_cfb_rosters_crosswalk(return_as_pandas=True)

# Pipeline next step (one team's ESPN<->Fox athlete map)

import polars as pl
osu = load_cfb_rosters_crosswalk().filter(pl.col("espn_team_id") == 194)
```

### load_draft_outcomes {#load_draft_outcomes}

`load_draft_outcomes(years: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NFL draft picks with the college of each pick, for the requested draft years.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `years` | `int \| list[int]` |  | A draft year or list of draft years. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per pick: `draft_year` (Int64), `college` (Utf8 PFR-style college name), `player_id` (Utf8 ESPN college athlete id; null for older drafts), `player_name` (Utf8), `round` / `pick` (Int64), `position` (Utf8). Zero-row (typed) when the source is unavailable.

| col_name | type | description |
|---|---|---|
| `draft_year` | integer | NFL draft year of the pick. |
| `college` | character | College of the pick (PFR-style name, e.g. "Ohio St."). |
| `player_id` | character | ESPN college athlete id as a string (null for older drafts). |
| `player_name` | character | Player name as listed on the pick record. |
| `round` | integer | Round of the NFL draft the player was selected in (1-7 in the modern format). |
| `pick` | integer | Overall pick number. |
| `position` | character | Position drafted at (PFR abbreviation). |

**Example**

```python
from sportsdataverse.cfb import load_draft_outcomes
picks = load_draft_outcomes([2023, 2024])
picks.group_by("college").len().sort("len", descending=True).head()
```

### load_fp_curve {#load_fp_curve}

`load_fp_curve() -> 'pl.DataFrame'`

Load the bundled EP-by-yardline curve (no network, no first-use download).

**Returns**

`yardline_own: Int64 (1..99), ep: Float64`.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99). |
| `ep` | double | Bundled expected points for a drive starting at this yard line (2018-2021 fit). |

**Example**

```python
from sportsdataverse.cfb.cfb_field_position import load_fp_curve
curve = load_fp_curve()
```

### load_recruit_classes {#load_recruit_classes}

`load_recruit_classes(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load recruiting classes as per-recruit rows from the 247 RDB feed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single recruiting-class year or a list of them. |
| `division` | `str` | `'fbs'` | Division slug (reserved for constant lookups downstream; the feed itself is queried for all of college football). |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per committed recruit: `season` (Int64), `team_id` (Utf8 — the 247 committed-team key), `team` (Utf8 full name — the downstream name-join key, since the 247 recruit-team key differs from the 247 talent-composite key), `recruit_id` (Utf8), `stars` (Int64), `grade` (Float64 247 composite rating), `position` (Utf8). Zero-row (typed) when no data is available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Recruiting-class year the recruit signed in. |
| `team_id` | character | 247Sports signed-institution team key as a string (falls back to the committed institution when unsigned). |
| `team` | character | Signed-institution full name (falls back to committed) - the downstream name-join key. |
| `recruit_id` | character | 247Sports recruit key as a string (integer-origin). |
| `stars` | integer | 247 composite star rating (1-5; null for unrated recruits). |
| `grade` | double | 247 composite rating on the 0-100 scale. |
| `position` | character | Primary position abbreviation from the 247 recruit record. |

**Example**

```python
from sportsdataverse.cfb.cfb_roster_talent import load_recruit_classes
rec = load_recruit_classes([2022, 2023])
rec.group_by("team").len().sort("len", descending=True).head()
```

### add_era_columns {#add_era_columns}

`add_era_columns(df: 'pl.DataFrame', model: 'str', season: 'int | None' = None) -> 'pl.DataFrame'`

Add the era column(s) `model` consumes, using ITS card's cuts.

The cuts are read from the published contract, never restated here. That is
the fix for cfbfastR-cfb-data#70, where both consumers kept a private copy of
the era boundary, both drifted to a 2017 cut the trainer never used, and
2018-2020 scored an era off the models trained with them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame carrying a `season` column, or any frame when `season` is given explicitly. |
| `model` | `str` |  | Bundle stem, used to look up the era contract. |
| `season` | `int \| None` | `None` | Season to use when `df` has no `season` column -- the hand-built-row case. |

**Returns**

`df` with the contract's columns added. Returned unchanged when the model declares no era contract, or when the columns are already present.

**Example**

```python
from sportsdataverse.cfb.model_calculators import add_era_columns
add_era_columns(pl.DataFrame({"season": [2018]}), "xpass_model")
```

### add_play_type_canonical {#add_play_type_canonical}

`add_play_type_canonical(df: 'pl.DataFrame', *, source: 'str' = 'type.text', with_family: 'bool' = True) -> 'pl.DataFrame'`

Append `play_type_canonical` (and optionally `play_type_family`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | A play-by-play frame. |
| `source` | `str` | `'type.text'` | Name of the raw play-type column. |
| `with_family` | `bool` | `True` | Also append the coarse `play_type_family` column. |

**Returns**

The frame with the canonical column(s) appended. Returned unchanged when `source` is absent, so the helper is safe to apply to frames that have already been projected down.

**Example**

```python
import polars as pl
from sportsdataverse.cfb import add_play_type_canonical

pbp = pl.DataFrame({"type.text": ["Rush", "Pass Reception", "Timeout"]})
out = add_play_type_canonical(pbp)
out.group_by("play_type_family").agg(pl.len())
```

### assert_rating_scale {#assert_rating_scale}

`assert_rating_scale(ratings: 'pl.DataFrame', *, era: 'str' = 'modern', tol: 'float' = 1.6) -> 'float'`

Warn if the ratings have drifted off the scale the constants were fit on.

THE FAILURE THIS PREVENTS. `net_points_scale` is a frozen statement about
a relationship between two things: rating units and points. When the
ratings change -- a different ridge penalty, a rescale, a rebuilt corpus --
the constant silently becomes wrong while every function keeps returning
plausible numbers. That is exactly what happened: the shipped 44.5367 was
fit on 2026-07-28, the ridge lambda moved on 08-01, the corpus was rebuilt
on 08-02, and nothing failed. Measured out-of-sample the result was a
calibration slope of 0.55 -- predictions stretched nearly 2x wider than
reality -- for two days, undetected.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Team ratings frame carrying an `adj_net` column, as returned by `cfb_ratings.efficiency_ratings`. Frames without that column, or with fewer than 30 rows, are too thin to judge and return `1.0` unchecked. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`, used only to name the era in the warning text. |
| `tol` | `float` | `1.6` | Fold-change tolerance. The check fires outside `[1/tol, tol]`. |

**Returns**

The observed/fitted sd ratio. `1.0` when the frame is too thin to judge, so a caller can treat "1.0" as "no evidence of drift" either way.

**Example**

```python
from sportsdataverse.cfb import cfb_ratings
from sportsdataverse.cfb.cfb_game_predict import assert_rating_scale
ratings = cfb_ratings.efficiency_ratings(2024)
ratio = assert_rating_scale(ratings)

# Treat a large drift as a refit signal, not a nuisance warning

assert ratio < 1.6, "refit the constants before trusting predictions"
```

### canonical_play_type_expr {#canonical_play_type_expr}

`canonical_play_type_expr(source: 'str' = 'type.text') -> 'pl.Expr'`

Build the polars expression mapping raw `type.text` to a canonical type.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source` | `str` | `'type.text'` | Name of the raw play-type column. |

**Returns**

A `pl.Expr` aliased `play_type_canonical`. Values absent from `PLAY_TYPE_CANONICAL` (and nulls) yield null, so upstream vocabulary drift surfaces rather than silently creating a category.

**Example**

```python
import polars as pl
from sportsdataverse.cfb import canonical_play_type_expr

pbp = pl.DataFrame({"type.text": ["Pass Reception", "Punt Return"]})
pbp.with_columns(canonical_play_type_expr())
```

### check_box_invariants {#check_box_invariants}

`check_box_invariants(drive_summary: 'dict | None' = None, situational: 'dict | None' = None) -> 'list[str]'`

Every identity the two aggregates must satisfy; violations as strings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drive_summary` | `dict \| None` | `None` | the dict from `create_drive_summary`, or None. |
| `situational` | `dict \| None` | `None` | the dict from `create_situational_stats`, or None. |

**Returns**

one line per violation, `[]` when everything holds.

**Example**

```python
assert check_box_invariants(summary, stats) == []
```

### create_drive_summary {#create_drive_summary}

`create_drive_summary(drives: list[dict] | dict, frame: polars.dataframe.frame.DataFrame, home_id: str | int, away_id: str | int, periods: set[int] | str | None = None) -> dict | None`

Build the StatBroadcast-style drive summary, chart, and long-play lists.

A drive belongs to the quarter it STARTED in. On a windowed build the
full drive sequence still provides context (running score, the previous
drive for OBTAINED and points-off-turnovers), but only in-window drives
are counted, charted, or listed. `largest_lead` and the time-leading /
time-tied split window too: the score state is read from the whole
regulation play sequence and then clipped to the window's clock intervals
(one per contiguous run of quarters, so a gapped set never charges the
quarter it skipped). Under `"ot"` the clock has no axis to integrate over
and only `largest_lead` ships.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drives` | `list[dict]` |  | the ESPN drives grouping, in game order (`previous` plus the in-progress `current` drive, if any). |
| `frame` | `pl.DataFrame` |  | the enriched plays frame from `CFBPlayProcess.run_processing_pipeline` (`plays_frame`). |
| `home_id` | `str \| int` |  | ESPN home team id. |
| `away_id` | `str \| int` |  | ESPN away team id. |
| `periods` | `set[int] \| str \| None` | `None` | optional window -- a set of quarter numbers (e.g. `{1, 2}`) or the string `"ot"` (every period > 4). `None` = full game. |

**Returns**

`{"teams": {...}, "chart": [...], "scores": [...], "longPlays": {...}}` keyed by team id, or `None` when the inputs are unusable (no drives, empty frame, or an empty window).

**Example**

```python
summary = create_drive_summary(drives, game.plays_frame, "52", "61")
```

### create_situational_stats {#create_situational_stats}

`create_situational_stats(frame: polars.dataframe.frame.DataFrame, home_id: str | int, away_id: str | int, window_expr: polars.expr.expr.Expr | None = None) -> dict | None`

Build the situational team-stats block from a plays frame.

`two_minute` and `middle_8` are omitted from a windowed build: both name
a clock window of their own, so intersecting them with another window
describes neither (middle-8 inside Q1 is empty). Every other section,
`pace` and `non_garbage` included, is computed on the windowed slice and
ships with it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `frame` | `pl.DataFrame` |  | the enriched plays frame from `CFBPlayProcess.run_processing_pipeline` (`plays_frame`). |
| `home_id` | `str \| int` |  | ESPN home team id. |
| `away_id` | `str \| int` |  | ESPN away team id. |
| `window_expr` | `Expr \| None` | `None` | optional polars filter expression windowing the windowable sections to that slice (e.g. `pl.col("period") == 3`). `None` = full game. |

**Returns**

`{"teams": {<team_id>: {<section>: ...}}}` or `None` when the frame is unusable or the window is empty.

**Example**

```python
stats = create_situational_stats(game.plays_frame, "52", "61")
```

### efficiency_ratings {#efficiency_ratings}

`efficiency_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted offensive/defensive efficiency.

Fits the offense/defense ridge from `cfb_adjusted_epa` on the
competitive plays in `plays` (`min_competitive_wp <= wp_before <=
max_competitive_wp`), then nets each team's raw per-game EPA (all
pass/rush plays, garbage time included) against the opponent's fitted
strength and averages across games -- the R `adjust_epa` /
gameonpaper `team_agg.R` statistic and scale (a top team nets
~0.30-0.40/play; the pre-2026-07-28 coefficient+intercept scale ran
~1.8x hotter). The ridge's dropped reference team nets normally from
its own games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying every column in `cfb_adjusted_epa._REQUIRED_COLUMNS` (`game_id`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `wp_before`). Callers pass an already as-of-date-filtered frame; this function is pure. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs. Only `ridge_lambda` is consulted here; defaults to `RatingsConfig` when omitted. |

**Returns**

A `polars.DataFrame` with one row per `team_id`: `team_id` (Utf8), `adj_off_epa` / `adj_def_epa` / `adj_net` (Float64), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model consumes). Empty (zero-row, correctly-typed) when `plays` has no competitive plays.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()

# Custom ridge penalty

from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig
ratings = efficiency_ratings(pbp, config=RatingsConfig(ridge_lambda=100.0))
```

### espn_cfb_teams {#espn_cfb_teams}

`espn_cfb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_cfb_teams - look up the college football teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 80 is FBS, 81 is FCS. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.cfb.espn_cfb_teams.clear_cache().

**Example**

```python
from sportsdataverse.cfb import espn_cfb_teams
teams = espn_cfb_teams()
print(teams.shape)

# Pull FCS teams (group 81)

fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
fcs.head()

# Pipeline next step (build an abbreviation lookup)

teams = espn_cfb_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### fei_ratings {#fei_ratings}

`fei_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted per-drive efficiency (FEI-style).

The Fremeau Efficiency Index rates teams on drive value above expectation
given starting field position. The cfbfastR-schema `plays` frame this
package works with carries no starting-field-position column, so this
function uses the documented fallback: per-play EPA summed within each
`(game_id, drive_id)` group stands in for drive value, and that
aggregate is fit through the same opponent-adjustment ridge as
`efficiency_ratings` / `special_teams_ratings` -- no forked
solver. Offline validation against the Fremeau FEI oracle put this
fallback's team ranking at Spearman 0.967.

`cfb_adjusted_epa._prepare` filters to individual pass/rush plays and
is not reused here (drive value should reflect every play on the drive,
special-teams snaps included); the `hfa` treatment is reproduced
directly, matching `special_teams_ratings`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying every column in `cfb_adjusted_epa._REQUIRED_COLUMNS` (`game_id`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `wp_before`) plus `drive_id`. Not pre-aggregated to drives -- this function does that grouping itself. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs. Only `ridge_lambda` is consulted here; defaults to `RatingsConfig` when omitted. |

**Returns**

A `polars.DataFrame` with one row per `team_id` appearing as `pos_team_id` on at least one drive: `team_id` (Utf8), `fei_off` / `fei_def` / `fei_net` (Float64). The ridge's dropped reference team is re-added at the shared intercept (`fei_net == 0.0`). Zero-row (correctly-typed) when `plays` has no rows with a non-null `EPA`.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import fei_ratings
fei = fei_ratings(pbp)
fei.sort("fei_net", descending=True).head()
```

### fit_field_position_ep {#fit_field_position_ep}

`fit_field_position_ep(drives: 'pl.DataFrame', *, start_col: 'str' = 'drive_start_yardline', pts_col: 'str' = 'drive_next_score_pts') -> 'pl.DataFrame'`

Fit the monotone EP-by-starting-yardline curve from a drives frame.

Groups drives by starting yard line (from own goal), takes the mean
next-score points, and applies sample-count-weighted isotonic regression
(weight = number of drives at each starting yard line, non-decreasing),
interpolated onto the full 1..99 grid.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drives` | `DataFrame` |  | one row per drive. |
| `start_col` | `str` | `'drive_start_yardline'` | starting yard line from own goal (1..99). |
| `pts_col` | `str` | `'drive_next_score_pts'` | net next-score points for the drive's offense. |

**Returns**

`yardline_own: Int64 (1..99), ep: Float64` -- monotone non-decreasing. Empty input returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99). |
| `ep` | double | Fitted expected points for a drive starting at this yard line (isotonic, non-decreasing). |

**Example**

```python
import polars as pl
from sportsdataverse.cfb.cfb_field_position import fit_field_position_ep
curve = fit_field_position_ep(drives_frame)
```

### get_2pt_probs {#get_2pt_probs}

`get_2pt_probs(pbp_df: 'Any') -> 'pd.DataFrame'`

Two-point-conversion decision surface (cfb4th `get_2pt_wp`).

Treats each row as "the scoring team just made a touchdown; decide between
the extra point and going for two". Enumerates the three point outcomes
(`0` / `1` / `2`) of the try, scores the opponent's ensuing-drive WP for
each from the scoring team's perspective, and combines them with the
two-point conversion probability (bundled CFB model) and the empirical CFB
extra-point make rate (XP_MAKE_PROB`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Any` |  | Play-by-play frame (polars or pandas) carrying the `start.*` state columns in `sportsdataverse.cfb.cfb_fourth_down._PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` plus: * `two_pt_wp` -- `prob_2pt * wp(pts=2) + (1 - prob_2pt) * wp(pts=0)`. * `xp_wp` -- `prob_xp * wp(pts=1) + (1 - prob_xp) * wp(pts=0)` with `prob_xp = _XP_MAKE_PROB`. * `prob_2pt` -- the bundled-model two-point conversion probability. * `two_pt_recommendation` -- `"go_for_2"` iff `two_pt_wp > xp_wp` else `"kick_xp"` (None where the inputs are NaN). * `two_pt_wp_diff` -- `two_pt_wp - xp_wp` (positive => go for 2). When the two-point model isn't bundled (`TWO_PT_MODEL_AVAILABLE` is False) or the required state columns are missing, all decision columns are null -- probabilities are never fabricated.

**Example**

```python
from sportsdataverse.cfb.cfb_two_point import get_2pt_probs
out = get_2pt_probs(touchdown_rows)
print(out[["two_pt_wp", "xp_wp", "two_pt_recommendation"]].head())
```

### get_4th_down_probs {#get_4th_down_probs}

`get_4th_down_probs(pbp_df) -> 'pd.DataFrame'`

Full 4th-down decision surface (cfb4th `add_4th_probs`) + recommendation.

Runs `get_go_wp`, `get_fg_wp`, `get_punt_wp` on the
fourth-down rows and adds the combined option columns plus:

* `fourth_down_recommendation` -- the max-WP choice among `{go, punt,
  field_goal}` (NaN options are excluded; when the FG model isn't bundled,
  `field_goal` is excluded from the comparison).
* `go_wp_diff` / `punt_wp_diff` / `fg_wp_diff` -- each option's WP minus
  the recommended option's WP (the recommended option's diff is 0, the others
  <= 0). NaN where the option WP is NaN.
* `go_boost` -- cfb4th's headline number: `100 * (go_wp - max(fg_wp,
  punt_wp))` in percentage points.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the `start.*` state columns in PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` with the decision columns added. Empty input returns the input plus empty decision columns.

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_4th_down_probs

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_4th_down_probs(fourth_down_rows)
print(out[["go_wp", "punt_wp", "fg_wp", "fourth_down_recommendation"]].head())
```

### get_fg_wp {#get_fg_wp}

`get_fg_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of attempting a field goal (cfb4th `get_fg_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `fg_make_prob`, `make_fg_wp`, `miss_fg_wp` and `fg_wp` (= make_prob*make_wp + (1-make_prob)*miss_wp, from the kicking team's perspective). All four are NaN when the FG model is not bundled (`FG_MODEL_AVAILABLE` is False) -- probabilities are never fabricated.

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_fg_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_fg_wp(fourth_down_rows)
print(out[["fg_make_prob", "fg_wp"]].head())
```

### get_go_wp {#get_go_wp}

`get_go_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of going for it on 4th down (cfb4th `get_go_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the `start.*` state columns in PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` plus `go_wp` (prob-weighted WP of going for it), `first_down_prob` (P(conversion)), `wp_succeed` (mean WP over conversion outcomes) and `wp_fail` (mean WP over failure outcomes). `go_wp` is always in [0, 1]; the conditional columns are in [0, 1] but can be NaN for degenerate goal-line plays where one outcome bucket is empty (matches the R reference `pivot_wider` NA behavior).

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_go_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_go_wp(fourth_down_rows)
print(out[["go_wp", "first_down_prob"]].head())
```

### get_punt_wp {#get_punt_wp}

`get_punt_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of punting on 4th down (cfb4th `get_punt_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `punt_wp` (prob-weighted WP of punting, from the punting team's perspective). `punt_wp` is NaN where the punt end-yardline distribution has no support for the play's `yards_to_goal` (e.g. inside the 31, where punting is dominated and the cfb4th table is empty -- matching the R reference's left-join NA behavior).

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_punt_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_punt_wp(fourth_down_rows)
print(out[["punt_wp"]].head())
```

### make_ratings_compute_results {#make_ratings_compute_results}

`make_ratings_compute_results(ratings: 'pl.DataFrame', *, era: 'str' = 'modern') -> 'ComputeResultsFn'`

Build a `cfb_simulations` `compute_results` closure from fixed ratings.

The returned closure implements the engine's results contract -- `(teams, games,
week_num, *, rng, **kwargs) -> {"teams", "games"}` -- filling every unplayed
`week == week_num` game's `result` with a sampled home margin
`round(Normal(exp_margin, margin_sd))`, where `exp_margin` is
`cfb_game_predict.predict_margin` on the two teams' `adj_net` (home-field
applied unless `neutral`). Unlike the default elo sampler the ratings are
**fixed**, so `teams` passes through unchanged (no elo update). Postseason games
(`game_type != "REG"`) re-break a sampled tie by win probability.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | A `cfb_ratings.cfb_ratings`-style frame with `team_id` and `adj_net`. Teams absent from it are treated as league-average (0.0). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |

**Returns**

A `compute_results` callable suitable for `cfb_simulations(..., compute_results=...)`.

**Example**

```python
import numpy as np, polars as pl
from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results
cr = make_ratings_compute_results(pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.3, -0.3]}))
teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
games = pl.DataFrame({"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"],
                      "neutral": [0], "result": [None]})
cr(teams, games, 1, rng=np.random.default_rng(0))["games"]
```

### normalize_pbp_columns {#normalize_pbp_columns}

`normalize_pbp_columns(df: 'pl.DataFrame', model: 'str') -> 'pl.DataFrame'`

Add card-named copies of any play-by-play columns `df` already carries.

A hand-built frame using the card's own names passes through untouched; a
pbp frame gains the names the card asks for. Copies rather than renames, so
nothing the caller passed in is removed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Caller's frame. |
| `model` | `str` |  | Bundle stem, used to look up which features are wanted. |

**Returns**

`df` plus any alias columns that could be resolved.

**Example**

```python
normalize_pbp_columns(pbp, "xpass_model")
```

### on3_industry_player_rankings {#on3_industry_player_rankings}

`on3_industry_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison player rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per recruit (consensus On3/Rivals/247/ESPN). Zero-row frame on empty.

**Example**

```python
from sportsdataverse.cfb import on3_players_industry_comparision  # forward RDB native
df = on3_players_industry_comparision(sport_key=1, year=2026)
print(df.shape)
```

### on3_industry_team_rankings {#on3_industry_team_rankings}

`on3_industry_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison team rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per team class (consensus ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_consensus_team_rankings  # forward RDB native
df = on3_team_ranking_consensus_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```

### on3_player_rankings {#on3_player_rankings}

`on3_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 player rankings for a class year (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per ranked recruit (On3 ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_person_sport_rankings  # forward RDB native
df = on3_person_sport_rankings(sport_key=1, year=2026)
print(df.shape)
```

### on3_team_rankings {#on3_team_rankings}

`on3_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 team recruiting-class rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per team class (On3 ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_team_rankings  # forward RDB native
df = on3_team_ranking_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```

### play_type_family_expr {#play_type_family_expr}

`play_type_family_expr(source: 'str' = 'play_type_canonical') -> 'pl.Expr'`

Build the polars expression mapping a canonical type to its phase family.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source` | `str` | `'play_type_canonical'` | Name of the canonical play-type column. |

**Returns**

A `pl.Expr` aliased `play_type_family`; unmapped values yield null.

**Example**

```python
import polars as pl
from sportsdataverse.cfb import add_play_type_canonical

pbp = pl.DataFrame({"type.text": ["Rush", "Timeout"]})
add_play_type_canonical(pbp).filter(
    pl.col("play_type_family") != "administrative"
)
```

### predict_from_card {#predict_from_card}

`predict_from_card(df: 'pl.DataFrame', model: 'str', booster: 'Any') -> 'np.ndarray'`

Score `df` with `booster`, validated and ordered by the model's card.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame carrying at least the model's declared features. Extra columns are ignored, so a full pbp frame passes through unchanged. |
| `model` | `str` |  | Bundle stem, used to look up the card and to name the model in any error. |
| `booster` | `Any` |  | The loaded `xgboost.Booster`. |

**Returns**

The booster's raw predictions.

**Example**

```python
from sportsdataverse.cfb.model_calculators import predict_from_card
predict_from_card(pbp, "xpass_model", booster)
```

### predict_margin {#predict_margin}

`predict_margin(home_adj_net: 'float', away_adj_net: 'float', neutral: 'bool', *, era: 'str' = 'modern', games_played: 'float | None' = None) -> 'float'`

Expected home scoring margin from the two net ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_net` | `float` |  | Home team's opponent-adjusted net rating (`adj_net` from `cfb_ratings.efficiency_ratings`). |
| `away_adj_net` | `float` |  | Away team's opponent-adjusted net rating. |
| `neutral` | `bool` |  | Whether the game is at a neutral site (no home-field advantage). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying the fitted slope, `hfa_points` and the attenuation curve. |
| `games_played` | `float \| None` | `None` | Games behind the WEAKER of the two as-of ratings. Supplying it selects the games-played slope (see `slope_for_games`) and is worth ~0.6 MAE; omitting it falls back to the flat `net_points_scale`, which is the average over the curve. |

**Returns**

The expected margin (home minus away), in points: `slope * (home_adj_net - away_adj_net) + hfa_points` on a home field, or without the HFA term on a neutral one. HFA is added in POINTS, not routed through the slope. The previous form multiplied an EPA-scale `2 * hfa_epa` by `net_points_scale`, which tied the two together and let them drift apart unnoticed -- the shipped pair implied ~1.65 points against a measured ~3.0.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import predict_margin
predict_margin(0.30, 0.10, neutral=False)

# With games-played, which selects the attenuation-corrected slope

predict_margin(0.30, 0.10, neutral=False, games_played=9)
```

### predict_total {#predict_total}

`predict_total(home_adj_off: 'float', home_adj_def: 'float', away_adj_off: 'float', away_adj_def: 'float', game_pace: 'float', *, era: 'str' = 'modern') -> 'float'`

Expected combined point total from the four efficiency ratings + tempo.

Fitted linear model `total_intercept + total_scale * sum4 + total_pace_scale *
game_pace`, where `sum4 = home_adj_off + away_adj_def + away_adj_off +
home_adj_def`. The four ratings are summed because each side's scoring rises
with its own offense and with the opponent's EPA-*allowed* (`adj_def` is
lower = better defense). `game_pace` (the matchup's expected scrimmage plays,
`home_off_pace * away_off_pace / league_avg_pace`) enters because a total is a
*sum* -- tempo scales both sides' points the same way, so it compounds into the
total (whereas in the margin, a differential, pace cancels). All three
coefficients are fitted on 2023 actual totals.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_off` | `float` |  | Home offense adjusted EPA/play (`adj_off_epa`). |
| `home_adj_def` | `float` |  | Home defense adjusted EPA/play allowed (`adj_def_epa`). |
| `away_adj_off` | `float` |  | Away offense adjusted EPA/play. |
| `away_adj_def` | `float` |  | Away defense adjusted EPA/play allowed. |
| `game_pace` | `float` |  | Expected scrimmage plays for the matchup, i.e. `home_off_pace * away_off_pace / league_avg_pace` from the ratings' `off_pace` column (`cfb_predict_games` computes this for you). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying the fitted `total_intercept` / `total_scale` / `total_pace_scale`. |

**Returns**

The expected combined total points.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import predict_total
predict_total(0.20, -0.05, 0.10, 0.02, game_pace=66.0)
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Internal helper that flattens an ESPN scoreboard event dict into a shape

suitable for `pd.json_normalize`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard `events[*]` entry from the ESPN college-football scoreboard API. |

**Returns**

The same event dict, mutated in place with `home`/`away` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.cfb import espn_cfb_schedule
sched = espn_cfb_schedule(dates=2023, week=5)
```

### slope_for_games {#slope_for_games}

`slope_for_games(games_played: 'float | None', *, era: 'str' = 'modern') -> 'float'`

Points per unit of rating differential, given how many games back it.

A single slope is wrong. An as-of rating built on two games is a far
noisier predictor than one built on twelve, and OLS slopes attenuate
toward zero as predictor noise grows -- so the correct multiplier is
smaller early and grows through the season. Measured, walk-forward on
2014-2025:

    0-3 games -> 10.62      6-7 games -> 42.00
    4-5 games -> 26.06      8+  games -> 54.49

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games_played` | `float \| None` |  | Games behind the as-of rating. When two ratings back a prediction this should be the WEAKER (smaller) of the two, since the noisier rating binds the attenuation. `None` selects the flat `net_points_scale`. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |

**Returns**

The points-per-rating-unit slope for that bucket, or the flat `net_points_scale` when `games_played` is `None` or falls outside every bucket. The flat value is the average over the curve, so it is a safe default rather than a silent zero.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import slope_for_games
slope_for_games(2)      # early season -- heavily attenuated
slope_for_games(11)     # late season -- near the full slope

# Unknown game count falls back to the flat scale

slope_for_games(None)
```

### special_teams_ratings {#special_teams_ratings}

`special_teams_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: a per-unit special-teams EPA composite.

Special teams was empirically found NOT to obey the offense-minus-defense
symmetry `efficiency_ratings` / `fei_ratings` rely on, and not
to benefit from opponent adjustment, when validated against the 2023 SP+
special-teams oracle (`tests/fixtures/cfb_prediction/sp_plus_2023.parquet`
`sp_special`):

* The executing `pos_team` owns the EPA on a kickoff / punt / field
  goal. The `def_pos_team` "coverage" side reflects the opposing
  returner's skill, not the coverage team's, and is not recoverable from
  EPA -- adding any coverage unit *lowers* SP+ agreement (0.77 -> 0.58),
  so coverage/defense units are excluded entirely (see the module's
  special-teams unit patterns).
* The opponent-adjustment ridge (`cfb_adjusted_epa._fit_opponent_ridge`)
  *hurts* agreement (0.72 vs 0.77) -- special teams is only weakly
  opponent-dependent, so this function does not fit a ridge at all.
* Splitting the offense-side plays into per-phase units (field goal, punt,
  kick return) is what helps. Each unit's per-team mean EPA/play is
  centered on that unit's league-wide per-play mean and the three
  centered deviations are summed -- true EPA units. This centered form
  reached Spearman 0.865 against SP+ special teams, beating both the
  originally-shipped z-scored composite (0.768 -- dimensionless, std
  ~1.7, range +-5 under an epa` column name; replaced 2026-07-28)
  and a single-unit offense-minus-intercept ridge fit (0.703).

`adj_st_epa` is therefore the sum, over the three special-teams units
(field goal, punt, kick return), of each unit's per-team mean EPA/play
above the unit's league average. A team with no plays in a given unit
contributes 0 for that unit (not a penalty). `config` is accepted for
signature parity with
`efficiency_ratings` / `fei_ratings` but is unused -- there is
no ridge (and therefore no `ridge_lambda`) in this recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying `game_id`, `pos_team_id`, `EPA`, and `play_type`. Not pre-filtered to special-teams plays -- this function does that filtering itself. |
| `config` | `RatingsConfig \| None` | `None` | Unused (kept for signature parity across the three rating functions). See the note above. |

**Returns**

A `polars.DataFrame` with one row per `team_id` appearing anywhere in `plays`: `team_id` (Utf8), `adj_st_epa` (Float64, the sum of per-unit executing-team mean EPA/play above each unit's league average). Teams with no special-teams plays get `adj_st_epa == 0.0`. Zero-row (correctly-typed) when `plays` has no special-teams plays.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import special_teams_ratings
st = special_teams_ratings(pbp)
st.sort("adj_st_epa", descending=True).head()
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, era: 'str' = 'modern') -> 'float'`

Home win probability from an expected margin via the Gaussian CDF.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home margin in points (e.g. from `predict_margin`). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying `margin_sd`. |

**Returns**

`Phi(exp_margin / margin_sd)` -- the probability the home team wins under a `Normal(exp_margin, margin_sd**2)` margin model. `0.5` at a zero expected margin.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import win_prob_from_margin
win_prob_from_margin(7.0)
```
