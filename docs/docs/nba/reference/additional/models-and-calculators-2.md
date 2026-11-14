---
title: "NBA — additional Python functions — Models and calculators: game_page–nba_matchup"
sidebar_label: "Models and calculators: game_page–nba_matchup"
sidebar_position: 12
description: "NBA — additional Python functions — Models and calculators: game_page–nba_matchup — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: game_page–nba_matchup

### game_page_tables {#game_page_tables}

`game_page_tables(pbp: 'pl.DataFrame', team_box: 'pl.DataFrame', player_box: 'pl.DataFrame', *, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None, neutral_site: 'Optional[bool]' = False, rated_home_prob: 'Optional[float]' = None, shot_cells: 'Optional[pl.DataFrame]' = None) -> 'dict[str, object]'`

Every game-page table for one game, from its basketball pbp / team box / player box frames.

The warehouse path: the sdv-db nightly stage reads one game's rows from `<league>.pbp` /
`team_box` / `player_box` and its schedule row (`season`, `format_regulation_periods`,
`neutral_site`) and calls this. `game_page_frames` builds the same frames live from ESPN
and calls this too, so a game renders identically from either source. Runs
`score_win_prob`, `shot_coordinates`, `team_game_rates`,
`player_game_rates`, `~sportsdataverse.nba.nba_game_splits.team_game_splits`,
`~sportsdataverse.nba.nba_game_splits.player_game_splits` and
`~sportsdataverse.nba.nba_game_expected.team_game_expected` on the game's own regulation.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game's plays in the basketball pbp schema, in play order (empty before tip-off). |
| `team_box` | `DataFrame` |  | The game's team box rows (rates need exactly two). |
| `player_box` | `DataFrame` |  | The game's player box rows. |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods; default `game_regulation_periods` from the frame's `season`. Pass the schedule's `format_regulation_periods` through `game_regulation_periods` when you have it. |
| `neutral_site` | `Optional[bool]` | `False` | Whether the game was at a neutral site (no home-court advantage in a lineless prior); `None`, NaN or `pd.NA` counts as false. |
| `rated_home_prob` | `Optional[float]` | `None` | The game's rated pregame home win probability, the prior when there is no line (NBA / WNBA: `~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`). |
| `shot_cells` | `Optional[DataFrame]` | `None` | The league's shot counts as of the game date (`~sportsdataverse.nba.nba_game_expected.shot_cells`), for the expected points of `team_game_expected`; `None` leaves its expected columns null. |

**Returns**

`{"pbp", "team_game_rates", "player_game_rates", "team_game_splits", "player_game_splits", "team_game_expected", "periods", "regulation_periods", "period_kind", "prior"}`: the scored plays with shot coordinates, the two rate frames (empty with their documented columns without two team rows), the two split frames (counted from the plays so far; the team frame is empty before tip-off, the player frame then holds the boxed players with zero counts), the expected points by zone and situation (empty before tip-off), the highest period played (the regulation count before tip-off), the regulation periods, `"half"` or `"quarter"`, and `{"home_win_prob", "source", "home_team_spread", "game_spread_available"}`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the league is unknown, `league` contradicts `league_id`, or `pbp` holds more than one game. |
| `polars.exceptions.ColumnNotFoundError` | If a frame lacks a column the derivations need. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import game_page_tables, game_regulation_periods
from sportsdataverse.wbb.wbb_loaders import load_wbb_pbp, load_wbb_player_boxscore, load_wbb_team_boxscore
gid = 293170052
pbp = load_wbb_pbp([2010]).filter(pl.col("game_id") == gid)
tb = load_wbb_team_boxscore([2010]).filter(pl.col("game_id") == gid)
pb = load_wbb_player_boxscore([2010]).filter(pl.col("game_id") == gid)
tables = game_page_tables(pbp, tb, pb, league="wbb", regulation_periods=game_regulation_periods("wbb", season=2010))

# The win-probability chart

tables["pbp"].select("game_play_number", "home_win_prob", "wp_delta", "wp_regime")
```

### game_regulation_periods {#game_regulation_periods}

`game_regulation_periods(league: 'str', *, season: 'int | None' = None, format_regulation_periods: 'float | None' = None) -> 'int'`

Regulation periods of one game: 2 (halves) or 4 (quarters).

The game's own value wins: ESPN's summary `format.regulation.periods` on the live path, the
schedule's `format_regulation_periods` in the warehouse. Without it (null, NaN, or anything
other than 2 or 4) the season decides: the NBA always plays quarters, men's college basketball
always halves, the WNBA halves before 2006 and women's college basketball halves before the
2015-16 season (`season` 2016, ESPN's end year). With neither, WNBA and WBB default to quarters.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"`; `None` means NBA. |
| `season` | `int \| None` | `None` | ESPN `season.year` of the game (the end year for college); null or NaN counts as unknown. |
| `format_regulation_periods` | `float \| None` | `None` | The game's own regulation period count, if known. |

**Returns**

`2` or `4`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is a string other than the four. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import game_regulation_periods
game_regulation_periods("wbb", season=2015)  # 2: halves before 2015-16
game_regulation_periods("wnba", season=2024, format_regulation_periods=4.0)  # 4

# A warehouse schedule row

game_regulation_periods("mbb", season=2024, format_regulation_periods=2.0)  # 2
```

### get_constants {#get_constants}

`get_constants(league_id: 'str') -> 'LeagueConstants'`

Return the `LeagueConstants` for a `league_id`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `str` |  | stats.nba.com league id -- `"00"` NBA, `"10"` WNBA, `"20"` G-League. |

**Returns**

The league's `LeagueConstants`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not a known key of `LEAGUE_CONSTANTS`. |

**Example**

```python
from sportsdataverse.nba.nba_prediction_constants import get_constants
get_constants("00").hfa
```

### get_shrinkage_k {#get_shrinkage_k}

`get_shrinkage_k(league_id: 'str') -> 'float'`

Shooter-talent shrinkage `k` for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `str` |  | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |

**Returns**

The pseudo-attempt shrinkage constant. `"00"` / `"20"` are fitted split-half (`tools/fit_nba_shot_shrinkage_k.py`); `"10"` is a seeded placeholder, not a fit.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | Unknown `league_id`. |

**Example**

```python
from sportsdataverse.nba.nba_shot_value_constants import get_shrinkage_k
get_shrinkage_k("00")
```

### home_prior {#home_prior}

`home_prior(home_team_spread: 'Optional[float]', game_spread_available: 'Optional[bool]', *, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, neutral_site: 'Optional[bool]' = False, rated_home_prob: 'Optional[float]' = None) -> 'tuple[float, str]'`

Pregame home win probability for the in-game model, and where it came from.

`home_team_spread` is the signed expected home margin the basketball pbp frames carry
(positive when the home side is favoured). With no line, every basketball pickcenter helper leaves
it `None` (older releases carry an invented `+2.5`), so the value is only trusted when
`game_spread_available` is true. A line
already prices the venue. Without one the prior is `rated_home_prob` when given (the NBA / WNBA
as-of team-ratings prior, `~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`, which prices
the venue itself), else home-court advantage alone, and at a neutral site (common in college
tournaments) there is none: the prior is 0.5. NBA and WNBA use
`~sportsdataverse.nba.nba_game_predict.win_prob_from_margin` and their fitted `hfa`;
MBB and WBB use `~sportsdataverse.mbb.mbb_game_predict.win_prob_from_margin` with
`league="mens"` / `"womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_team_spread` | `Optional[float]` |  | Signed expected home margin, or `None`. |
| `game_spread_available` | `Optional[bool]` |  | Whether a real line backed that spread. |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `neutral_site` | `Optional[bool]` | `False` | ESPN `neutralSite` / the schedule's `neutral_site`; `None`, NaN or `pd.NA` counts as false. |
| `rated_home_prob` | `Optional[float]` | `None` | The game's rated pregame home win probability, used when there is no line; `None`, NaN or a value outside [0, 1] falls through to home-court advantage. |

**Returns**

`(probability, source)` with `source` `"spread"`, `"rated"`, `"neutral"` or `"hfa"`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the league is unknown or `league` contradicts `league_id`. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import home_prior
home_prior(3.0, True)      # (0.58, 'spread')
home_prior(2.5, False)     # (0.56, 'hfa')  the default line is ignored
home_prior(None, False, rated_home_prob=0.71)  # (0.71, 'rated')

# A lineless college tournament game

home_prior(None, None, league="mbb", neutral_site=True)  # (0.5, 'neutral')
```

### in_game_features {#in_game_features}

`in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'`

Per-play in-game win-probability features from a `load_nba_pbp` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame with `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id` (event team) and `home_team_id` (the `load_nba_pbp` schema). |
| `pregame_home_prob` | `float` |  | The pregame home win probability (e.g. from `win_prob_from_margin`), encoded as a constant logit column. Clipped to `[1e-6, 1 - 1e-6]` so a saturated CDF (exact 0/1) cannot crash the logit. |

**Returns**

One row per input play: `score_diff` (home - away), `sec_left` (clipped at 0 -- overtime plays count as 0 seconds left), `sqrt_sec_left`, `pregame_logit`, `home_has_ball` (`Int8`; dead-ball / unknown-team plays are 0), `margin_per_sqrt_sec` (`score_diff / sqrt(sec_left + 1)`: the lead measured against the time left, so a fixed lead grows as the clock runs down) and `sqrt_sec_left_60` (`sqrt(max(sec_left, 60))`: the clock with the final minute folded into `margin_per_sqrt_sec`).

| col_name | type | description |
|---|---|---|
| `score_diff` | Float64 |  |
| `sec_left` | Float64 |  |
| `sqrt_sec_left` | Float64 |  |
| `pregame_logit` | Float64 |  |
| `home_has_ball` | Int64 |  |
| `margin_per_sqrt_sec` | Float64 | Score margin over the time left, score_diff / sqrt(sec_left + 1): a fixed lead grows as the clock runs down. The basketball in-game boosters read the final minute only through this column. |
| `sqrt_sec_left_60` | Float64 | Square root of the seconds left, floored at one minute (sqrt(max(sec_left, 60))): the clock the basketball in-game boosters read outside the final minute. |

**Example**

```python
from sportsdataverse.nba.nba_game_predict import in_game_features
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
feats = in_game_features(pbp, 0.62)
```

### league_pbp_functions {#league_pbp_functions}

`league_pbp_functions(league: 'str') -> 'tuple[Callable[..., dict], Callable[[Any, dict], dict]]'`

The ESPN fetch and parse pair for one league: `(espn_<league>_pbp, helper_<league>_pbp)`.

A live processor fetches once (`fetch(game_id, raw=True)`, the summary with its keys
defaulted and nothing parsed), checks the payload has plays, then parses (`parse(game_id,
summary)`) and hands the result to `game_page_frames`. Splitting the two keeps an
upstream failure (fetch) apart from a parsing bug (parse).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"`. |

**Returns**

`(fetch, parse)`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not one of the four. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import game_page_frames, league_pbp_functions
fetch, parse = league_pbp_functions("wnba")
summary = fetch(401726992, raw=True)
frames = game_page_frames(parse(401726992, summary), league="wnba")
```

### luck_adjusted_response {#luck_adjusted_response}

`luck_adjusted_response(possessions: 'pl.DataFrame', shooting: 'pl.DataFrame', player_rates: 'Optional[dict[int, tuple[float, float]]]' = None, *, fg3_k: 'float' = 100.0, ft_k: 'float' = 50.0) -> 'pl.DataFrame'`

Attach a per-possession `la_points` expected-points response.

**DECISION 2/4**: `la_points = 2*fg2m + 3*Σ_shooter fg3a·p̂3 + Σ_shooter fta·p̂ft`
(offense-only, "one_way"). 2-pt makes stay realized. `p̂` come from
`player_rates` when given, else `_shrunk_shooter_rates` on `shooting`.

**Defense-shooter exclusion (bugfix)**: `shooting`
(`~sportsdataverse.nba.nba_possessions.build_possession_shooting`)
deliberately retains defense-team shooters in a possession group — e.g. a
defensive technical free throw shooter — because it is a per-shooter
companion frame, not a team-attributed one (that's why it carries its own
`team_id` column). The expected-points sum is offense-only by
definition (**DECISION 2**), so *before* aggregating `exp_extra` this
function joins `possessions[["game_id", "possession_number",
"offense_team_id"]]` onto `shooting` and filters to
`team_id == offense_team_id`, dropping any defense-team shooter row.
Without this filter a defense tech-FT's `fta·p̂ft` term leaks into the
offense's `la_points`, inflating it (reproduced: 2.9 vs. the
offense-only-correct 2.0 for a single offense 2-pt make plus one defense
tech FT).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame carrying team-level `fg2m` and the join keys `game_id` + `possession_number` + `offense_team_id`. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame (`build_possession_shooting`), which may include defense-team shooter rows (e.g. technical FTs) — filtered out here before aggregation. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override (e.g. planted truth in tests); `None` → shrink from `shooting`. |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to `_shrunk_shooter_rates` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to `_shrunk_shooter_rates` when `player_rates` is `None`. |

**Returns**

`possessions` with an added `la_points: Float64` column (same rows, same order). Empty `possessions` → returned unchanged with an empty `la_points` column.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 |  |
| `start_order_index` | Int64 |  |
| `end_order_index` | Int64 |  |
| `start_seconds_remaining` | Float64 |  |
| `end_seconds_remaining` | Float64 |  |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean |  |
| `number_in_period` | Int64 |  |
| `possession_start_type` | String |  |
| `count_as_possession` | Boolean |  |
| `fg2a` | Int64 |  |
| `fg2m` | Int64 |  |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `off_player_1` | Int64 |  |
| `off_player_2` | Int64 |  |
| `off_player_3` | Int64 |  |
| `off_player_4` | Int64 |  |
| `off_player_5` | Int64 |  |
| `def_player_1` | Int64 |  |
| `def_player_2` | Int64 |  |
| `def_player_3` | Int64 |  |
| `def_player_4` | Int64 |  |
| `def_player_5` | Int64 |  |
| `lineup_source` | String |  |
| `la_points` | Float64 |  |

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import luck_adjusted_response
out = luck_adjusted_response(possessions_df, shooting_df)
print(out["la_points"].mean())

# Planted-truth override for testing

out = luck_adjusted_response(possessions_df, shooting_df, {7: (0.4, 0.8)})
```

### nba_adj_rapm {#nba_adj_rapm}

`nba_adj_rapm(possessions: 'pl.DataFrame', prior: 'Dict[int, Tuple[float, float]]', *, league_id: 'str | None' = None, alpha: 'float | None' = None, alphas: 'np.ndarray | None' = None, prior_scale: 'float | None' = None, n_samples: 'int' = 200, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

One-shot prior-informed RAPM over a possession frame -> per-player ratings.

Builds the sparse design matrix via
`~sportsdataverse.nba.nba_rapm.build_rapm_design`, constructs the
per-possession `prior_mean` vector from `prior`, fits a residualized
ridge with an RTO posterior via `_fit_prior_ridge`, and returns the
per-player offensive, defensive, and combined adj-RAPM ratings alongside
possession counts.

Sign convention (matches `~sportsdataverse.nba.nba_rapm.nba_rapm`):
`d_adj_rapm` is positive for a good defender (lowers opponent points);
`adj_rapm = o_adj_rapm + d_adj_rapm` exactly.

Centring: the ridge fits an unpenalized intercept (the league's points per possession), as
`~sportsdataverse.nba.nba_rapm.nba_rapm` does, so `o_adj_rapm` and `d_adj_rapm` are
each relative to an average player: their possession-weighted means sit near 0 (2025-26 NBA
+0.32 / +0.06, 2025 WNBA +0.45 / +0.11, against o_rapm's +0.41 / +0.11). Fit without one,
as every release fit before #1137 was, the penalty split that level between every offensive and defensive
coefficient: o / d read about +12 / -11 (2025-26 NBA +12.03 / -10.84), a thinly-used
player kept less of the offset (-8.9 on offense under 200 possessions), and so `adj_rapm`
itself leaned toward heavy-minute players.

Penalty and prior scale: one per league, `ADJ_RAPM_PENALTY` and
`ADJ_RAPM_PRIOR_SCALE` (fitted by `tools/fit_adj_rapm_penalty.py`; the constants'
comment carries the evidence), as `~sportsdataverse.nba.nba_rapm.nba_rapm` takes
`~sportsdataverse.nba.nba_rapm.RAPM_PENALTY`. Before #1150 each fit took RidgeCV's
per-season pick at the unscaled prior, which collapsed the spread in seven published WNBA
seasons (2004: sd 0.64 against plain RAPM's 1.47) and held NBA 2025-26 at 0.77 of plain RAPM's.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | A possession+lineup frame produced by the possession engine (`game_id`, `offense_team_id`, `points`, `off_player_1..5`, `def_player_1..5`). |
| `prior` | `Dict[int, Tuple[float, float]]` |  | Per-player `{player_id: (o_prior, d_prior)}` in per-100 units. Players absent from `prior` receive a `(0.0, 0.0)` default. |
| `league_id` | `str \| None` | `None` | `"00"`, `"10"` or `"20"`: the league whose penalty and prior scale are used. `None` reads it from the `game_id` prefix (NBA without one). |
| `alpha` | `float \| None` | `None` | A penalty instead of the league's (positive; `ValueError` otherwise). |
| `alphas` | `ndarray \| None` | `None` | Deprecated: pick the penalty per fit by RidgeCV over this grid (the old rule, whose scale is not comparable across seasons). |
| `prior_scale` | `float \| None` | `None` | A multiplier on `prior` instead of the league's. |
| `n_samples` | `int` | `200` | Number of RTO posterior samples (default 200). |
| `seed` | `int` | `0` | RNG seed for the RTO sampler (default 0). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with columns `player_id` (Int64), `o_adj_rapm` (Float64), `d_adj_rapm` (Float64), `adj_rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `o_adj_rapm` | Float64 |  |
| `d_adj_rapm` | Float64 |  |
| `adj_rapm` | Float64 |  |
| `off_poss` | Int64 |  |
| `def_poss` | Int64 |  |

**Example**

```python
from sportsdataverse.nba import nba_adj_rapm
ratings = nba_adj_rapm(possessions, spm_prior_dict)
print(ratings.sort("adj_rapm", descending=True).head())
```

### nba_aging_curve {#nba_aging_curve}

`nba_aging_curve(*, league: 'str' = 'nba', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Load the bundled per-age value-multiplier curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `age:Int64, rel_value:Float64, peak_age:Float64` (`peak_age` repeated on every row for convenient filtering/joining).

| col_name | type | description |
|---|---|---|
| `age` | Int64 | Player age (in years). |
| `rel_value` | Float64 | Value multiplier for this age relative to the peak age: a delta-method curve chaining minutes-weighted within-player consecutive-age changes in per-100-possession box-score value, quadratic-smoothed and min-max scaled to [0.4, 1.0], so the peak age is exactly 1.0 and the lowest-valued age 0.4. |
| `peak_age` | Float64 | Age at which rel_value reaches its maximum of 1.0, repeated on every row for filtering and joining (29.0 in the bundled curve). |

**Example**

```python
from sportsdataverse.nba import nba_aging_curve
curve = nba_aging_curve()
print(curve.sort("rel_value", descending=True).head(1))

# Pipeline next step (one line)

curve.filter(pl.col("age").is_between(24, 30))
```

### nba_bpm {#nba_bpm}

`nba_bpm(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', positions: 'pl.DataFrame', *, team_adjust: 'bool' = True, granularity: 'str' = 'season', league_ortg: 'float | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Faithful BPM 2.0 per player, at season or single-game granularity.

Team adjustment (Myers, "About Box Plus/Minus (BPM)", BPM 2.0, Basketball-Reference): a
team's %Min-weighted ratings (five slots) sum to its rating, so its minute-weighted mean
`bpm` is its net rating per 100 / 5, and its minute-weighted `obpm` is (its offensive
rating - the league's) / 5, a separate constant. `dbpm = bpm - obpm` then averages (the
league's offensive rating - the team's defensive rating) / 5, positive for a good defense, so
league minute-weighted obpm and dbpm each sit near 0 (2025-26
NBA -0.001 / -0.006, 2025 WNBA +0.005 / +0.013). Team ratings are raw per-100 box margins, not
the schedule-adjusted ones Basketball-Reference uses. A traded player's minutes count with
the first team he appears for in the logs. Before #1148 every player took `net - team mean`
on both bpm and obpm: a team-mean target of the full net rating, five times BPM 2.0's, and
all of the adjustment on offense, so dbpm stayed raw (2025-26 regular season, minute-weighted:
league obpm -2.94 / dbpm +2.90, 97% of players with dbpm > 0; Michael Porter
Jr. -8.97 / +2.26, now +2.22 / -0.95). On 2024-25 the fix lifts agreement with
Basketball-Reference's BPM from corr 0.708 / MAE 3.83 to 0.945 / 0.65 (500+ minutes).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | per-player-per-game box lines (`nba_box_logs`'s `player`); must carry `game_id` when `granularity="game"`. |
| `team_logs` | `DataFrame` |  | per-team-per-game lines incl. `plus_minus` (`nba_box_logs`'s `team`); must carry `game_id` when `granularity="game"`. |
| `positions` | `DataFrame` |  | listed positions (`nba_player_positions`): player_id, position_num. |
| `team_adjust` | `bool` | `True` | apply the team adjustment (True) or return raw box-BPM (False). |
| `granularity` | `str` | `'season'` | `"season"` (default) aggregates every row in `player_logs`/ `team_logs` into one row per player. `"game"` runs the exact same pipeline independently per `game_id` (position/role are estimated game-native, mirroring `NbaBpmModel`'s existing fold-native design) and returns one row per (game_id, player_id) with a leading `game_id` column; `gp` is always 1 in this mode. |
| `league_ortg` | `float \| None` | `None` | The league offensive rating (points per 100) the OBPM adjustment is relative to. `None` (default) takes it from `team_logs`: per `season_id` when that column is there (stats.nba.com's season_id also encodes the season type, so regular season and playoffs stay apart), else over all of `team_logs`, which must then be one season and season type. Pass it when `team_logs` is not a whole league, for example one game scored on its own. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`"season"`: frame with `player_id`, `obpm`, `dbpm`, `bpm`, `min`, `gp` (Int64 player_id/gp, Float64 obpm/dbpm/bpm/min). `"game"`: the same columns prefixed with `game_id` (Utf8), one row per (game_id, player_id). Empty (that schema) input -> zero-row frame with the same schema; never raises on empty.

No returns table is published for this function: no capture: its positions input is nba_player_positions output, which reads stats.nba.com (HTTP 403 to the datacenter IP the docs are built on).

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `granularity` is not `"season"`/`"game"`, or if `granularity="game"` is requested but `player_logs` has no `game_id` column. |

**Example**

```python
from sportsdataverse.nba import nba_bpm, nba_box_logs, nba_player_positions
logs = nba_box_logs("2023-24"); pos = nba_player_positions("2023-24")
bpm = nba_bpm(logs["player"], logs["team"], pos)
print(bpm.sort("bpm", descending=True).head())

# Per-game BPM

bpm_game = nba_bpm(logs["player"], logs["team"], pos, granularity="game")
print(bpm_game.filter(pl.col("game_id") == "0022300001").sort("bpm", descending=True))

# Raw (no team adjustment)

bpm_raw = nba_bpm(logs["player"], logs["team"], pos, team_adjust=False)

# Pandas output

bpm_pd = nba_bpm(logs["player"], logs["team"], pos, return_as_pandas=True)
```

### nba_career_trajectory {#nba_career_trajectory}

`nba_career_trajectory(player_values: 'pl.DataFrame', *, league: 'str' = 'nba', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Age-adjust player-season values with the bundled aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_values` | `DataFrame` |  | Frame `player_id, age:Int64, value:Float64`. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`player_values` plus `age_adjusted_value` (`value / rel_value(age)`, peak-centered) and `proj_next_value` (`value * rel_value(age+1) / rel_value(age)`). Ages outside the bundled curve's range fall back to `rel_value = 1.0` (no adjustment). Empty input returns the zero-row schema.

No returns table is published for this function: no capture: its input is a user-built player-age panel (player_id, age, value) that no package function produces.

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_career_trajectory
player_values = pl.DataFrame({"player_id": ["1"], "age": [24], "value": [10.0]})
nba_career_trajectory(player_values)
```

### nba_current_impact {#nba_current_impact}

`nba_current_impact(possessions: 'pl.DataFrame', priors: 'pl.DataFrame', as_of: 'dt.date | str | Sequence[dt.date | str]', *, params: 'CurrentImpactParams | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Current impact: each player's expected offense / defense impact per 100 as of a date.

The box prior in force on the date plus the Kalman-filtered deviation from it (see the module
docstring), from games dated strictly before the date only: possessions dated on or after the latest
`as_of` are dropped before the filter runs, and so are priors dated after it (a prior dated on a date
is built from games before it and is in force on it).

Measured by `tools/fit_current_impact.py` on NBA regular seasons 2014-2026 (the SDs fitted on
2007-2013), weekly dates, each scored on the possessions of the next 28 days: it beats the season-to-date
rating (`~sportsdataverse.nba.nba_reml_rapm.nba_reml_rapm`, fixed penalty, on the season's games
before the date at the BB-D38 in-season box prior) by 5.53 bp of MSE (season-cluster bootstrap SE 0.35),
in all 13 seasons: 13.2 bp in a season's first four weeks, 3.3 bp from its thirteenth. Against the
strongest season-to-date variant (the window prior, penalty tuned on 2007-2013) it is still 1.91 bp better
(SE 0.20, 13 of 13), so the carried deviation adds beyond the prior. The window prior alone is 5.10 bp
worse than the filter. The fitted team-change SD and drift barely move the ratings
(without either the gate error changes by under 0.03 bp; without drift it is 0.03 bp lower).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup rows with `game_id`, `game_date`, `season`, `points`, `offense_team_id`, `defense_team_id`, `off_player_1..5`, `def_player_1..5`, from the first season the filter should learn from (regular season: the fit used `002` games). |
| `priors` | `DataFrame` |  | `CURRENT_IMPACT_PRIOR_SCHEMA` rows, e.g. one `current_impact_prior` per week; a game uses the latest `prior_date` on or before its date. |
| `as_of` | `date \| str \| Sequence[date \| str]` |  | One date or several (one pass of the filter serves all of them). |
| `params` | `CurrentImpactParams \| None` | `None` | The shock and decay SDs (default the fitted constants). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`CURRENT_IMPACT_SCHEMA`, one row per date and player seen before it: `o_current_impact` / `d_current_impact` / `current_impact` (per 100, `d` positive = good defense, `current_impact = o + d`), the prior in force (`o_prior` / `d_prior`), the season-to-date possessions (`off_poss` / `def_poss`, 0 for a player who has not played this season) and the player's last game date, and the prior weights `o_prior_weight` / `d_prior_weight` / `prior_weight` (the share of each estimate that is the box prior, 0-1; module docstring). `season` is that of the last game before the date.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | NBA stats person id. |
| `as_of` | Date | The rating date: only games dated strictly before it are used. |
| `season` | Int64 | Season (end year) of the last game before as_of. |
| `o_current_impact` | Float64 | Expected offensive impact per 100 on as_of: o_prior plus the Kalman-filtered offensive deviation from it. |
| `d_current_impact` | Float64 | Expected defensive impact per 100 on as_of (positive = good defense): d_prior plus the filtered defensive deviation. |
| `current_impact` | Float64 | o_current_impact + d_current_impact. |
| `o_prior` | Float64 | The offensive box prior in force on as_of (the latest prior_date on or before it), per 100. |
| `d_prior` | Float64 | The defensive box prior in force on as_of, per 100 (positive = good defense). |
| `off_poss` | Int64 | Offensive possessions this season before as_of (0 for a player carried from an earlier season who has not played in this one). |
| `def_poss` | Int64 | Defensive possessions this season before as_of. |
| `last_game_date` | Date | Date of the player's last game before as_of in the input. |
| `o_prior_weight` | Float64 | Share of o_current_impact that is the box prior (0-1): the offensive deviation's posterior variance over its variance with no game updates. 1 before his first game; falls as his games (this season's and carried earlier ones) accumulate. |
| `d_prior_weight` | Float64 | Share of d_current_impact that is the box prior (0-1), computed the same way for the defensive deviation. |
| `prior_weight` | Float64 | Share of current_impact that is the box prior (0-1): the net (offense + defense) deviation's posterior variance over its no-update variance. |

**Example**

```python
from sportsdataverse.nba import nba_current_impact
cur = nba_current_impact(possessions, priors, ["2025-01-15", "2025-03-01"])
print(cur.filter(pl.col("as_of") == pl.date(2025, 3, 1)).sort("current_impact", descending=True).head())
```

### nba_darko {#nba_darko}

`nba_darko(panel: 'pl.DataFrame', ages: 'pl.DataFrame', *, aging_curve: "'AgingCurve | None'" = None, process_var: "'float | None'" = None, obs_base: "'float | None'" = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Project each player's next-season rating via a per-player Kalman filter + aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | `player_id, season, rating` (+ optional `weight`) — a multi-season rating panel. |
| `ages` | `DataFrame` |  | `player_id, season, age` (from `nba_player_ages`). |
| `aging_curve` | `AgingCurve \| None` | `None` | an `AgingCurve`; fitted from `panel` if None. |
| `process_var` | `float \| None` | `None` | Kalman process variance `q`; MLE-fit from `panel` if None. |
| `obs_base` | `float \| None` | `None` | Kalman base observation variance; MLE-fit from `panel` if None. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`player_id, last_season, forecast_season, filtered_skill, projected_rating, projected_sd`.

No returns table is published for this function: no capture: its input is a multi-season player rating panel with ages per season that no package function produces.

**Example**

```python
from sportsdataverse.nba import nba_darko, nba_player_ages
proj = nba_darko(rating_panel, ages_panel)
print(proj.sort("projected_rating", descending=True).head())
```

### nba_decay_rapm {#nba_decay_rapm}

`nba_decay_rapm(possessions: 'pl.DataFrame', *, asof: 'Optional[datetime.date]' = None, half_life_days: 'float' = 180.0, alphas: 'Optional[np.ndarray]' = None, league_id: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Time-decay RAPM: ridge weighted by `0.5 ** (days_ago / half_life_days)`.

`asof=None` disables decay: every possession is weighted `1.0` and the
fit uses **exactly** plain `~sportsdataverse.nba.nba_rapm.nba_rapm`'s
own penalty (`~sportsdataverse.nba.nba_rapm.rapm_penalty`, the league's
fixed value) so the two agree byte-for-byte (see
`test_decay_rapm_asof_none_equals_plain_rapm`). When `asof` is set,
possessions dated after `asof` are dropped, the remainder is
exponentially down-weighted by age, and the fit switches to the
**oracle** regularization schedule (`oracle_rapm_alphas` evaluated
at the post-filter possession count, `cv=` `ORACLE_RAPM_CV`) per
the binding WP2 ridge-schedule ruling documented in the module docstring.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Multi-season possession+lineup frame. Must carry a `game_date` (`pl.Date`) column when `asof` is not `None`. |
| `asof` | `Optional[date]` | `None` | Reference date; `None` -> unweighted, plain-RAPM-equivalent fit. |
| `half_life_days` | `float` | `180.0` | Weight half-life in days (default 180). |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects plain RAPM's league penalty (`~sportsdataverse.nba.nba_rapm.rapm_penalty`) when `asof is None` or `oracle_rapm_alphas` (evaluated at the possession count) when `asof` is set. |
| `league_id` | `Optional[str]` | `None` | `"00"` / `"10"` / `"20"`, the league whose plain-RAPM penalty the `asof=None` branch fits at; `None` reads it from the `game_id` prefix (`~sportsdataverse.nba.nba_rapm.rapm_penalty`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `DECAY_RAPM_SCHEMA`. Empty input, or an `asof` that drops every possession, -> zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `o_decay_rapm` | Float64 |  |
| `d_decay_rapm` | Float64 |  |
| `decay_rapm` | Float64 |  |
| `off_poss` | Int64 |  |
| `def_poss` | Int64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `asof` is not `None` but *possessions* lacks a `game_date` column. |

**Example**

```python
import datetime
from sportsdataverse.nba.nba_rapm_variants import nba_decay_rapm

df = nba_decay_rapm(season_poss, asof=datetime.date(2024, 3, 1), half_life_days=120.0)
print(df.sort("decay_rapm", descending=True).head())

# Plain-RAPM-equivalent (no decay)

df = nba_decay_rapm(season_poss)  # asof=None
```

### nba_draft_model {#nba_draft_model}

`nba_draft_model(draft_year: "'Union[int, list[int]]'", *, league: 'str' = 'nba', college_prior: "'Optional[pl.DataFrame]'" = None, gleague_bridge: 'bool' = False, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Project prospect career value + draft probability from combine measurements.

Loads the draft-combine wrappers for `draft_year` (or each year in the
list), builds the shared combine-feature vector
(`sportsdataverse.nba.nba_draft_constants.build_combine_features`),
and applies the bundled ridge (`proj_career_value`) / logistic
(`draft_prob`) heads fit in `tools/models/nba_draft/fit_draft_model.py`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `Union[int, list[int]]` |  | A draft year (e.g. `2019`) or list of years. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"` -- selects the bundled artifact and the combine-wrapper family. |
| `college_prior` | `Optional[DataFrame]` | `None` | Optional frame keyed on `player_id:Utf8` carrying the college-side MBB/WBB player-value spine's `projected_pick` / `box_bpm` / `archetype` (model ⑤, see design doc §3.5). When present and the bundled artifact has matching feature columns, it is left-joined as an extra feature block. This function **never** imports `sportsdataverse.mbb` -- callers pass the frame in. |
| `gleague_bridge` | `bool` | `False` | When `True`, left-joins G-League (`league_id="20"`) bulk production (`gleague_pts`/`gleague_gp`/`gleague_min`) for the draft year's season as extra, forward-looking feature columns. Not part of any bundled artifact's scored features today (joining it never changes `proj_career_value`/`draft_prob`); gracefully absent when the G-League bulk call returns no rows -- never raises. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_career_value:Float64, draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8` — one row per prospect with combine measurements for that class. `projected_pick` is a contiguous 1..N rank within each draft year. Empty/malformed input returns the zero-row schema, never raises.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_draft_model
board = nba_draft_model(2019)
print(board.sort("proj_career_value", descending=True).head())

# With a college-side prior

board = nba_draft_model(2019, college_prior=mbb_prior_df)

# Pipeline next step (one line)

board.filter(pl.col("pro_tier") == "lottery")
```

### nba_expected_turnovers {#nba_expected_turnovers}

`nba_expected_turnovers(season: 'str', *, league_id: 'str' = '00', base: 'Optional[pl.DataFrame]' = None, player_mix: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Expected TOV + residual ball-security skill from Synergy play-type mix.

`lg_to_rate_t` = poss-weighted league mean of `turnover_freq` for play
type `t`; `expected_tov = scale · Σ_t poss_t · lg_to_rate_t`;
`ball_security_skill = 100·(expected_tov − tov)/poss` (fewer turnovers
than expected ⇒ positive skill -- sign flipped vs. the foul-drawing model).

`scale = Σ actual tov / Σ raw type-mix estimate` is derived from the
fetched season itself (covers players below Synergy's per-type
classification threshold) so `Σ expected_tov ≡ Σ tov` holds exactly.
Swapping each player's own `turnover_freq` in for the league rate
reconstructs their real season TOV almost exactly (slope ~1.03, Spearman
~0.97 on the 2023-24 oracle corpus) -- confirming the column semantics are
correct. The *expected* (league-rate) version necessarily explains less
variance than *actual* (turnover-avoidance is a more individual,
less play-type-bound skill than foul-drawing), so its calibration slope
runs lower than model (3)'s -- see the oracle gate for the observed floor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base` measure) frame: `player_id`, `tov`, `poss` (bypasses the live fetch). |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix: `player_id`, `play_type`, `poss`, `turnover_freq`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `poss`/`tov`/ `expected_tov`/`ball_security_skill` (Float64). Zero-row frame with this schema when the inputs are empty (sparse-coverage leagues never raise).

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_expected_turnovers
t = nba_expected_turnovers("2023-24")
print(t.sort("ball_security_skill", descending=True).head(10))

# Injected offline (oracle / test) path

t = nba_expected_turnovers("2023-24", base=base_df, player_mix=mix_df)

# Pipeline next step

t.filter(pl.col("poss") >= 200).sort("ball_security_skill", descending=True)
```

### nba_four_factor_rapm {#nba_four_factor_rapm}

`nba_four_factor_rapm(possessions: 'pl.DataFrame', *, alphas: 'Optional[np.ndarray]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Four-factor RAPM: four independent ridge fits (efg/ftr/orbd/tov) on the SAME design.

Each factor is regressed on the identical offense/defense design matrix,
differing only in the per-possession response (`_FACTOR_RESPONSES`).
Output mirrors the oracle's `RA_*__Off/__Def` layout. **DECISION 5/6/7**
govern the response definitions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame carrying team-level `fg2m, fg3m, ftm, oreb, tov` and the ten lineup columns. Must also carry a `points` column -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally) requires it unconditionally even though none of the four factor responses use it. `offense_team_id` is NOT required here (unlike `nba_la_rapm`): none of the four factor responses need the offense-only shooter join. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override, shared by all four factor fits. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `FOUR_FACTOR_SCHEMA` — `{factor}__off` / `{factor}__def` columns per factor, plus possession counts. Empty input → zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `efg__off` | Float64 |  |
| `efg__def` | Float64 |  |
| `ftr__off` | Float64 |  |
| `ftr__def` | Float64 |  |
| `orbd__off` | Float64 |  |
| `orbd__def` | Float64 |  |
| `tov__off` | Float64 |  |
| `tov__def` | Float64 |  |
| `off_poss` | Int64 |  |
| `def_poss` | Int64 |  |

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_four_factor_rapm
ff = nba_four_factor_rapm(season_poss)
print(ff.sort("efg__off", descending=True).head())
```

### nba_in_game_win_prob {#nba_in_game_win_prob}

`nba_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-play home win probability from the bundled in-game model.

Scores `in_game_features` through the committed artifact
(`sportsdataverse/nba/models/nba_in_game_wp.ubj` for NBA, `wnba_in_game_wp.ubj`
for WNBA): a shallow monotone-constrained xgboost booster fit by
`tools/models/nba_prediction/train_in_game_wp.py` (NBA on 2022, 2023 and 2025,
held out on 2026; WNBA on 2021-2024, held out on 2025). Within the final minute
the clock reaches the model only through `margin_per_sqrt_sec`, so at a fixed
lead the probability moves toward the leader as time runs out. This is the raw
model output: it never applies the game result. `nba_game_page.score_win_prob` sets the
last play of a finished game to the result, and that settle step is small now but
not always zero (the WNBA booster saturates near 0.99 for a lead of 5 or more).

Gate note: the plan's concurrent oracle (stats.nba.com
`winprobabilitypbp` HOME_PCT) is a dead endpoint, so this model is
validated ONLY on realized-outcome calibration, not against a native WP
feed. See the fixtures README + SDD ledger.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play for ONE game in the `load_nba_pbp` schema (`start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`). |
| `pregame_home_prob` | `float` |  | Pregame home win probability (e.g. from `win_prob_from_margin`). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the `in_game_features` columns plus `home_win_prob`.

| col_name | type | description |
|---|---|---|
| `score_diff` | Float64 |  |
| `sec_left` | Float64 |  |
| `sqrt_sec_left` | Float64 |  |
| `pregame_logit` | Float64 |  |
| `home_has_ball` | Int64 |  |
| `margin_per_sqrt_sec` | Float64 | Score margin over the time left, score_diff / sqrt(sec_left + 1): a fixed lead grows as the clock runs down. The basketball in-game boosters read the final minute only through this column. |
| `sqrt_sec_left_60` | Float64 | Square root of the seconds left, floored at one minute (sqrt(max(sec_left, 60))): the clock the basketball in-game boosters read outside the final minute. |
| `home_win_prob` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | If the bundled artifact is an xgboost `.ubj` and `xgboost` is not installed. |

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
wp = nba_in_game_win_prob(pbp, 0.62)
```

### nba_la_rapm {#nba_la_rapm}

`nba_la_rapm(possessions: 'pl.DataFrame', shooting: 'pl.DataFrame', player_rates: 'Optional[dict[int, tuple[float, float]]]' = None, *, alphas: 'Optional[np.ndarray]' = None, fg3_k: 'float' = 100.0, ft_k: 'float' = 50.0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Luck-adjusted RAPM: ridge on an expected-points response (high-variance shooting regressed).

Replaces realized 3-point and free-throw outcomes with the shooter's shrunk
expected value (`luck_adjusted_response`); 2-pt makes stay realized.
**DECISION 2/3/4** govern the response recipe and shrinkage constants.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame with team-level `fg2m` and the ten lineup columns; join keys `game_id` + `possession_number` + `offense_team_id` (the last is required by `luck_adjusted_response`'s defense-shooter leak filter). Must also carry a `points` column even though the LA response (`la_points`) supersedes it for fitting -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally via `_prepare`) requires it unconditionally. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame from `build_possession_shooting`. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override; `None` → shrink from `shooting`. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule (`cv=` `ORACLE_RAPM_CV` always; there is no plain-schedule mode). |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `LA_RAPM_SCHEMA`. Empty input → zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `o_la_rapm` | Float64 |  |
| `d_la_rapm` | Float64 |  |
| `la_rapm` | Float64 |  |
| `off_poss` | Int64 |  |
| `def_poss` | Int64 |  |

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_la_rapm
df = nba_la_rapm(season_poss, season_shooting)
print(df.sort("la_rapm", descending=True).head())

# Planted-truth shooter rates (e.g. for testing)

df = nba_la_rapm(season_poss, season_shooting, {7: (0.4, 0.8)})
```

### nba_matchup_drapm {#nba_matchup_drapm}

`nba_matchup_drapm(season: 'str', *, league_id: 'str' = '00', matchups: 'Optional[pl.DataFrame]' = None, config: 'Optional[PlaytypeConfig]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Matchup-based defensive RAPM (offense-quality-controlled).

Fits `points_allowed_per_100 ~ defender_FE + offense_FE` via
`~sklearn.linear_model.RidgeCV` (weighted by matchup possessions),
reusing the shipped RAPM ridge machinery on the
`build_matchup_drapm_design` two-way-FE design.

**Sign + scale:** the design target `y` is already points-allowed *per 100*
matchup possessions (`100 * player_pts / partial_poss`), so the defender
coefficient is already on the per-100 scale -- `matchup_drapm =
-(beta_defender - mean_beta_defender)` (centered, NO extra ×100, unlike
`~sportsdataverse.nba.nba_rapm.nba_rapm` whose `y` is per-*possession*
and needs the ×100). Sign is negated so higher = better defense (fewer points
allowed), matching the `d_rapm` convention. Typical magnitudes are a few to
low-double-digit points per 100 vs the league defender average.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `matchups` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leagueseasonmatchups`-shaped frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `config` | `Optional[PlaytypeConfig]` | `None` | `~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig`; defaults to a fresh instance (`ridge_alphas` = the shared RAPM grid, `min_matchup_poss` = 25.0 inclusion floor). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per defender: `player_id` (Int64), `matchup_drapm` (Float64, points-allowed-per-100 estimate, higher = better defense), `matchup_poss` (Float64, total matchup possessions guarded). Returns a zero-row frame with this schema when the upstream fetch/injection is empty or no row survives the `min_matchup_poss` floor (sparse-coverage leagues never raise).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `matchup_drapm` | Float64 |  |
| `matchup_poss` | Float64 |  |

**Example**

```python
from sportsdataverse.nba import nba_matchup_drapm
d = nba_matchup_drapm("2023-24")
print(d.sort("matchup_drapm", descending=True).head(10))

# Injected offline (oracle / test) path

d = nba_matchup_drapm("2023-24", matchups=matchups_df)

# Pipeline next step

d.filter(pl.col("matchup_poss") >= 200).sort("matchup_drapm", descending=True)
```
