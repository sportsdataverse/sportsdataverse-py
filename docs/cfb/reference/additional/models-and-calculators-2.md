# CFB — additional Python functions — Models and calculators: cfb_compute–fit_field

> CFB — additional Python functions — Models and calculators: cfb_compute–fit_field — function reference in sdv-py, the SportsDataverse Python package.

### cfb_compute_results {#cfb_compute_results}

`cfb_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'int', *, rng: 'Optional[np.random.Generator]' = None, elo: 'Optional[Dict[str, float]]' = None, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame]'`

Default results generator — nflseedR's dynamic ELO model for CFB.

Fills `result` for week `week_num` games that are still unplayed and
updates each team's ELO rating from that week's results (real results
included). Constants are nflseedR's `nflseedR_compute_results` exactly,
minus the NFL rest-day adjustment (CFB plays weekly — documented
simplification).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `DataFrame` |  | Per-sim team table (`sim`, `team`, `conference`, optionally `elo` carried over from the previous week). |
| `games` | `DataFrame` |  | Per-sim games table (engine schema; see `sportsdataverse.cfb.cfb_standings`). |
| `week_num` | `int` |  | The week to fill. |
| `rng` | `Optional[Generator]` | `None` | numpy Generator (seeded by `cfb_simulations`). A fresh default generator is created when omitted. |
| `elo` | `Optional[Dict[str, float]]` | `None` | Optional initial ratings `{team: elo}` applied to every sim. Teams missing from the dict start at 1500. When neither `elo` nor a `teams.elo` column exists, ratings initialize randomly at `N(1500, 150)` per (sim, team) — nflseedR behavior. |

**Returns**

`{"teams": ..., "games": ...}` — updated frames, mirroring nflseedR's returned list.

| col_name | type | description |
|---|---|---|
| `sim` | integer | Simulation identifier the game row belongs to (1..n simulated seasons; ELO ratings never mix across simulations). |
| `week` | integer | Week of the season the game is played in; only games matching the requested week_num are filled. |
| `game_type` | character | Game classification in the seedr engine schema - REG (regular season), CONF_CHAMP (conference championship) or POST (postseason/CFP). |
| `home_team` | character | Team name of the home team in the simulated game (returned games frame). |
| `away_team` | character | Team name of the away team in the simulated game (returned games frame). |
| `result` | double | Home-team margin of victory (home score minus away score) - real results are preserved and the target week's unplayed games are filled from the ELO model. |
| `neutral` | integer | Neutral-site flag (1 = neutral site, 0 = true home game; only non-neutral games receive the ELO home bump). |

**Example**

```python
from sportsdataverse.cfb.cfb_simulations import cfb_compute_results
out = cfb_compute_results(teams, games, 5, rng=rng)
teams, games = out["teams"], out["games"]
```

### cfb_draft_projection {#cfb_draft_projection}

`cfb_draft_projection(target_draft_year: 'int', *, division: 'str' = 'fbs', history_years: 'list[int] | None' = None, l2: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame]'`

Project NFL-draft probability per player + expected picks per team.

Fits an L2 logistic of `drafted` on `[recruit_stars, talent_points,
career_production_z, class_year]` over draft years strictly before the
target (the as-of boundary, enforced internally), then scores the target
year's eligible players.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_draft_year` | `int` |  | Draft year to project. |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `history_years` | `list[int] \| None` | `None` | Training draft years (default: the five before target). |
| `l2` | `float` | `1.0` | Logistic L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, both frames return as pandas. |

**Returns**

`{"players": ..., "teams": ...}` — players: `draft_year` (Int64), `team_id` / `player_id` / `player_name` (Utf8), `draft_prob` (Float64); teams: `draft_year`, `team_id`, `proj_draft_picks` (Float64, the sum of member draft probabilities). Zero-row (typed) frames when no data is available.

**Example**

```python
from sportsdataverse.cfb import cfb_draft_projection
out = cfb_draft_projection(2024)
out["teams"].sort("proj_draft_picks", descending=True).head(10)
```

### cfb_field_position {#cfb_field_position}

`cfb_field_position(seasons: 'Union[int, list[int]]', *, exclude_garbage: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team-season field-position value: avg start, drive EP, margin, pts/drive.

Derives one row per drive from `load_cfb_pbp`, values each starting
yard line with the bundled EP curve, and aggregates per (season, team):
`avg_start_yardline` (yards from own goal, higher = better),
`fp_ep` (mean drive-start EP), `fp_margin` (own `fp_ep` minus the
mean drive-start EP of opponents' drives faced), and
`points_per_drive` (mean realized offensive points: TD=7, FG=3;
non-offensive negative results such as safeties and defensive return
TDs are floored to 0 before averaging).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | season or list of seasons (hosted pbp covers 2002-2021). |
| `exclude_garbage` | `bool` | `True` | drop drives that start in Connelly garbage time. |
| `return_as_pandas` | `bool` | `False` | return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (season, team_id); zero-row frame with the documented schema on empty input.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the field-position stats cover. |
| `team_id` | character | Team ESPN id (character join key). |
| `drives` | integer | Offensive drives counted (garbage-time drives excluded by default). |
| `avg_start_yardline` | double | Mean drive-start yard line from the team's own goal (higher = better field position). |
| `fp_ep` | double | Mean bundled expected points of the team's drive starts. |
| `fp_margin` | double | Own fp_ep minus the mean drive-start EP of opponents' drives faced. |
| `points_per_drive` | double | Mean realized offensive points per drive (TD=7, FG=3). |

**Example**

```python
from sportsdataverse.cfb import cfb_field_position
df = cfb_field_position([2021])
print(df.shape)

# Pipeline next step (one line)

df.sort("fp_margin", descending=True).head()
```

### cfb_predict_games {#cfb_predict_games}

`cfb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Predict a whole schedule of games from a ratings frame (vectorized).

Applies the three closed-form predictors across every row of `games` in
one pass. `ratings` is joined twice -- once on `home_team_id` and once on
`away_team_id` -- so each game carries both teams' `adj_net` / `adj_off_epa`
/ `adj_def_epa` / `off_pace`. The totals model's `game_pace` factor is
computed here as `home_off_pace * away_off_pace / league_avg_pace`, where the
league average is the mean `off_pace` of the passed ratings frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Schedule frame with `game_id`, `home_team_id`, `away_team_id`, and `neutral_site` columns. The two team-id columns must share the dtype of `ratings["team_id"]` (asserted before the join). |
| `ratings` | `DataFrame` |  | A `cfb_ratings.cfb_ratings`-style frame with `team_id`, `adj_net`, `adj_off_epa`, `adj_def_epa`, and `off_pace`. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per game with `game_id`, `home_team_id`, `away_team_id`, `neutral_site`, `exp_margin`, `home_win_prob`, `exp_total`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game identifier carried through from the input schedule. |
| `home_team_id` | character | Home team ESPN id (character; the ratings `team_id` join key). |
| `away_team_id` | character | Away team ESPN id (character; the ratings `team_id` join key). |
| `neutral_site` | logical | Whether the game is at a neutral site (home-field advantage is dropped when true). |
| `exp_margin` | double | Expected home scoring margin in points (net_points_scale * net rating differential + the ridge-native home-field advantage on non-neutral fields). |
| `home_win_prob` | double | Home win probability, Phi(exp_margin / margin_sd) under a Gaussian margin model. |
| `exp_total` | double | Expected combined point total from the fitted efficiency + pace totals model. |

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import cfb_predict_games
from sportsdataverse.cfb import cfb_ratings
from sportsdataverse.cfb.cfb_schedule import cfb_schedule  # schedule loader
ratings = cfb_ratings(2023)
preds = cfb_predict_games(schedule_2023, ratings)
```

### cfb_ratings {#cfb_ratings}

`cfb_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, fbs_only: 'bool' = True, drop_kneels: 'bool' = True, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

One row per team: the full CFB ratings spine (off/def/ST EPA + FEI).

Public orchestrator over `efficiency_ratings`,
`special_teams_ratings`, and `fei_ratings`. Loads play-by-play
+ schedule via `sportsdataverse.cfb.cfb_loaders.load_cfb_pbp` /
`sportsdataverse.cfb.cfb_loaders.load_cfb_schedule`, joins the
schedule's per-game date onto the plays, optionally applies the
as-of-date leakage boundary
(`sportsdataverse.cfb.cfb_prediction_constants.as_of_ratings_split`),
then fits all three component ratings on the (optionally filtered) plays
and reshapes them into one wide per-team table with dense ranks and a
net-rating z-score.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (e.g. `2023`) or a list of seasons to pool into one combined fit. |
| `as_of_date` | `date \| None` | `None` | When given, the leakage boundary -- only plays from games with `date < as_of_date` are used to fit the ratings (mirrors what was knowable heading into that date). `None` (default) uses the full season(s), unfiltered. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs forwarded to all three component functions. Defaults to `RatingsConfig` when omitted. |
| `fbs_only` | `bool` | `True` | Keep only FBS-vs-FBS games (gameonpaper `cfb-team-summaries` parity) -- both of the schedule's `home_division` / `away_division` must be `"fbs"`. Default True. Skipped (all games kept) when the schedule lacks the division columns; pass False to rate FCS opponents as regular teams. |
| `drop_kneels` | `bool` | `True` | Strip kneel-downs before fitting (gameonpaper parity). Default True. Uses a pipeline `kneel_down` flag when present, otherwise the play-text regex (`kneel` / `takes a knee`) plus the end-of-half anonymized-TEAM-run clock heuristic; skipped when neither a flag nor a play-text column exists. Pass False to let kneels with non-null EPA flow into the fit. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

A DataFrame with one row per `team_id`, columns in this order: `season` (Int64 -- the single passed season for the common single-season call; `null` for a pooled multi-season call, since no single season applies to every row), `team_id` (Utf8), `adj_off_epa`, `adj_def_epa` (Float64, from `efficiency_ratings`), `adj_st_epa` (Float64, from `special_teams_ratings`), `adj_net` (Float64 -- offense minus defense only; special teams is a separate column, not folded in), `fei_off`, `fei_def`, `fei_net` (Float64, from `fei_ratings`), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model uses), `off_rank` (Int64, dense rank on `adj_off_epa` descending), `def_rank` (Int64, dense rank on `adj_def_epa` **ascending** -- fewer EPA allowed ranks better), `net_rank` (Int64, dense rank on `adj_net` descending), `net_z` (Float64, z-score of `adj_net`), `fei_off_rank` (Int64, dense rank on `fei_off` descending), `fei_def_rank` (Int64, dense rank on `fei_def` **ascending** -- fewer drive EPA allowed ranks better), `fei_net_rank` (Int64, dense rank on `fei_net` descending). Zero-row (correctly-typed) when the requested season(s) have no published pbp/schedule asset, or when `as_of_date` filters out every play.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season (4-digit year). |
| `team_id` | character | ESPN team id. |
| `adj_off_epa` | double |  |
| `adj_def_epa` | double |  |
| `adj_st_epa` | double |  |
| `adj_net` | double |  |
| `fei_off` | double |  |
| `fei_def` | double |  |
| `fei_net` | double |  |
| `games` | integer | Number of games included in the ATS summary. |
| `off_pace` | double |  |
| `off_rank` | integer |  |
| `def_rank` | integer |  |
| `net_rank` | integer |  |
| `net_z` | double |  |
| `fei_off_rank` | integer |  |
| `fei_def_rank` | integer |  |
| `fei_net_rank` | integer |  |

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import cfb_ratings
ratings = cfb_ratings(2023)
ratings.sort("net_rank").head()

# As-of-date leakage boundary

import datetime as dt
week3 = cfb_ratings(2023, as_of_date=dt.date(2023, 9, 18))

# Pandas round-trip

ratings_pd = cfb_ratings(2023, return_as_pandas=True)
```

### cfb_recruiting_projection {#cfb_recruiting_projection}

`cfb_recruiting_projection(target_season: 'int', *, division: 'str' = 'fbs', history_seasons: 'list[int] | None' = None, alpha: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Project team wins / scoring margin for a season from preseason roster features.

Fits a ridge regression of realized wins (and average scoring margin) on
`[talent_composite, blue_chip_ratio, off_returning, def_returning,
prior_wins]` over strictly-prior seasons, then predicts the target season
from its preseason-known features. The as-of boundary is enforced
internally: rows with `season >= target_season` never enter training even
if `history_seasons` includes them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int` |  | Season to project. |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `history_seasons` | `list[int] \| None` | `None` | Seasons to draw training rows from (default: the six seasons before `target_season`). |
| `alpha` | `float` | `1.0` | Ridge L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per team: `season` (Int64, = target), `team_id` (Utf8 ESPN id), `pred_wins`, `pred_margin` (Float64), `pred_net_epa` (Float64, currently null -- the adjusted-EPA target's hosted pbp source 404s). Zero-row (typed) when no history is available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Target season being projected (equals the requested target_season). |
| `team_id` | character | ESPN team id as a string (integer-origin). |
| `pred_wins` | double | Ridge-projected season win total from preseason roster features. |
| `pred_margin` | double | Ridge-projected average scoring margin per game. |
| `pred_net_epa` | double | Reserved adjusted-EPA projection - currently null (the hosted pbp source 404s). |

**Example**

```python
from sportsdataverse.cfb import cfb_recruiting_projection
proj = cfb_recruiting_projection(2024)
proj.sort("pred_wins", descending=True).head(10)
```

### cfb_roster_talent {#cfb_roster_talent}

`cfb_roster_talent(seasons: 'int | list[int]', *, division: 'str' = 'fbs', composite_247: 'pl.DataFrame | None' = None, max_class_size: 'int' = 25, rank_decay: 'float' = 0.75, recruits: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Team-talent composite per team-season (247 Team Talent Composite style).

Talent is the class-recency-weighted sum of per-recruit star points over the
trailing eligible recruiting classes (window = the length of the division's
`class_recency_weights`). When a 247 team-talent snapshot is supplied via
`composite_247`, its value overrides the derived composite for matched
team-seasons (the derived value remains the fallback).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Target season or list of seasons to rate. |
| `division` | `str` | `'fbs'` | Division slug for `get_constants` (star points, weights). |
| `composite_247` | `DataFrame \| None` | `None` | Optional frame with `season` (Int64), `team_id` (Utf8), `talent_247` (Float64). Join-key dtypes are asserted. |
| `max_class_size` | `int` | `25` | Top-N recruits per class that count toward `talent_composite`, ranked by star points. Defaults to the FBS limit of 25 initial counters. Raise it only deliberately: an uncapped sum measures class VOLUME, which put Air Force 7th nationally on 200 signees at a 0.000 blue-chip ratio. Largely superseded by `rank_decay`; retained as a hard floor. |
| `rank_decay` | `float` | `0.75` | Diminishing-returns exponent on a recruit's rank within their class (see RANK_DECAY`). 0.0 restores the flat sum. The default 0.75 was selected by sweeping against Spearman with actual wins, not chosen by taste. |
| `recruits` | `DataFrame \| None` | `None` | Pre-loaded per-recruit frame (the `load_recruit_classes` contract). Supplying it SKIPS the 247 fetch entirely, which is what the cfbfastR-cfb-data producer does when compiling from the raw store: a class is immutable once signed, but the composite spans a 4-season window, so fetching live re-pulled the same frozen classes once per target season (~20 min per call). Callers passing this own the frame's completeness. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per `(season, team_id)`: `team` (Utf8), `talent_composite` (Float64), `talent_rank` (Int64 dense rank desc within season), `blue_chip_ratio` (Float64), `n_recruits` (Int64). Zero-row (typed) when no recruits load.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the talent composite describes (trailing eligible classes aggregated). |
| `team_id` | character | 247Sports signed-institution team key as a string (integer-origin; joins to the recruit feed, not ESPN). |
| `team` | character | 247Sports full team name - the cross-source name-join key (the recruit-feed and talent-feed id spaces differ). |
| `talent_composite` | double | Class-recency-weighted sum of per-recruit star points (247 Team Talent Composite style); the 247 snapshot value when composite_247 is supplied. |
| `talent_rank` | integer | Dense rank on talent_composite descending within season (best = 1). |
| `blue_chip_ratio` | double | Share of the trailing four signing classes rated 4+ stars. |
| `n_recruits` | integer | Total signees across the trailing recruiting-class window. |

**Example**

```python
from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent
tal = cfb_roster_talent(2023)
tal.sort("talent_rank").head(10)
```

### cfb_simulations {#cfb_simulations}

`cfb_simulations(games: 'FrameLike', teams: 'FrameLike', compute_results: 'Optional[ComputeResultsFn]' = None, *, simulations: 'int' = 10000, playoff_seeds: 'int' = 12, tiebreaker_depth: 'str' = 'SOS', sim_include: 'str' = 'POST', rankings: 'Optional[FrameLike]' = None, seed: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Dict[str, Union[pl.DataFrame, Any]]'`

Simulate college football seasons (nflseedR-style week loop).

Replicates the input season `simulations` times, fills unplayed games
week by week through the pluggable `compute_results`, then simulates
the postseason (conference championships + CFP bracket) and aggregates
per-team probabilities. See the module docstring for every documented
CFB simplification.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `FrameLike` |  | One season of games in the engine schema (`season` or `sim`, `week`, `game_type`, `home_team`, `away_team`, `result` — null = unplayed, `neutral`). Played results are kept as-is. |
| `teams` | `FrameLike` |  | Team table (`team`, `conference`). |
| `compute_results` | `Optional[ComputeResultsFn]` | `None` | Results generator with the signature `fn(teams, games, week_num, **kwargs) -> {"teams": ..., "games": ...}` filling `result` for that week's unplayed games only. Defaults to `cfb_compute_results` (dynamic ELO). |
| `simulations` | `int` | `10000` | Number of simulated seasons (sequential, no chunking). |
| `playoff_seeds` | `int` | `12` | CFP field size passed to `cfb_playoff_seeds`. |
| `tiebreaker_depth` | `str` | `'SOS'` | nflseedR depth ladder (`RANDOM` < `PRE-SOV` < `SOS` < `POINTS`) used by every standings computation. |
| `sim_include` | `str` | `'POST'` | How deep to simulate: `"REG"` (regular season only), `"CONF"` (+ conference championships) or `"POST"` (+ CFP bracket, default). |
| `rankings` | `Optional[FrameLike]` | `None` | Optional committee rankings (`team`, `rank`) forwarded to `cfb_playoff_seeds`. When None, seeding falls back to the per-sim standings ordering (documented in `cfb_playoff_seeds`). |
| `seed` | `Optional[int]` | `None` | Seed for the numpy RNG (deterministic runs). |
| `return_as_pandas` | `bool` | `False` | Return pandas DataFrames instead of polars. |

**Returns**

Dict of frames mirroring the nflseedR summary list: * `"standings"` — per (sim, team) standings incl. `conf_rank`, `conf_champ` and (`sim_include="POST"`) `seed`. * `"games"` — all games incl. simulated results and generated postseason rows. * `"overall"` — per-team probabilities (`won_conf`, `made_playoff`, `first_round_bye`, `won_cfp`) and mean record columns. * `"game_summary"` — per unique matchup: games played, home win / tie rates and mean margin.

| col_name | type | description |
|---|---|---|
| `team` | character | Team name the simulated probabilities belong to (overall summary frame). |
| `conference` | character | Conference the team belongs to; null or "FBS Independents" marks an independent. |
| `wins` | double | Mean wins per simulated season (all game types through the conference championship). |
| `losses` | double | Mean losses per simulated season (all game types through the conference championship). |
| `ties` | double | Mean ties per simulated season. |
| `win_pct` | double | Mean overall win percentage across the simulated seasons. |
| `won_conf` | double | Share of simulations in which the team won its conference (CONF_CHAMP game winner, or rank-1 fallback). |
| `made_playoff` | double | Share of simulations in which the team made the College Football Playoff field. |
| `first_round_bye` | double | Share of simulations in which the team earned a CFP first-round bye (seed 4 or better). |
| `won_cfp` | double | Share of simulations in which the team won the College Football Playoff national championship. |

**Example**

```python
from sportsdataverse.cfb import cfb_simulations
out = cfb_simulations(games, teams, simulations=100, seed=42,
                      playoff_seeds=12)
print(out["overall"].sort("won_cfp", descending=True).head())

# Regular season only

out = cfb_simulations(games, teams, simulations=100,
                      sim_include="REG", seed=1)
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

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()

# Custom ridge penalty

from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig
ratings = efficiency_ratings(pbp, config=RatingsConfig(ridge_lambda=100.0))
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

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

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
