---
title: "NBA — additional Python functions — Models and calculators: score_win–wp_variants"
sidebar_label: "Models and calculators: score_win–wp_variants"
sidebar_position: 14
description: "NBA — additional Python functions — Models and calculators: score_win–wp_variants — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: score_win–wp_variants

### score_win_prob {#score_win_prob}

`score_win_prob(pbp: 'pl.DataFrame', *, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None, neutral_site: 'Optional[bool]' = False, rated_home_prob: 'Optional[float]' = None) -> 'pl.DataFrame'`

Per-play home win probability, its play-to-play delta, and the prior, for one game.

NBA and WNBA games score with the NBA / WNBA in-game booster
(`~sportsdataverse.nba.nba_game_predict.nba_in_game_win_prob`), men's and women's college
games with the men's / women's college booster
(`~sportsdataverse.mbb.mbb_game_predict.mbb_in_game_win_prob`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game in the basketball pbp schema (needs `home_team_spread`, `game_spread_available`, `period_number`, `start_game_seconds_remaining`, the period clock (`start_quarter_seconds_remaining` or `start_period_seconds_remaining`), `home_score`, `away_score`, `team_id`, `home_team_id`), in play order. |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods (2 halves, 4 quarters); default from the frame's `season` via `game_regulation_periods`. |
| `neutral_site` | `Optional[bool]` | `False` | Whether the game is at a neutral site: no home-court advantage in a lineless prior; see `home_prior`. |
| `rated_home_prob` | `Optional[float]` | `None` | The game's rated pregame home win probability (NBA / WNBA: `~sportsdataverse.nba.nba_rated_prior.nba_rated_priors`), the prior when there is no line; see `home_prior`. |

**Returns**

The input plus `pregame_home_prob`, `prior_source` (`"spread"`, `"rated"`, `"neutral"` or `"hfa"`), `wp_regime`, `home_win_prob` and `wp_delta` (the first play's delta is measured from the prior). `home_win_prob` is settled to 1.0 / 0.0 on "End Game" / "End Period" rows (`type_text`) from the last regulation period on (period 2 in a halves game, 4 in a quarters game) where the score is not tied, before `wp_delta` is taken; tied rows go to overtime and keep the model value. A frame without `type_text` is not settled. An empty input returns an empty frame with those columns. A null `home_score` / `away_score` means "no change" and is carried forward within the game for the model (a null before the first score is 0); the returned `home_score` / `away_score` are left as given.

| col_name | type | description |
|---|---|---|
| `pregame_home_prob` | Float64 | Pregame home win probability fed to the in-game model: Phi(home_team_spread / margin_sd) when a line existed, else rated_home_prob when the caller passed one (nba_rated_priors), else home-court advantage alone (0.5 at a neutral site). |
| `prior_source` | String | Where the prior came from: "spread" (a real line), "rated" (no line; the as-of team-ratings prior passed as rated_home_prob), "hfa" (no line and no rating; home-court advantage only, so the default +2.5 never reaches the model) or "neutral" (no line or rating at a neutral site; prior 0.5). |
| `wp_regime` | String | "reg" for regulation plays; "ot" for plays after the last regulation period (period 3 on in a halves game, 5 on in a quarters game), scored on the overtime period clock as a five-minute endgame. |
| `home_win_prob` | Float64 | Home win probability before the play from the bundled in-game model (score margin, time left, the prior, possession). |
| `wp_delta` | Float64 | Change in home win probability from the previous play; the first play's delta is measured from the pregame prior. |

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing (`home_team_spread`, `game_spread_available`, `period_number`, `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`, or neither period clock column). |
| `ValueError` | If the league is unknown, `league` contradicts `league_id`, or `pbp` holds more than one `game_id`. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import score_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
one = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
wp = score_win_prob(one)

# Biggest swings

wp.sort(pl.col("wp_delta").abs(), descending=True).select("text", "wp_delta").head(10)

# A men's college tournament game

from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
one = load_mbb_pbp([2026]).filter(pl.col("game_id") == 401830342)
score_win_prob(one, league="mbb", neutral_site=True)
```

### season_game_excitement {#season_game_excitement}

`season_game_excitement(pbp: 'pl.DataFrame', *, league_id: 'str | None' = None, league: 'str | None' = None, schedule: 'pl.DataFrame | None' = None) -> 'pl.DataFrame'`

Every game's excitement index, lead changes and biggest swing, ranked within its season.

Scores each game with `score_win_prob` and `game_excitement`, then ranks the
completed games of each season by GEI. A game whose pbp stops before the end (a truncated
release) keeps its GEI so far but is not ranked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Plays of one league in the basketball pbp schema (a season's `load_<league>_pbp` release, or any set of games), the columns `score_win_prob` needs plus `game_id` and `season`. |
| `league_id` | `str \| None` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `str \| None` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `schedule` | `DataFrame \| None` | `None` | Optional `load_<league>_schedule` frame; its `game_id` / `neutral_site` give each game's neutral site for the prior (games it lacks are not neutral). |

**Returns**

One row per game, sorted by `season` then `game_id`: `game_id`, `season`, `gei`, `lead_changes`, `max_swing_pts`, `completed`, `gei_rank` (1 = the season's most exciting completed game, ties share the best rank), `gei_pct` (the share of the season's completed games with a GEI at or below this one, so the top game is 1.0) and `gei_n` (the season's completed games). The three rank columns are null for a game that is not completed. An empty input returns an empty frame with these columns.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id, the join key to the play-by-play and box-score frames. |
| `season` | Int64 | Season year as the pbp release labels it (the year the season ends). |
| `gei` | Float64 | Game excitement index: the league's mean plays per game (GEI_SCALE) / the game's plays x the sum of the absolute play-to-play home win-probability changes, the first from the pregame prior and, in a completed game, the last play to the result; football's Game on Paper formula with a per-league scale. |
| `lead_changes` | Int64 | How often the scoring leader flipped; a tie is not a lead, so one side ahead, tied, then the other ahead counts once. |
| `max_swing_pts` | Float64 | Largest single-play home win-probability change, in percentage points, with no team named. |
| `completed` | Boolean | Whether the pbp reaches the end of the game (an End Game / End Period row from the last regulation period on with the score not tied), so the last play was taken to the result. |
| `gei_rank` | Int64 | Rank of gei among the season's completed games, 1 = most exciting, ties share the best rank; null for a game that is not completed. |
| `gei_pct` | Float64 | Share of the season's completed games with a gei at or below this one (the top game is 1.0); null for a game that is not completed. |
| `gei_n` | Int64 | Number of completed games in the season the rank and percentile are taken over; null for a game that is not completed. |

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing. |
| `ValueError` | If the league is unknown or `league` contradicts `league_id`. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import season_game_excitement
from sportsdataverse.wnba.wnba_loaders import load_wnba_pbp, load_wnba_schedule
gei = season_game_excitement(load_wnba_pbp([2025]), league="wnba", schedule=load_wnba_schedule([2025]))

# The season's best games

gei.sort("gei_rank", nulls_last=True).head(10)
```

### season_gram {#season_gram}

`season_gram(possessions: 'pl.DataFrame') -> 'SeasonGram'`

The `SeasonGram` of a possession frame (rows with a full lineup).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup rows (`points`, `off_player_1..5`, `def_player_1..5`). |

**Returns**

The season's `SeasonGram`.

### shot_coordinates {#shot_coordinates}

`shot_coordinates(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Shot locations in the court frame the Game on Paper hex chart uses.

ESPN's play coordinates are feet across the court with the hoop at `(25, 0)` and `y`
growing toward half court (measured on 401766458: dunks at `y` about 2, a 22-foot corner
three at `(3, 0)`). The chart wants tenths of a foot with the hoop at the origin, so
`x_legacy = (x - 25) * 10` and `y_legacy = y * 10`. Free throws carry no real location
(the NBA frame pins them to `(25, 13.75)`, the college frames to the rim, `(25, 0)`;
`(?i)free ?throw` matches `Free Throw - 1 of 2` and college `MadeFreeThrow`) and ESPN's
missing-coordinate sentinel is a large negative number; both become `null`, as does every
non-shooting play.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Plays in the `load_nba_pbp` schema (`shooting_play`, `type_text`, `coordinate_x_raw`, `coordinate_y_raw`). |

**Returns**

The input plus `x_legacy` and `y_legacy` (Float64).

| col_name | type | description |
|---|---|---|
| `x_legacy` | Float64 | Shot location in tenths of a foot with the hoop at the origin, x across the court; null for free throws, unlocated shots and non-shooting plays. |
| `y_legacy` | Float64 | Shot location in tenths of a foot with the hoop at the origin, y toward half court; null for free throws, unlocated shots and non-shooting plays. |

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing (`shooting_play`, `type_text`, `coordinate_x_raw`, `coordinate_y_raw`). |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import shot_coordinates
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2025]).filter(pl.col("game_id") == 401766458)
shots = shot_coordinates(pbp).filter(pl.col("x_legacy").is_not_null())
shots.select("text", "x_legacy", "y_legacy").head()
```

### team_game_rates {#team_game_rates}

`team_game_rates(team_box: 'pl.DataFrame', *, periods: 'Optional[int]' = None, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None) -> 'pl.DataFrame'`

Four factors, possessions, ratings and pace for both teams of one game.

Dean Oliver's estimates on the `load_nba_team_boxscore` columns: possessions =
FGA - ORB + TOV + 0.44 FTA, averaged over the two teams; eFG% = (FGM + 0.5 3PM) / FGA;
TOV% = TOV / (FGA + 0.44 FTA + TOV); ORB% = ORB / (ORB + opponent DRB); FT rate = FTA / FGA;
ORtg / DRtg per 100 possessions; pace per regulation length (48 minutes NBA; 40 WNBA, MBB and WBB). `TOV` is the team's player plus team turnovers:
`total_turnovers`, except that when it is null it is `turnovers + team_turnovers`, and when ESPN
has duplicated the team turnover (`team_turnovers == turnovers` and `total_turnovers == 2 *
turnovers`, measured on every 2002-2012 game, e.g. 17/17/34) it is `turnovers`. Without the
`turnovers` / `team_turnovers` columns it is `total_turnovers` as given. For MBB and WBB
`TOV` is `turnovers` (`team_turnovers` in WBB 2009-2012, where the other two are 0), and a 0
is a real 0, except where the WBB box recorded none: in WBB before 2009 (and for a WBB box with
no `season`) and in a WBB game of any season whose two teams together show fewer than 5,
TOV%, possessions, ratings and pace are null, never computed from unrecorded turnovers. A
team's 0 in a game whose total is 5 or more stays 0. A zero denominator gives `null`.
For the NBA, `orb_pct` is null when either team's `offensive_rebounds + defensive_rebounds`
differs from its `total_rebounds` (ESPN's 2002-2012 boxes); a box without `total_rebounds`
and the other leagues are not checked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_box` | `DataFrame` |  | Exactly two rows (one per team) of one game in the `load_nba_team_boxscore` schema. |
| `periods` | `Optional[int]` | `None` | Highest period played (default: the regulation periods), for the pace denominator. |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA (48-minute regulation) / `"10"` WNBA (40). |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA); overtime is 5 minutes in all four. |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods (2 or 4); default `game_regulation_periods` for the league and the box's `season`. |

**Returns**

One row per `(game_id, team_id)` with `efg_pct, tov_pct, orb_pct, ft_rate, possessions, off_rtg, def_rtg, pace, total_turnovers, game_minutes`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id, the join key to the play-by-play and box-score frames. |
| `team_id` | Int64 | ESPN team id, the join key to the team box score. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 x 3PM) / FGA; null with no attempts. |
| `tov_pct` | Float64 | Turnover rate, total turnovers / (FGA + 0.44 x FTA + total turnovers); null when the denominator is not positive or the box records no turnovers (WBB before 2009, or a WBB game whose two teams together show fewer than 5 turnovers). |
| `orb_pct` | Float64 | Offensive rebound rate, own ORB / (own ORB + opponent DRB); null when the denominator is not positive. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA; null with no attempts. |
| `possessions` | Float64 | Oliver's possession estimate (FGA - ORB + total turnovers + 0.44 x FTA) averaged over the two teams; null when the box records no turnovers (WBB before 2009, or a WBB game whose two teams together show fewer than 5 turnovers). |
| `off_rtg` | Float64 | Points scored per 100 possessions; null when possessions are not positive. |
| `def_rtg` | Float64 | Points allowed per 100 possessions; null when possessions are not positive. |
| `pace` | Float64 | Possessions per regulation length (48 minutes NBA; 40 WNBA, MBB and WBB), overtime minutes included in the denominator; null when the box records no turnovers. |
| `total_turnovers` | Int64 | The team turnover count every rate above uses, player plus team turnovers; ESPN's doubled pre-2013 NBA total is repaired to the player count; college: the box turnovers (team_turnovers in WBB 2009-2012), null when not recorded (WBB before 2009, or a WBB game whose two teams together show fewer than 5 turnovers). |
| `game_minutes` | Int64 | Minutes played in the game: regulation (48 NBA, 40 WNBA and college) plus five per overtime. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `team_box` does not hold exactly two rows, or the rows are not two different `team_id` values of one `game_id`, or the league is unknown, or `league` contradicts `league_id`. |
| `polars.exceptions.ColumnNotFoundError` | If a required box-score column is missing. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import team_game_rates
from sportsdataverse.nba.nba_loaders import load_nba_team_boxscore
box = load_nba_team_boxscore([2025]).filter(pl.col("game_id") == 401766458)
team_game_rates(box).select("team_id", "efg_pct", "off_rtg", "pace")

# WNBA box (40-minute regulation)

team_game_rates(box, league_id="10")

# A women's college game from the halves era

from sportsdataverse.wbb.wbb_loaders import load_wbb_team_boxscore
box = load_wbb_team_boxscore([2010]).filter(pl.col("game_id") == 293170052)
team_game_rates(box, league="wbb", regulation_periods=2)
```

### train_box_prior {#train_box_prior}

`train_box_prior(panel: 'pl.DataFrame', *, season: 'int', half_life: 'float' = 4.0, alpha: 'float' = 1.0, folds: 'int' = 5) -> 'BoxPriorModel'`

Possession-weighted ridge of the career target on the box-prior features, cross-fit by player.

Fold `k`'s rule is fit on every row whose player is NOT in fold `k` (`player_id % folds`) and
is applied only to fold `k`'s players, so no player's prior saw his own target. Each row is
weighted by its possessions times the time weight of its season relative to `season`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | Training rows: `player_id`, `season`, `poss`, `def_poss`, `o_target`, `d_target` and `BOX_PRIOR_FEATURES`; seasons from `box_prior_training_seasons` (a row of `season` itself raises). |
| `season` | `int` |  | The season the prior is for. |
| `half_life` | `float` | `4.0` | Half-life of the row time weight, in seasons. |
| `alpha` | `float` | `1.0` | Ridge penalty on the standardized features. |
| `folds` | `int` | `5` | Player folds. |

**Returns**

A `BoxPriorModel`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `panel` holds a row of `season` or a fold has no training rows. |

**Example**

```python
from sportsdataverse.nba.nba_box_prior import apply_box_prior, train_box_prior
model = train_box_prior(panel, season=2015)
prior = apply_box_prior(features_2015, model)
```

### train_spm {#train_spm}

`train_spm(box_features: 'pl.DataFrame', rapm_target: 'pl.DataFrame', *, feature_names: 'Optional[List[str]]' = None, alpha: 'float' = 100.0) -> 'SpmCoefficients'`

Ridge-fit box features onto `o_rapm` and `d_rapm` (two regressions).

The two models share the same feature matrix but separate target vectors,
producing independent offense and defense coefficient vectors.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_features` | `DataFrame` |  | Per-player per-100 features. Must contain `player_id` and every column in *feature_names*. |
| `rapm_target` | `DataFrame` |  | Per-player RAPM target frame with columns `player_id`, `o_rapm`, and `d_rapm`. Only the rows whose `player_id` appears in *box_features* are used (inner join). |
| `feature_names` | `Optional[List[str]]` | `None` | Ordered list of feature columns to regress on. Defaults to `SPM_FEATURES` (= `_STATS` from `nba_box_logs`). |
| `alpha` | `float` | `100.0` | Ridge regularization strength (`sklearn.linear_model.Ridge`). Lower values approach OLS; higher values shrink toward zero. |

**Returns**

`SpmCoefficients` with offense and defense coefficient vectors, intercepts, and the ordered `feature_names`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If *feature_names* lists a column absent from *box_features* or *rapm_target* is missing required columns. |

**Example**

```python
from sportsdataverse.nba import train_spm
coef = train_spm(box_feats, rapm_ratings)

# With custom regularization

coef = train_spm(box_feats, rapm_ratings, alpha=50.0)
```

### validate_model {#validate_model}

`validate_model(model: 'AnyModel', season_frames: 'List[pl.DataFrame]', *, model_name: 'str' = 'model', oracles: 'Tuple[str, ...]' = ('retrodiction', 'reliability', 'cross_season', 'calibration'), seed: 'int' = 0, external_ratings: 'Optional[pl.DataFrame]' = None, external_oracle: 'Optional[pl.DataFrame]' = None, external_rating_col: 'str' = 'rating', external_oracle_col: 'str' = 'oracle_value', external_join: 'str' = 'id', walk_forward_horizon_days: 'int' = 14, walk_forward_min_games: 'int' = 15) -> 'ValidationReport'`

Run the selected oracles and assemble a `ValidationReport`.

`retrodiction`/`reliability`/`calibration` run on the pooled possessions
(all seasons concatenated); `cross_season` runs on the ordered per-season
frames. Any oracle not selected is left `None`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A fitted or unfitted RAPM-family estimator (`fit(X, y)` protocol). |
| `season_frames` | `List[DataFrame]` |  | Ordered list of per-season possession frames. All frames are concatenated into a single pooled frame for Oracles 1, 2, and 4. |
| `model_name` | `str` | `'model'` | Label written into the returned report and markdown card. |
| `oracles` | `Tuple[str, ...]` | `('retrodiction', 'reliability', 'cross_season', 'calibration')` | Tuple of oracle names to run. Omit a name to skip that oracle and leave its result field `None`. Accepts `"external"` and `"walk_forward"` in addition to the four original names; the default tuple is unchanged, so existing callers are unaffected. |
| `seed` | `int` | `0` | RNG seed forwarded to each oracle for determinism. |
| `external_ratings` | `Optional[DataFrame]` | `None` | The model's own ratings frame -- required when `"external"` is in `oracles`. |
| `external_oracle` | `Optional[DataFrame]` | `None` | A loaded oracle frame (from `nba_oracle_data`) -- required when `"external"` is in `oracles`. |
| `external_rating_col` | `str` | `'rating'` | Rating column name in `external_ratings`. |
| `external_oracle_col` | `str` | `'oracle_value'` | Value column name in `external_oracle`. |
| `external_join` | `str` | `'id'` | `"id"` or `"name"`, forwarded to `external_validity`. |
| `walk_forward_horizon_days` | `int` | `14` | Forwarded to `walk_forward`. |
| `walk_forward_min_games` | `int` | `15` | Forwarded to `walk_forward` as `min_games_before_first_checkpoint`. |

**Returns**

A `ValidationReport` whose fields are populated for every selected oracle and `None` for every skipped oracle.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `"external"` is in `oracles` but `external_ratings` or `external_oracle` is `None`. |

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model,
)

# season_frames is a list[pl.DataFrame] of possession stints
rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
print(rep.retrodiction.game_margin_rmse)   # out-of-sample margin RMSE
print(rep.reliability.spearman_brown)      # split-half Spearman-Brown
print(rep.calibration)                     # None — RidgeRapmModel has no posterior

# Skip slow oracles when iterating quickly

rep = validate_model(
    RidgeRapmModel(), season_frames,
    oracles=("retrodiction", "reliability"),
)
print(rep.cross_season)   # None — not selected
```

### walk_forward {#walk_forward}

`walk_forward(model: 'AnyModel', possessions: 'pl.DataFrame', *, checkpoint_dates: 'Optional[List[datetime.date]]' = None, horizon_days: 'int' = 14, min_games_before_first_checkpoint: 'int' = 15) -> 'WalkForwardResult'`

Oracle 6: time-ordered "predict tomorrow" retrodiction.

For each checkpoint date D: fit on games with `game_date <= D`, predict
games with `D < game_date <= D + horizon_days`, aggregate to
per-(game, team) margins -- reusing `_fit_on` / `_design_with_ids`
/ `predict_points` / `_team_game_margins` (the same machinery
`retrodiction` uses). `carry_forward_rmse` reapplies the PREVIOUS
checkpoint's fit (no refit) to the current window. `random_fold_rmse` is
`retrodiction`'s pooled game-margin RMSE on the same possessions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model (`RapmModel`/`RatingsModel`/`PriorModel`). |
| `possessions` | `DataFrame` |  | A season possession+lineup frame with `game_date` (from `compile_nba_season`), `game_id`, `offense_team_id`, `points`, and the ten lineup columns. |
| `checkpoint_dates` | `Optional[List[date]]` | `None` | Explicit checkpoint grid; derived from `possessions` via `horizon_days`/`min_games_before_first_checkpoint` when `None` (default). |
| `horizon_days` | `int` | `14` | Days-ahead prediction window per checkpoint (default 14). |
| `min_games_before_first_checkpoint` | `int` | `15` | Distinct-game-date index of the first checkpoint when deriving the default grid (default 15, "~game 15 of the season"). |

**Returns**

`WalkForwardResult`. All metrics `nan` and counts `0` when `possessions` is empty, lacks a `game_date` column, or the derived/given grid produces zero non-degenerate checkpoints.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel, walk_forward
res = walk_forward(RidgeRapmModel(), season_possessions)
print(res.game_margin_rmse, res.carry_forward_rmse, res.random_fold_rmse)
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, league_id: 'str' = '00') -> 'float'`

Home win probability from an expected margin (normal-CDF closed form).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home-minus-away margin in points. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted margin sigma. |

**Returns**

Probability the home team wins, in `(0, 1)`.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import win_prob_from_margin
win_prob_from_margin(5.0)
```

### wp_variants {#wp_variants}

`wp_variants(pbp: 'pl.DataFrame', market_home_prob: 'float | None' = None, *, league: 'str | None' = None, regulation_periods: 'int | None' = None, neutral_site: 'bool | None' = False) -> 'pl.DataFrame'`

Two extra per-play home WP columns for one game: naive (prior 0.5) and market-anchored.

Both rescore the plays with `~sportsdataverse.nba.nba_game_page.score_win_prob`, the same
in-game model as `home_win_prob`, with only the pregame prior swapped: 0.5 for
`home_win_prob_naive` (the game state alone: score, clock and possession), `market_home_prob` for
`home_win_prob_market`. End-of-game rows settle to 1 / 0 as in `score_win_prob`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game in the basketball pbp schema, as `score_win_prob` takes it. |
| `market_home_prob` | `float \| None` | `None` | The game's pregame no-vig market home WP (`game_lines` `market_home_wp_close`); `None` or NaN leaves `home_win_prob_market` null. |
| `league` | `str \| None` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `int \| None` | `None` | The game's regulation periods; see `score_win_prob`. |
| `neutral_site` | `bool \| None` | `False` | Passed to `score_win_prob` (it does not change a given prior). |

**Returns**

`game_id` (when present), `game_play_number` (when present), `home_win_prob_naive` and `home_win_prob_market`, one row per play in play order; empty with those columns for an empty `pbp`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 / Int64 | ESPN game id, the join key to the play-by-play and the play_wp table (the dtype the pbp carries). |
| `game_play_number` | Int32 / Int64 | Play order within the game, as in the play-by-play (the dtype the pbp carries). |
| `home_win_prob_naive` | Float64 | Home win probability from the game state alone (score, clock and possession), the in-game model with a 0.5 prior. |
| `home_win_prob_market` | Float64 | Home win probability with the closing no-vig market WP as the prior; null without a line. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the league is unknown, `market_home_prob` is outside [0, 1], or `pbp` holds more than one game. |
| `polars.exceptions.ColumnNotFoundError` | If a column `score_win_prob` needs is missing. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_lines import wp_variants
from sportsdataverse.nba.nba_loaders import load_nba_pbp
one = load_nba_pbp([2025]).filter(pl.col("game_id") == 401704638)
wp = wp_variants(one, 0.31, league="nba")

# Where the market prior moved the curve most

wp.with_columns((pl.col("home_win_prob_market") - pl.col("home_win_prob_naive")).alias("d")).sort("d")
```
