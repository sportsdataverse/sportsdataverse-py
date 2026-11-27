---
title: "NFL — additional Python functions — Analytics"
sidebar_label: "Analytics"
sidebar_position: 28
description: "NFL — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Analytics

### calculate_nfl_standings {#calculate_nfl_standings}

`calculate_nfl_standings(games: 'pl.DataFrame', *, teams: 'pl.DataFrame | None' = None, tiebreaker_depth: 'int' = 3, playoff_seeds: 'int | None' = None, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Compute NFL division standings + conference playoff seeds.

A reduced port of the tiebreaker ladder nflfastR delegates to the external
`nflseedR` package (see the module docstring for the exact scope). Games
are doubled into one row per team per game, regular-season win/loss/tie
records are computed per team, and ties are broken win_pct -> head-to-head
-> division record -> conference record, to the depth configured by
`tiebreaker_depth`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | A `load_nfl_schedule`-shaped frame: `game_id`, `season`, `game_type`, `week`, `home_team`, `away_team`, `home_score`, `away_score`. Only `game_type == "REG"` rows with both scores present are used. |
| `teams` | `DataFrame \| None` | `None` | A `load_nfl_teams`-shaped frame (`team_abbr`, `team_conf`, `team_division`). When `None` (default), calls `sportsdataverse.nfl.load_nfl_teams`. Must cover every team abbreviation appearing in `games` -- a team absent from `teams` gets null `conf`/`division` and is silently pooled into the `(season, None)` division/conference group rather than raising. |
| `tiebreaker_depth` | `int` | `3` | `1` (win_pct only), `2` (adds head-to-head + division record), or `3` (default; adds conference record too). |
| `playoff_seeds` | `int \| None` | `None` | Number of teams per conference that receive a non-null `seed`. When `None` (default), uses the 2020 playoff -format cutover: `6` for seasons <= 2019, `7` for 2020+. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with one row per (season, team): `conf`, `division`, `div_rank`, `seed` (null past `playoff_seeds`), `team`, `games`, `wins`, `losses`, `ties`, `win_pct` (ties count as 0.5 win), `div_pct`, `conf_pct`. Sorted by `(season, division, div_rank, seed)`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `conf` | String |  |
| `division` | String |  |
| `div_rank` | Int64 |  |
| `seed` | Int64 |  |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `games` | Int64 | Games played in career |
| `wins` | Int64 |  |
| `losses` | Int64 |  |
| `ties` | Int64 |  |
| `win_pct` | Float64 |  |
| `div_pct` | Float64 |  |
| `conf_pct` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `tiebreaker_depth` is not `1`, `2`, or `3`. |

**Example**

```python
from sportsdataverse.nfl import calculate_nfl_standings, load_nfl_schedule
games = load_nfl_schedule(seasons=[2023])
standings = calculate_nfl_standings(games)
standings.filter(standings["div_rank"] == 1)

# Injected teams frame (offline)

standings = calculate_nfl_standings(games, teams=my_teams_df)

# Pipeline next step (one line)

standings.sort(["conf", "seed"]).select("team", "seed", "win_pct")
```

### compose_counting_projection {#compose_counting_projection}

`compose_counting_projection(rate_proj: 'pl.DataFrame', avail_proj: 'pl.DataFrame', *, rate_col: 'str' = 'proj_rate', volume_col: 'str' = 'proj_volume') -> 'pl.DataFrame'`

Compose skill and availability into a counting projection.

The **only** place skill (rate x volume) and availability meet:
`proj_counting = rate * volume * proj_availability`, joined on
`player_id` (dtype-asserted).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rate_proj` | `pl.DataFrame` |  | Skill projection carrying `player_id` + `rate_col` + `volume_col`. |
| `avail_proj` | `pl.DataFrame` |  | Availability projection carrying `player_id` + `proj_availability`. |
| `rate_col` | `str` | `'proj_rate'` | Rate column name in `rate_proj`. |
| `volume_col` | `str` | `'proj_volume'` | Volume column name in `rate_proj`. |

**Returns**

`rate_proj` columns plus `proj_availability` and `proj_counting:Float64`.

| col_name | type | description |
|---|---|---|
| `player_id` | String | nflverse gsis player id (character join key; asserted Utf8 on both sides of the join). |
| `proj_rate` | Float64 | Projected per-opportunity rate carried through from the skill projection (rate_col). |
| `proj_volume` | Float64 | Projected opportunity volume carried through from the skill projection (volume_col). |
| `proj_availability` | Float64 | Projected availability rate in [0, 1] from nfl_availability_projection. |
| `proj_counting` | Float64 | Composed counting projection - proj_rate x proj_volume x proj_availability (the only place skill and availability meet). |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.nfl_availability import compose_counting_projection
out = compose_counting_projection(rate_frame, avail_frame)
```

### nfl_availability_projection {#nfl_availability_projection}

`nfl_availability_projection(seasons: 'list[int]', target_season: 'int', *, team_games: 'int' = 17, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Empirical-Bayes availability projection: expected fraction of team games.

Shrinks each player's historical availability toward the fitted position

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load snap counts/rosters for. |
| `target_season` | `int` |  | The season being projected. |
| `team_games` | `int` | `17` | Regular-season team games. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

`player_id:Utf8, target_season:Int64, position:Utf8, proj_availability:Float64, proj_games:Float64, proj_games_missed:Float64`. Empty history returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | String | nflverse gsis player id (character join key). |
| `target_season` | Int64 | The season being projected (features use strictly earlier seasons only). |
| `position` | String | Roster position from the most recent visible season. |
| `proj_availability` | Float64 | Projected availability rate in [0, 1] - empirical-Bayes shrinkage of historical snap-based availability toward the fitted position base rate, then the fold-fit linear recalibration. |
| `proj_games` | Float64 | Expected games available - proj_availability x team_games (17). |
| `proj_games_missed` | Float64 | Expected games missed - team_games minus proj_games. |

**Example**

```python
from sportsdataverse.nfl.nfl_availability import nfl_availability_projection
avail = nfl_availability_projection([2021, 2022, 2023], 2024)
avail.sort("proj_games").head()
```

### nfl_expected_starters {#nfl_expected_starters}

`nfl_expected_starters(schedule: 'pl.DataFrame', *, depth_charts: 'pl.DataFrame | None' = None, injuries: 'pl.DataFrame | None' = None, config: 'QBLayerConfig | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Pregame expected starting QB, backup, report status and start probability per team-game.

1. A timestamped depth chart (nflverse's `dt` format, 2025 on): QB1 and QB2 of the
   team's last snapshot strictly before kickoff (`gameday` + `gametime`,
   US Eastern).
2. Otherwise the team's realized starter in its previous game of the season, with no
   known backup. A team's first game with no snapshot has no expected starter.

`status` is the expected starter's `report_status` on that week's injury report
(Out / Doubtful / Questionable; null, Probable or absent is `none`), and `p_start`
the fitted chance he starts at that status. The realized starter columns of the
schedule (`home_qb_id` / `away_qb_id`) are read only for rule 2's previous game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | `sportsdataverse.nfl.load_nfl_schedule` rows (`game_id`, `season`, `week`, `gameday`, `gametime`, `home_team`, `away_team`, `home_qb_id`, `away_qb_id`). |
| `depth_charts` | `DataFrame \| None` | `None` | `sportsdataverse.nfl.load_nfl_depth_charts` rows in the timestamped (`dt`) format. `None` uses rule 2 only. |
| `injuries` | `DataFrame \| None` | `None` | `sportsdataverse.nfl.load_nfl_injuries` rows (`season`, `week`, `team`, `gsis_id`, `report_status`). `None` gives every starter status `none`. |
| `config` | `QBLayerConfig \| None` | `None` | Layer constants; defaults to `QB_LAYER_CONSTANTS["modern"]`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

One row per team-game: `game_id` / `team` / `expected_qb_id` / `backup_qb_id` / `starter_source` (`depth_chart` / `previous_start` / null) / `status` (Utf8), `kickoff` / `chart_dt` (UTC Datetime; `chart_dt` is the snapshot used, null under rule 2), `p_start` (Float64). Zero-row on an empty schedule. `team` is the franchise's current code (`OAK` -> `LV`, `SD` -> `LAC`, `STL` -> `LA`), as in the play-by-play.

| col_name | type | description |
|---|---|---|
| `game_id` | String | nflverse game id from the schedule. |
| `team` | String | Franchise code of the side, relocated to the current one as in the play-by-play. |
| `kickoff` | Datetime | Kickoff in UTC (schedule gameday + gametime, US Eastern). |
| `chart_dt` | Datetime | Timestamp of the depth-chart snapshot used, strictly before kickoff; null when the previous start was used or there is no source. |
| `expected_qb_id` | String | GSIS id of the pregame expected starter: depth-chart QB1, else the previous game's starter. |
| `backup_qb_id` | String | GSIS id of depth-chart QB2 from the same snapshot; null without a snapshot or when it lists one QB. |
| `starter_source` | String | Where the expected starter came from: depth_chart, previous_start, or null (no source). |
| `status` | String | Expected starter's final injury-report status that week: Out, Doubtful, Questionable, or the string none. |
| `p_start` | Float64 | Fitted chance the expected starter starts at that status (1.0 when there is no expected starter). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | On a game with no `gametime`, depth charts without `dt`, an injury or depth-chart team code the schedule does not have, or a `p_start` missing a status. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts, load_nfl_injuries, load_nfl_schedule
from sportsdataverse.nfl.nfl_qb_layer import nfl_expected_starters
starters = nfl_expected_starters(
    load_nfl_schedule([2025]),
    depth_charts=load_nfl_depth_charts([2025]),
    injuries=load_nfl_injuries([2025]),
)
```

### nfl_game_script {#nfl_game_script}

`nfl_game_script(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Team-season pace / PROE / expected-plays engine.

Loads pbp + schedules for `seasons`, aggregates per-game pace to the
team-season level, and computes expected plays per game from the fitted
`sportsdataverse.nfl.nfl_scheme_constants.PACE_CONSTANTS`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons (nflverse pbp coverage). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, team)`: `games`, `off_plays_pg`, `sec_per_play`, `neutral_sec_per_play`, `proe`, `exp_plays_pg`, `plays_oe`, `pace_rank` (1 = fastest neutral pace). Empty seasons yield a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the aggregate. |
| `team` | String | Team abbreviation. |
| `games` | Int64 | Games included in the aggregate. |
| `off_plays_pg` | Float64 | Realized offensive plays per game. |
| `sec_per_play` | Float64 | Season mean of the per-game sec_per_play. |
| `neutral_sec_per_play` | Float64 | Season mean neutral-situation seconds per play (lower = faster). |
| `proe` | Float64 | Season pass-rate over expected, dropback-weighted so it reconciles exactly with the pbp pass_oe aggregate. |
| `exp_plays_pg` | Float64 | Expected plays per game from the fitted PACE_CONSTANTS OLS (own pace, opponent pace, market total). |
| `plays_oe` | Float64 | Realized minus expected plays per game. |
| `pace_rank` | Int64 | Rank of neutral pace within the season (1 = fastest). |

**Example**

```python
from sportsdataverse.nfl.nfl_gamescript import nfl_game_script
gs = nfl_game_script([2023])
print(gs.sort("proe", descending=True).head())

# Pipeline next step

gs.filter(pl.col("plays_oe") > 0).sort("plays_oe", descending=True).head()
```

### nfl_game_story {#nfl_game_story}

`nfl_game_story(pbp: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame]'`

How the winner won it, for one or many NFL games: edges in EPA, the moment, periods, runs, performers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | The ESPN-processed plays of one or many games: the `espn_nfl_pbp` release or a live game's `NFLPlayProcess.plays_frame` (set by `run_processing_pipeline()`), with the columns `~sportsdataverse.cfb.cfb_game_story.cfb_game_story` lists. The NFL frame has no `homeFinalScore` / `awayFinalScore`, so the final is the last running score and `periods_agree` is always true; add those two columns (the schedule's score) to check it. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"summary", "edges", "moment", "periods", "runs", "performers"}`, the frames of `~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`: the accounting phases (`passing`, `rushing`, `special_teams`, `other`) and splits (`turnovers`, `explosive_plays`, `success_rate`, `red_zone`, `late_downs`) in expected points, ranked; the largest-`|wpa|` play; quarters and `OT`. A tie (`decided` false) has null winner ids.

`summary`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's ESPN game id. |
| `home_team_id` | Int64 | Home team's ESPN team id. |
| `away_team_id` | Int64 | Away team's ESPN team id. |
| `home_score` | Int64 | Home team's official final score. |
| `away_score` | Int64 | Away team's official final score. |
| `winner_team_id` | Int64 | Winning team's id; null on a tie. |
| `loser_team_id` | Int64 | Losing team's id; null on a tie. |
| `margin` | Int64 | Winner's final score less the loser's; 0 on a tie. |
| `decided` | Boolean | True when the game has a winner (false on a tie). |
| `periods_agree` | Boolean | True when the period sums (from the play-by-play's running score) equal the official final; false flags a source whose plays end on another score. |
| `n_periods` | Int64 | Periods played, overtimes included. |
| `overtime` | Boolean | True when the game went past regulation. |

`edges`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's ESPN game id. |
| `winner_team_id` | Int64 | Winning team's id (null on a tie, where the edges read from the home side's view). |
| `loser_team_id` | Int64 | Losing team's id (null on a tie). |
| `edge` | String | Edge name. Accounting: passing, rushing, special_teams, other (every play with an EPA once; their sum is the net EPA). Splits: turnovers, explosive_plays, success_rate, red_zone, late_downs. |
| `kind` | String | accounting for edges that split the game between them (their points_worth add up to the margin, exactly or nearly; see the function's docstring), split for overlapping cuts. |
| `unit` | String | Always points: expected points added, the unit the edges are priced in. |
| `winner_value` | Float64 | Winner's value for the edge in the edge's own unit: points for a points edge, a rate for a rate edge. |
| `loser_value` | Float64 | Loser's value for the edge, in the same unit as winner_value. |
| `points_worth` | Float64 | The winner's edge in the sport's scoring unit: the winner's points from the edge less the loser's. Positive favours the winner. |
| `rank` | Int64 | Rank of the edge within the game by points_worth, 1 = largest; equal worths keep the edge list's order. |

`moment`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's ESPN game id. |
| `play_id` | Int64 | ESPN play id of the deciding moment. |
| `order` | Int64 | The play's position in the game's play order. |
| `period` | Int64 | Period of the play. |
| `clock` | String | Game clock of the play as the source shows it. |
| `team_id` | Int64 | Team of the play (the offense in football, the event's team in basketball). |
| `text` | String | Play description as the source writes it. |
| `wpa` | Float64 | Home win probability after the play less before it. |
| `winner_wpa` | Float64 | The same change from the winner's side (null on a tie). |
| `home_score` | Int64 | Home score after the play. |
| `away_score` | Int64 | Away score after the play. |
| `rule` | String | max_abs_wpa: the play with the largest absolute win-probability change (earliest on ties); deciding_score where no play has one: the winner's score that put it ahead for good. |

`periods`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's ESPN game id. |
| `period` | Int64 | Period number, from 1; overtimes follow regulation. |
| `period_label` | String | Q1-Q4, then OT, OT2, ... |
| `is_overtime` | Boolean | True for a period after regulation. |
| `home_pts` | Int64 | Home points in the period: its last running score less the previous period's. |
| `away_pts` | Int64 | Away points in the period. |
| `home_cum` | Int64 | Home running score at the end of the period. |
| `away_cum` | Int64 | Away running score at the end of the period. |

`runs`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's ESPN game id. |
| `team_id` | Int64 | Team that made the run. |
| `points` | Int64 | The side's largest unanswered run: points it scored in a row with no score by the other side (a score correction stays in the run and counts against its side). |
| `start_order` | Int64 | Play order of the run's first score. |
| `end_order` | Int64 | Play order of the run's last score. |
| `start_period` | Int64 | Period of the run's first score. |
| `end_period` | Int64 | Period of the run's last score. |
| `home_score_after` | Int64 | Home score at the end of the run. |
| `away_score_after` | Int64 | Away score at the end of the run. |

`performers`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's ESPN game id. |
| `team_id` | Int64 | Team of the top performer. |
| `player_id` | Int64 | ESPN athlete id of the side's top performer. |
| `player_name` | String | Player's display name. |
| `metric` | String | Always epa: the expected points added the performer is chosen by. |
| `value` | Float64 | The player's EPA as passer, rusher and receiver combined (a target credits both the passer and the receiver; ties go to touchdowns). |

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If `pbp` lacks a column the football adapter reads. |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.nfl_game_story import nfl_game_story
url = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
    "espn_nfl_pbp/play_by_play_2025.parquet"
)
pbp = pl.read_parquet(url, use_pyarrow=True)
story = nfl_game_story(pbp.filter(pl.col("game_id") == pbp["game_id"].max()))
story["edges"].head(3)

# Every game's moment

nfl_game_story(pbp)["moment"].select("game_id", "text", "winner_wpa")
```

### nfl_play_call_probabilities {#nfl_play_call_probabilities}

`nfl_play_call_probabilities(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None, *, models_dir: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Score the bundled play-call classifier over offensive plays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp (must carry `xpass`; run `sportsdataverse.nfl.ep_wp.calculate_xpass` first if not). |
| `participation` | `Optional[DataFrame]` | `None` | Optional participation frame for personnel features. |
| `models_dir` | `Optional[str]` | `None` | Optional directory holding `nfl_playcall.ubj` (defaults to the bundled package artifact; no first-use download). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Keys + per-family probabilities `p_inside_run` / `p_outside_run` / `p_short_pass` / `p_deep_pass` / `p_scramble`, `p_pass` (pass-family sum), `pred_family` (argmax) and `pass_oe_model` (`100 * (is_pass - p_pass)`). Empty input yields a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `game_id` | String | nflverse game identifier (Utf8 join key). |
| `play_id` | Int64 | nflverse play identifier within the game (Int64 join key). |
| `season` | Int64 | Season of the play. |
| `week` | Int64 | Week of the play. |
| `posteam` | String | Offense (possession) team abbreviation. |
| `p_inside_run` | Float64 | Predicted probability of an inside run (guard/center gap or middle). |
| `p_outside_run` | Float64 | Predicted probability of an outside run (end/tackle or off-middle). |
| `p_short_pass` | Float64 | Predicted probability of a short pass. |
| `p_deep_pass` | Float64 | Predicted probability of a deep pass. |
| `p_scramble` | Float64 | Predicted probability of a QB scramble. |
| `p_pass` | Float64 | Predicted pass probability (short + deep + scramble family sum). |
| `pred_family` | String | Argmax family among the five class probabilities. |
| `pass_oe_model` | Float64 | Pass-rate over model expectation for the play, 100 * (is_pass - p_pass). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_playcall import nfl_play_call_probabilities
out = nfl_play_call_probabilities(calculate_xpass(load_nfl_pbp([2023])))
print(out.select("p_pass", "pred_family").head())

# Pipeline next step

out.group_by("posteam").agg(pl.col("p_pass").mean()).sort("p_pass")
```

### nfl_play_call_tendencies {#nfl_play_call_tendencies}

`nfl_play_call_tendencies(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None, *, models_dir: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Aggregate scored play-call probabilities to team-season tendencies.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp (with `xpass`). |
| `participation` | `Optional[DataFrame]` | `None` | Optional participation frame. |
| `models_dir` | `Optional[str]` | `None` | Optional directory holding `nfl_playcall.ubj`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, posteam)`: `plays`, `mean_p_pass`, `pass_rate`, `proe` (`100 * (pass_rate - mean_p_pass)`) and the family mix shares `share_<family>`. Empty input yields a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the aggregate. |
| `posteam` | String | Offense team abbreviation. |
| `plays` | Int64 | Offensive run/pass plays scored. |
| `mean_p_pass` | Float64 | Mean model pass probability across the team's plays. |
| `pass_rate` | Float64 | Actual pass rate (scrambles count as passes). |
| `proe` | Float64 | Pass rate over expected, 100 * (pass_rate - mean_p_pass). |
| `share_inside_run` | Float64 | Share of plays labeled inside_run. |
| `share_outside_run` | Float64 | Share of plays labeled outside_run. |
| `share_short_pass` | Float64 | Share of plays labeled short_pass. |
| `share_deep_pass` | Float64 | Share of plays labeled deep_pass. |
| `share_scramble` | Float64 | Share of plays labeled scramble. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_playcall import nfl_play_call_tendencies
t = nfl_play_call_tendencies(calculate_xpass(load_nfl_pbp([2023])))
print(t.sort("proe", descending=True).head())
```

### nfl_player_props {#nfl_player_props}

`nfl_player_props(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, era: 'str' = 'modern', lines: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Empirical-Bayes player-prop projections, leakage-safe per week.

For every game in the requested season(s) (or, with `as_of_date`, every
game on/after that date), projects each rostered QB/RB/WR/TE's stat-family
mean as `usage x efficiency x matchup x game-script`:

- usage + efficiency from `player_usage_efficiency` built **as-of
  that game's week** (weeks strictly before it),
- the matchup multiplier from the opponent's `adj_def_epa` in
  `sportsdataverse.nfl.nfl_ratings.nfl_ratings` (as-of the week's
  first game date),
- game script from the **native** expected margin
  (`sportsdataverse.nfl.nfl_market.nfl_predict_games`) -- the
  market line is never read (binding non-market boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Season (e.g. `2023`) or list of seasons. |
| `as_of_date` | `date \| None` | `None` | When given, only games with `gameday >= as_of_date` are projected (history before each game's week still feeds the projections). `None` projects every week of the season(s). |
| `era` | `str` | `'modern'` | Constants era key. |
| `lines` | `DataFrame \| None` | `None` | Optional market lines to score `p_over` against -- columns `game_id` / `player_id` / `stat` (Utf8) + `line` (Float64), e.g. built from `espn_nfl_game_propbets` (ESPN only serves propbets for upcoming games). `None` leaves `line` / `p_over` null. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

One row per (player-game, stat): `season` / `week` (Int64), `game_id` / `player_id` / `position` / `team_id` / `opp_team_id` / `stat` (Utf8), `proj_mean` / `proj_sd` / `line` / `p_over` (Float64; `p_over = 1 - Phi((line - proj_mean) / proj_sd)` when a line is joined, else null). Stats are `passing_yards` (QB), `rushing_yards` (RB), `receiving_yards` (WR/TE). Zero-row, correctly-typed when there is nothing to project.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the projection belongs to. |
| `week` | Int64 | Week of the projected game; only weeks strictly before it feed the projection. |
| `game_id` | String | Game identifier from the schedule (nflverse id, e.g. "2023_06_DET_TB"). |
| `player_id` | String | nflverse GSIS player identifier (character join key). |
| `position` | String | Player position (QB, RB, WR, TE) selecting the projected stat family. |
| `team_id` | String | Player's team nflverse abbreviation as of the projection week. |
| `opp_team_id` | String | Opponent team nflverse abbreviation (drives the matchup multiplier). |
| `stat` | String | Projected stat name (passing_yards for QB, rushing_yards for RB, receiving_yards for WR/TE). |
| `proj_mean` | Float64 | Projected stat mean - EB-shrunk usage x efficiency x opponent matchup x game-script. |
| `proj_sd` | Float64 | Residual standard deviation for the stat family (fitted on the 2023 as-of backtest). |
| `line` | Float64 | Market prop line joined from the caller-supplied lines frame (e.g. espn_nfl_game_propbets); null when no line is available. |
| `p_over` | Float64 | Probability the player exceeds `line`, 1 - Phi((line - proj_mean) / proj_sd); null without a line. |

**Example**

```python
from sportsdataverse.nfl import nfl_player_props
props = nfl_player_props(2023)
props.filter(props["stat"] == "passing_yards").head()

# Upcoming-only, as-of a date

import datetime as dt
props = nfl_player_props(2024, as_of_date=dt.date(2024, 11, 1))
```

### nfl_predict_games {#nfl_predict_games}

`nfl_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, era: 'str' = 'modern', odds: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Vectorized pregame predictions (+ display-only market edge) per game.

Joins `ratings` twice (home/away) onto the schedule and computes the
three closed-form predictions. `odds` is **display-only**: it feeds
`market_edge = exp_margin - close_spread_home` and never the
predictions themselves (the binding non-market boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game: `game_id` (Utf8), `home_team_id` / `away_team_id` (Utf8 team abbreviations), `neutral_site` (Boolean). |
| `ratings` | `DataFrame` |  | The `sportsdataverse.nfl.nfl_ratings.nfl_ratings` output (needs `team_id`, `adj_off_epa`, `adj_def_epa`, `adj_net`). |
| `era` | `str` | `'modern'` | Constants era key. |
| `odds` | `DataFrame \| None` | `None` | Optional market frame (`game_id`, `close_spread_home` -- the market's expected home margin, positive = home favored). A sportsbook quotes the home favourite as `-7`: negate the quoted line (`close_spread_home = 7`) before passing it. Nothing in the package builds this column. Games absent from `odds` get a null `market_edge`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

One row per input game: `game_id` / `home_team_id` / `away_team_id` (Utf8), `neutral_site` (Boolean), `exp_margin` / `home_win_prob` / `exp_total` / `market_edge` (Float64; `market_edge` null without odds). Zero-row, correctly-typed on empty input.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Game identifier carried through from the input schedule. |
| `home_team_id` | String | Home team nflverse abbreviation (character; the ratings `team_id` join key). |
| `away_team_id` | String | Away team nflverse abbreviation (character; the ratings `team_id` join key). |
| `neutral_site` | Boolean | Whether the game is at a neutral site (home-field advantage is dropped when true). |
| `exp_margin` | Float64 | Expected home scoring margin in points (points_per_net * net rating differential + the fitted home-field advantage on non-neutral fields). |
| `home_win_prob` | Float64 | Home win probability, Phi(exp_margin / margin_sd) under a Gaussian margin model. |
| `exp_total` | Float64 | Expected combined point total (avg_total + total_scale * the four-way efficiency matchup sum). |
| `market_edge` | Float64 | Display-only native-minus-market spread edge (exp_margin - close_spread_home); null when no odds frame is supplied. |

**Raises**

| Exception | Description |
|---|---|
| `AssertionError` | If a join-key dtype disagrees (`home_team_id` / `away_team_id` vs `ratings.team_id`, or `game_id` vs `odds.game_id`). |

**Example**

```python
from sportsdataverse.nfl import nfl_ratings
from sportsdataverse.nfl.nfl_market import nfl_predict_games
ratings = nfl_ratings(2023)
preds = nfl_predict_games(games, ratings)
preds.sort("home_win_prob", descending=True).head()

# With a market edge (display only)

preds = nfl_predict_games(games, ratings, odds=odds)
```

### nfl_punter_value {#nfl_punter_value}

`nfl_punter_value(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Punter net-field-position value over expected.

Expected net comes from the shipped punt landing distribution
(`nfl_fourth_down._load_punt_data`) evaluated at each punt's line of
scrimmage; realized net is `kick_distance - return_yards - 20*touchback`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, punter_player_id)`: `punts`, `gross_avg`, `net_avg`, `exp_net_avg`, `net_over_expected`, `epa`. Empty seasons yield a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the aggregate. |
| `punter_player_id` | String | nflverse punter GSIS id (Utf8 join key). |
| `punts` | Int64 | Punts with a recorded kick distance. |
| `gross_avg` | Float64 | Mean gross punt distance (yards). |
| `net_avg` | Float64 | Mean net distance, kick_distance - return_yards - 20 * touchback. |
| `exp_net_avg` | Float64 | Mean expected net from the shipped punt landing distribution at each punt's line of scrimmage. |
| `net_over_expected` | Float64 | net_avg minus exp_net_avg (yards of field position per punt over expectation). |
| `epa` | Float64 | Total EPA on the punter's punt plays (kicking-team perspective). |

**Example**

```python
from sportsdataverse.nfl.nfl_special_teams import nfl_punter_value
pv = nfl_punter_value([2023])
print(pv.head())
```

### nfl_qb_dropbacks {#nfl_qb_dropbacks}

`nfl_qb_dropbacks(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

One row per offensive pass or run play, with the QB on dropbacks.

The shared input of the layer: QB values read the dropback rows, and each
team's dropback share reads all of them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-schema play-by-play (`sportsdataverse.nfl.load_nfl_pbp`) with `game_id`, `season`, `game_date`, `posteam`, `play_type`, `qb_dropback`, `qb_epa`, `passer_player_id` and `rusher_player_id`. |

**Returns**

`game_id` / `team` / `qb_id` (Utf8; `qb_id` null off dropbacks), `season` (Int64), `game_date` (Date), `qb_epa` (Float64), `is_dropback` (Boolean). Plays with `play_type` other than `pass` / `run` are dropped; a dropback with no QB id or no `qb_epa` is kept as a play but not as a dropback.

| col_name | type | description |
|---|---|---|
| `game_id` | String | nflverse game id of the play (`2025_05_DET_CIN`). |
| `season` | Int64 | Season of the play (the year the season starts). |
| `game_date` | Date | Date of the game, the as-of key every QB-layer window is cut on. |
| `team` | String | Offense's franchise code, relocated to the current one (OAK -> LV, SD -> LAC, STL -> LA). |
| `qb_id` | String | GSIS id of the QB on a dropback (passer, else scrambling rusher); null off dropbacks. |
| `qb_epa` | Float64 | nflverse qb_epa of the play (EPA credited to the QB). |
| `is_dropback` | Boolean | True on a QB dropback with a QB id and a non-null qb_epa; False on a run, an unattributed play or a null qb_epa. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_qb_layer import nfl_qb_dropbacks
db = nfl_qb_dropbacks(load_nfl_pbp([2024]))
```

### nfl_qb_margin_mixture {#nfl_qb_margin_mixture}

`nfl_qb_margin_mixture(margins: 'Sequence[float]', weights: 'Sequence[float]', *, sigma: 'float') -> 'dict[str, float]'`

Mean, win probability and 80% interval of a Normal mixture of home margins.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `margins` | `Sequence[float]` |  | Component means (points, home minus away). |
| `weights` | `Sequence[float]` |  | Component weights (non-negative; renormalised to sum to 1). |
| `sigma` | `float` |  | Common component standard deviation (the spine's `margin_sd`). |

**Returns**

`{"mean", "win_prob", "lo80", "hi80"}`: the mixture mean, `P(margin > 0)`, and its 0.10 / 0.90 quantiles (bisection to 1e-6 points). One component reduces to `m +/- 1.2816 * sigma`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | On empty or mismatched inputs, or weights that do not sum to a positive number. |

**Example**

```python
from sportsdataverse.nfl.nfl_qb_layer import nfl_qb_margin_mixture
nfl_qb_margin_mixture([3.0, -2.0], [0.7, 0.3], sigma=13.02)
```

### nfl_qb_predict_games {#nfl_qb_predict_games}

`nfl_qb_predict_games(schedule: 'pl.DataFrame', pbp: 'pl.DataFrame', *, starters: 'pl.DataFrame | None' = None, starter: "Literal['expected', 'realized']" = 'expected', era: 'str' = 'modern', config: 'QBLayerConfig | None' = None, ratings_config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

Base spine and QB-layer pregame margin, win probability and 80% interval per game.

For each game date the spine's efficiency ratings are fitted on that season's plays
from games strictly before the date (as `sportsdataverse.nfl.nfl_ratings`
with `as_of_date`), QB values on career dropbacks before the date, and each
team's dropback exposure on the same window. `base_*` is the shipped spine;
`qb_*` adds the starter shift and the start-probability mixture.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | The games to price (`sportsdataverse.nfl.load_nfl_schedule` rows; `location == "Neutral"` marks a neutral site). |
| `pbp` | `DataFrame` |  | nflverse play-by-play covering the priced seasons and the QB history (from `config.history_start_season`). |
| `starters` | `DataFrame \| None` | `None` | `nfl_expected_starters` output, required for `starter="expected"`. |
| `starter` | `Literal['expected', 'realized']` | `'expected'` | `"expected"` (the pregame arm) or `"realized"` (the schedule's `home_qb_id` / `away_qb_id` with `p = 1`: an upper bound, never a pregame input). |
| `era` | `str` | `'modern'` | Spine constants era (`hfa`, `points_per_net`, `margin_sd`). |
| `config` | `QBLayerConfig \| None` | `None` | Layer constants; defaults to `QB_LAYER_CONSTANTS["modern"]`. |
| `ratings_config` | `RatingsConfig \| None` | `None` | Ridge constants; defaults to `RatingsConfig`. |

**Returns**

One row per priced game with a rating for both teams (games before either team has a rated play are dropped): `game_id` / `home_team` / `away_team` (Utf8, team codes relocated as in `nfl_expected_starters`), `season` / `week` (Int64), `neutral_site` (Boolean); `base_margin` / `base_win_prob` / `base_lo80` / `base_hi80` (the shipped spine) and `qb_margin` / `qb_win_prob` / `qb_lo80` / `qb_hi80` (the layer's mixture mean, P(home wins) and 80% interval), all Float64; per side `*_qb_id` / `*_status` (Utf8), `*_p_start` and `*_shift` (Float64, the starter's shift to `adj_net` in EPA per play) and `*_layer_applied` (Boolean, False when the side had no starter or no rated dropbacks and the layer left it unchanged).

| col_name | type | description |
|---|---|---|
| `game_id` | String | nflverse game id from the schedule. |
| `season` | Int64 | Season of the game. |
| `week` | Int64 | Week of the game. |
| `home_team` | String | Home franchise code, relocated to the current one. |
| `away_team` | String | Away franchise code, relocated to the current one. |
| `neutral_site` | Boolean | True when the schedule's location is Neutral (no home-field advantage). |
| `base_margin` | Float64 | Shipped spine's expected home margin in points, ratings as of the game date. |
| `base_win_prob` | Float64 | Shipped spine's home win probability, Phi(base_margin / margin_sd). |
| `base_lo80` | Float64 | Lower end of the spine's 80% home-margin interval (base_margin - 1.2816 margin_sd). |
| `base_hi80` | Float64 | Upper end of the spine's 80% home-margin interval (base_margin + 1.2816 margin_sd). |
| `qb_margin` | Float64 | QB layer's expected home margin: mean of the start-probability mixture. |
| `qb_win_prob` | Float64 | QB layer's home win probability: P(margin > 0) under the mixture. |
| `qb_lo80` | Float64 | 0.10 quantile of the QB layer's home-margin mixture. |
| `qb_hi80` | Float64 | 0.90 quantile of the QB layer's home-margin mixture. |
| `home_qb_id` | String | GSIS id of the home starter priced (expected, or realized in the realized arm). |
| `away_qb_id` | String | GSIS id of the away starter priced (expected, or realized in the realized arm). |
| `home_status` | String | Home expected starter's report status (Out, Doubtful, Questionable, or the string none; none in the realized arm). |
| `away_status` | String | Away expected starter's report status (Out, Doubtful, Questionable, or the string none; none in the realized arm). |
| `home_p_start` | Float64 | Chance the home expected starter starts (1.0 in the realized arm or when unpriced). |
| `away_p_start` | Float64 | Chance the away expected starter starts (1.0 in the realized arm or when unpriced). |
| `home_shift` | Float64 | Home starter's shift to adj_net in EPA per play: gamma * dropback share * (value - rated value). |
| `away_shift` | Float64 | Away starter's shift to adj_net in EPA per play: gamma * dropback share * (value - rated value). |
| `home_layer_applied` | Boolean | False when the home side had no starter or no rated dropbacks and was left unchanged. |
| `away_layer_applied` | Boolean | False when the away side had no starter or no rated dropbacks and was left unchanged. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `starter="expected"` and `starters` is None. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp, load_nfl_schedule
from sportsdataverse.nfl.nfl_qb_layer import nfl_expected_starters, nfl_qb_predict_games
sched = load_nfl_schedule([2025]).filter(pl.col("game_type") == "REG")
pbp = load_nfl_pbp(list(range(2006, 2026)))
preds = nfl_qb_predict_games(sched, pbp, starters=nfl_expected_starters(sched))
```

### nfl_qb_values {#nfl_qb_values}

`nfl_qb_values(dropbacks: 'pl.DataFrame', *, as_of_date: 'datetime.date', config: 'QBLayerConfig | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Shrunk career EPA per dropback for every QB, as of a date.

`qb_value = (n * e + k * prior) / (n + k)` over the QB's dropbacks in games
strictly before `as_of_date` (seasons from `config.history_start_season`),
with `prior` the replacement level below `new_qb_dropbacks` career
dropbacks and the league mean from there on.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dropbacks` | `DataFrame` |  | `nfl_qb_dropbacks` output. |
| `as_of_date` | `date` |  | Exclusive cutoff (the game's date). |
| `config` | `QBLayerConfig \| None` | `None` | Layer constants; defaults to `QB_LAYER_CONSTANTS["modern"]`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

One row per QB: `qb_id` (Utf8), `dropbacks` (Int64), `epa_per_dropback` (Float64, raw), `qb_value` (Float64, shrunk). A QB absent from the frame has no row; callers value him at the replacement level.

| col_name | type | description |
|---|---|---|
| `qb_id` | String | GSIS id of the quarterback. |
| `dropbacks` | Int64 | Career dropbacks before the as-of date, from the config's history start season. |
| `epa_per_dropback` | Float64 | Raw mean qb_epa per dropback over those career dropbacks (unshrunk). |
| `qb_value` | Float64 | Shrunk value (n * e + k * prior) / (n + k), prior = replacement under new_qb_dropbacks (default 100) else league mean. |

**Example**

```python
import datetime as dt
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_qb_layer import nfl_qb_values, nfl_qb_dropbacks
db = nfl_qb_dropbacks(load_nfl_pbp(list(range(2006, 2025))))
values = nfl_qb_values(db, as_of_date=dt.date(2024, 10, 1))
```

### nfl_season_standings {#nfl_season_standings}

`nfl_season_standings(games: 'pl.DataFrame', *, ranks: 'str' = 'CONF', tiebreaker_depth: 'str' = 'SOS', playoff_seeds: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute NFL standings with the real NFL tiebreaking procedures.

Faithful polars port of `nflseedR::nfl_standings()` (v2 engine,
`R/standings.R` L82-155): initializes records, points, win
percentages, SOV and SOS from a games frame, then resolves division
ranks, conference ranks (playoff seeds) and draft order through the
full NFL tiebreaker cascades.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Games frame with one row per game. Required columns: `sim` or `season` (identifier), `game_type` (`'REG'`, `'WC'`, `'DIV'`, `'CON'`, `'SB'`), `week`, `away_team`, `home_team`, and `result` (home score minus away score; no missing values allowed). `away_score` / `home_score` are additionally required for `tiebreaker_depth='POINTS'` and enable the `pf`/`pa`/`pd` output columns. |
| `ranks` | `str` | `'CONF'` | One of `'DIV'`, `'CONF'` (default), `'DRAFT'`, or `'NONE'` — which rank columns (and thus tiebreakers) to compute. `'DRAFT'` implies `'CONF'` implies `'DIV'`. |
| `tiebreaker_depth` | `str` | `'SOS'` | One of `'SOS'` (default), `'PRE-SOV'`, `'POINTS'`, or `'RANDOM'`. Controls how deep the tiebreaker cascade goes before falling back to a coin toss. |
| `playoff_seeds` | `Optional[int]` | `None` | If not `None`, only conference ranks up to this value are resolved with tiebreakers; deeper ranks are returned as null. Must be in 1-16. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a pandas DataFrame. |

**Returns**

A standings frame with one row per (sim/season, team) including records, `win_pct`/`div_pct`/`conf_pct`, `sov`, `sos`, and the requested `div_rank`/`conf_rank`/`draft_rank` columns plus `*_tie_broken_by` bookkeeping. `conf_rank` is the playoff seed.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season identifier from the input games frame (named `sim` instead when the input used a `sim` column). |
| `conf` | String | Conference of the team (AFC or NFC). |
| `division` | String | Division of the team (e.g. "AFC East"). |
| `team` | String | Team abbreviation. |
| `games` | Int64 | Number of regular season games played. |
| `wins` | Float64 | Regular season wins with ties counted as half a win. |
| `true_wins` | Int64 | Regular season wins excluding ties (outright wins only). |
| `losses` | Int64 | Regular season losses. |
| `ties` | Int64 | Regular season ties. |
| `pf` | Int64 | Points scored across regular season games (points for); present only when the input carries home_score and away_score. |
| `pa` | Int64 | Points allowed across regular season games (points against); present only when the input carries scores. |
| `pd` | Int64 | Regular season point differential (pf minus pa); present only when the input carries scores. |
| `win_pct` | Float64 | Regular season win percentage with ties counted as half a win. |
| `div_pct` | Float64 | Win percentage in games against division opponents (0 when the team played no division games). |
| `conf_pct` | Float64 | Win percentage in games against conference opponents (0 when the team played no conference games). |
| `sov` | Float64 | Strength of victory - combined win percentage of all opponents the team defeated (0 for winless teams). |
| `sos` | Float64 | Strength of schedule - combined win percentage of all opponents the team faced. |
| `div_rank` | Int64 | Rank within the division (1-4) after applying the NFL division tiebreaking procedures. |
| `div_tie_broken_by` | String | Tiebreaker step that resolved the team's division rank (e.g. "Head-To-Head Win PCT (2)" or "Coin Toss"); null when the rank needed no tiebreaker. |
| `conf_rank` | Int64 | Conference rank, i.e. the playoff seed, after applying the NFL conference tiebreaking procedures; null beyond `playoff_seeds` when that argument is set. |
| `conf_tie_broken_by` | String | Tiebreaker step that resolved the team's conference rank; null when the rank needed no tiebreaker. |
| `exit` | String | Round of the team's final game - REG, WC, DIV, CON, SB, or SB_WIN for the Super Bowl winner (returned with ranks="DRAFT"). |
| `draft_rank` | Int64 | Draft pick position (1 = first overall pick) derived from postseason exit, win percentage, SOS and the draft tiebreaking procedures (returned with ranks="DRAFT"). |
| `draft_tie_broken_by` | String | Tiebreaker step that resolved the team's draft rank; null when the rank needed no tiebreaker. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If required columns are missing, results contain missing values, an argument is out of range, or a team abbreviation is unknown. |

**Example**

```python
import sportsdataverse.nfl as nfl
games = nfl.load_schedules([2024])
standings = nfl.nfl_season_standings(games, ranks="DRAFT")
print(standings.shape)

# Playoff seeds only, pandas output

df = nfl.nfl_season_standings(
    games, ranks="CONF", playoff_seeds=7, return_as_pandas=True
)

# Pipeline next step (one line)

standings.filter(pl.col("conf_rank") <= 7).sort("conf", "conf_rank")
```

### nfl_special_teams_epa {#nfl_special_teams_epa}

`nfl_special_teams_epa(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Special-teams EPA by team-unit.

Units: `punt` / `punt_return` / `kickoff` / `kickoff_return` /
`field_goal` / `extra_point`.  On each punt/kickoff the kicking
team's unit carries the play EPA signed to the kicking team and the
return team's unit its negation, so a team's units sum to its total
ST-play EPA.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, team, unit)`: `plays`, `epa`, `epa_per_play`. Empty seasons yield a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the aggregate. |
| `team` | String | Team abbreviation. |
| `unit` | String | Special-teams unit (punt, punt_return, kickoff, kickoff_return, field_goal, extra_point). |
| `plays` | Int64 | Plays credited to the unit. |
| `epa` | Float64 | Total EPA credited to the unit (kicking team carries the play EPA signed to it; the return team carries its negation). |
| `epa_per_play` | Float64 | EPA per play for the unit. |

**Example**

```python
from sportsdataverse.nfl.nfl_special_teams import nfl_special_teams_epa
st = nfl_special_teams_epa([2023])
print(st.filter(pl.col("unit") == "punt").sort("epa", descending=True).head())
```

### playcall_features {#playcall_features}

`playcall_features(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None) -> 'pl.DataFrame'`

Build the play-call feature frame (one row per offensive run/pass play).

Filters to plays with `pass == 1` or `rush == 1`, derives the 5-class
`family` label (scramble > deep/short pass > inside/outside run), and
left-joins the optional participation frame for personnel counts.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp with the pre-snap feature columns + `pass` / `rush` / `qb_scramble` / `pass_length` / `run_location` / `run_gap` and `xpass`. |
| `participation` | `Optional[DataFrame]` | `None` | Optional nflverse participation frame with `game_id` / `play_id` / `offense_personnel`. |

**Returns**

Keys + `PLAYCALL_FEATURE_ORDER` columns + `family` + `is_pass`. Personnel columns are null (`has_participation=0`) when no participation row matches.

| col_name | type | description |
|---|---|---|
| `game_id` | String | nflverse game identifier (Utf8 join key). |
| `play_id` | Int64 | nflverse play identifier within the game (Int64 join key). |
| `season` | Int64 | Season of the play. |
| `week` | Int64 | Week of the play. |
| `posteam` | String | Offense (possession) team abbreviation. |
| `down` | Float64 | Down (1-4) at the snap. |
| `ydstogo` | Float64 | Yards to go for a first down. |
| `yardline_100` | Float64 | Yards from the opponent end zone at the snap. |
| `score_differential` | Float64 | Offense score minus defense score at the snap. |
| `half_seconds_remaining` | Float64 | Seconds remaining in the half. |
| `game_seconds_remaining` | Float64 | Seconds remaining in the game. |
| `wp` | Float64 | Start-of-play win probability for the offense. |
| `shotgun` | Float64 | 1 when the offense lined up in shotgun. |
| `no_huddle` | Float64 | 1 when the play was run without a huddle. |
| `xpass` | Float64 | Shipped nflfastR-parity expected-dropback probability for the play. |
| `n_rb` | Float64 | Running backs in the offensive personnel grouping (null without participation data). |
| `n_te` | Float64 | Tight ends in the offensive personnel grouping (null without participation data). |
| `n_wr` | Float64 | Wide receivers in the offensive personnel grouping (null without participation data). |
| `has_participation` | Int64 | 1 when a participation row matched the play, else 0. |
| `family` | String | 5-class play-call label (inside_run, outside_run, short_pass, deep_pass, scramble). |
| `is_pass` | Int64 | 1 when the play was a pass (including scrambles), else 0. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_playcall import playcall_features
feat = playcall_features(calculate_xpass(load_nfl_pbp([2023])))
print(feat["family"].value_counts())
```

### player_usage_efficiency {#player_usage_efficiency}

`player_usage_efficiency(player_stats: 'pl.DataFrame', *, as_of_week: 'int', era: 'str' = 'modern') -> 'pl.DataFrame'`

Per-player as-of usage + efficiency with empirical-Bayes shrinkage.

Aggregates one season of week-level player stats over weeks strictly
before `as_of_week` (the leakage boundary), then shrinks every usage
(per-game attempts / carries / targets) and efficiency (yards + TDs per
opportunity) stat toward its position prior:
`(n * player_value + kappa * prior) / (n + kappa)` with `n` = games
played and `kappa` the stat family's fitted shrinkage.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_stats` | `DataFrame` |  | One season of `load_nfl_player_stats()` rows (columns `player_id`, `position`, `recent_team`, `week`, `attempts`, `passing_yards`, `passing_tds`, `carries`, `rushing_yards`, `rushing_tds`, `targets`, `receiving_yards`, `receiving_tds`). |
| `as_of_week` | `int` |  | Only weeks `< as_of_week` are used. |
| `era` | `str` | `'modern'` | Constants era key (supplies kappas + position priors). |

**Returns**

One row per `player_id` (Utf8) whose position has a prior table: `position` / `team_id` (Utf8, latest team), `games` (Int64), `exp_attempts` / `exp_carries` / `exp_targets` (Float64, shrunk per-game usage), `ypa` / `ypc` / `ypt` / `pass_td_rate` / `rush_td_rate` / `rec_td_rate` (Float64, shrunk per-opportunity efficiency). Zero-row, correctly-typed on empty input.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | String | Primary position as reported by NFL.com |
| `team_id` | String |  |
| `games` | Int64 | Games played in career |
| `exp_attempts` | Float64 |  |
| `exp_carries` | Float64 |  |
| `exp_targets` | Float64 |  |
| `ypa` | Float64 |  |
| `ypc` | Float64 |  |
| `ypt` | Float64 |  |
| `pass_td_rate` | Float64 |  |
| `rush_td_rate` | Float64 |  |
| `rec_td_rate` | Float64 |  |

**Example**

```python
import polars as pl
import sportsdataverse.nfl as nfl
stats = nfl.load_nfl_player_stats().filter(pl.col("season") == 2023)
usage = nfl.player_usage_efficiency(stats, as_of_week=10)
usage.sort("exp_attempts", descending=True).head()
```

### predict_margin {#predict_margin}

`predict_margin(home_adj_net: 'float', away_adj_net: 'float', neutral: 'bool', *, era: 'str' = 'modern') -> 'float'`

Expected home scoring margin from two net ratings.

`points_per_net * (home_adj_net - away_adj_net)` plus the era HFA on
non-neutral fields.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_net` | `float` |  | Home team's `adj_net` (EPA/play units). |
| `away_adj_net` | `float` |  | Away team's `adj_net`. |
| `neutral` | `bool` |  | True drops the home-field advantage. |
| `era` | `str` | `'modern'` | Constants era key (default `"modern"`). |

**Returns**

Expected home margin in points (positive = home favored).

**Example**

```python
from sportsdataverse.nfl.nfl_market import predict_margin
predict_margin(0.10, -0.05, False)
```

### predict_total {#predict_total}

`predict_total(home_adj_off: 'float', home_adj_def: 'float', away_adj_off: 'float', away_adj_def: 'float', *, era: 'str' = 'modern') -> 'float'`

Expected combined point total from the four efficiency components.

`avg_total + total_scale * (home_adj_off + away_adj_def + away_adj_off +
home_adj_def)`. The four ratings are **summed** because each side's
scoring rises with its own offense and with the opponent's EPA-*allowed*
(`adj_def` is lower = better defense) -- same semantics as the shipped
CFB analog. (The plan text wrote this with a minus; that sign flips a
good defense into raising the total, so the analog's sum is used.)

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_off` | `float` |  | Home `adj_off_epa`. |
| `home_adj_def` | `float` |  | Home `adj_def_epa` (lower = better defense). |
| `away_adj_off` | `float` |  | Away `adj_off_epa`. |
| `away_adj_def` | `float` |  | Away `adj_def_epa`. |
| `era` | `str` | `'modern'` | Constants era key. |

**Returns**

Expected combined total in points.

**Example**

```python
from sportsdataverse.nfl.nfl_market import predict_total
predict_total(0.10, -0.02, 0.05, 0.01)
```

### team_game_pace {#team_game_pace}

`team_game_pace(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Per team-game pace + pass-rate-over-expected.

`sec_per_play` is the per-drive elapsed `game_seconds_remaining`
divided by drive plays, averaged over the team's offensive drives
(kneels / spikes / no_plays excluded).  Neutral = `wp` in [0.2, 0.8]
and `half_seconds_remaining` > 120.  `proe` is the mean `pass_oe`
over dropbacks.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp with `game_id` / `season` / `week` / `posteam` / `drive` / `play_type` / `qb_dropback` / `pass_oe` / `game_seconds_remaining` / `wp` / `half_seconds_remaining`. |

**Returns**

One row per `(game_id, season, week, posteam)` with `off_plays`, `sec_per_play`, `neutral_plays`, `neutral_sec_per_play`, `proe`. Empty input yields a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `game_id` | String | nflverse game identifier (Utf8 join key). |
| `season` | Int64 | Season of the game. |
| `week` | Int64 | Week of the game. |
| `posteam` | String | Offense team abbreviation. |
| `off_plays` | Int64 | Offensive plays in the game (kneels, spikes and no_plays excluded). |
| `sec_per_play` | Float64 | Mean over the team's drives of elapsed game clock divided by drive plays. |
| `neutral_plays` | Int64 | Offensive plays in neutral situations (wp in [0.2, 0.8], over 2 minutes left in the half). |
| `neutral_sec_per_play` | Float64 | sec_per_play computed on neutral-situation plays only. |
| `proe` | Float64 | Mean pass_oe over the team's dropbacks in the game (percentage points). |
| `dropbacks` | Int64 | Dropbacks with a non-null pass_oe (the proe denominator). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_gamescript import team_game_pace
pace = team_game_pace(load_nfl_pbp([2023]))
print(pace.sort("sec_per_play").head())
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, era: 'str' = 'modern') -> 'float'`

Home win probability from an expected margin (Gaussian margin model).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home margin in points. |
| `era` | `str` | `'modern'` | Constants era key (supplies `margin_sd`). |

**Returns**

`Phi(exp_margin / margin_sd)` in `[0, 1]`.

**Example**

```python
from sportsdataverse.nfl.nfl_market import win_prob_from_margin
win_prob_from_margin(3.0)
```
