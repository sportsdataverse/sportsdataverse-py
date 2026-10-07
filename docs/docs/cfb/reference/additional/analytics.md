---
title: "CFB — additional Python functions — Analytics"
sidebar_label: "Analytics"
sidebar_position: 9
description: "CFB — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Analytics

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

### cfb_adjusted_tempo {#cfb_adjusted_tempo}

`cfb_adjusted_tempo(seasons: 'Union[int, list[int]]', *, exclude_garbage: 'bool' = True, config: 'Optional[AdjustConfig]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team-season situation-neutral, opponent-adjusted tempo / pace.

Counts scrimmage plays per team-game (garbage time and kneels/spikes
dropped) and per-play elapsed seconds, then opponent-adjusts both with
the iterative solver on the per-game values (a fast team facing slow
defenses gets `adj_plays_game > raw_plays_game`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | season or list of seasons (hosted pbp covers 2002-2021). |
| `exclude_garbage` | `bool` | `True` | drop Connelly garbage-time plays. |
| `config` | `Optional[AdjustConfig]` | `None` | `AdjustConfig` for the solver. |
| `return_as_pandas` | `bool` | `False` | return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (season, team_id): `games, raw_plays_game, adj_plays_game, raw_sec_play, adj_sec_play, pace_rank` (rank 1 = fastest adjusted pace). Zero-row frame with the documented schema on empty input.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the pace covers. |
| `team_id` | character | Team ESPN id (character join key). |
| `games` | integer | Games with situation-neutral offensive snaps in the loaded seasons. |
| `raw_plays_game` | double | Situation-neutral scrimmage plays per game (garbage time and kneels/spikes excluded). |
| `adj_plays_game` | double | Opponent-adjusted situation-neutral plays per game (iterative solver; higher = faster). |
| `raw_sec_play` | double | Mean seconds elapsed per situation-neutral play (season total seconds over total plays). |
| `adj_sec_play` | double | Opponent-adjusted seconds elapsed per situation-neutral play (lower = faster). |
| `pace_rank` | integer | Dense rank on adj_plays_game descending (fastest adjusted pace = 1). |

**Example**

```python
from sportsdataverse.cfb import cfb_adjusted_tempo
df = cfb_adjusted_tempo([2021])
print(df.shape)

# Pipeline next step (one line)

df.sort("pace_rank").head()
```

### cfb_games_from_schedule {#cfb_games_from_schedule}

`cfb_games_from_schedule(schedule: 'FrameLike', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, Any]'`

Map a `load_cfb_schedule()` frame into the seedr engine `games` schema.

Derives `game_type` heuristically: games whose `notes` mention a
championship (but not the CFP / national championship) are
`CONF_CHAMP`; otherwise `season_type == "regular"` maps to `REG`
and everything else to `POST`. `result` is the home margin
(`home_points - away_points`; null when either score is missing) and
`neutral` comes from `neutral_site`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `FrameLike` |  | Output of `sportsdataverse.cfb.load_cfb_schedule` (needs `season`, `week`, `season_type`, `home_team`, `away_team`, `home_points`, `away_points`, `neutral_site` and optionally `notes`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame with columns `season`, `week`, `game_type`, `home_team`, `away_team`, `result`, `neutral`, `home_points`, `away_points` — the `cfb_standings` / `cfb_simulations` input schema. The trailing per-game points columns feed the SEC `capped_scoring_margin` official tiebreaker rung (see `CONFERENCE_TIEBREAKERS`); `cfb_standings` skips that rung when they're absent, so passing this frame straight through is always safe.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the game belongs to; consumed as the sim/season identifier by cfb_standings and cfb_simulations. |
| `week` | integer | Week of the season the game is scheduled in, passed through from the schedule frame. |
| `game_type` | character | Derived game classification - CONF_CHAMP when the schedule notes mention a (non-CFP, non-national) championship, REG for regular-season rows, POST otherwise. |
| `home_team` | character | Team name of the home team, passed through from the schedule frame. |
| `away_team` | character | Team name of the away team, passed through from the schedule frame. |
| `result` | double | Home-team margin (home_points minus away_points); null when either score is missing, marking the game as unplayed for the simulation engine. |
| `neutral` | integer | Neutral-site flag derived from the schedule's neutral_site column (1 = neutral site, 0 = home game). |
| `home_points` | double | Home team's final score, passed through from the schedule frame; null when missing (unplayed game). Feeds the SEC capped_scoring_margin official conference tiebreaker rung in cfb_standings - the rung is skipped when absent. |
| `away_points` | double | Away team's final score, passed through from the schedule frame; null when missing (unplayed game). Feeds the SEC capped_scoring_margin official conference tiebreaker rung in cfb_standings - the rung is skipped when absent. |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import (
    load_cfb_schedule, cfb_games_from_schedule, cfb_standings,
)

sched = load_cfb_schedule(seasons=2024)
games = cfb_games_from_schedule(sched)
teams = (
    sched.select(team=pl.col("home_team"), conference=pl.col("home_conference"))
    .vstack(sched.select(team=pl.col("away_team"), conference=pl.col("away_conference")))
    .unique(subset=["team"], keep="first")
)
st = cfb_standings(games, teams)
print(st.head())
```

### cfb_playoff_seeds {#cfb_playoff_seeds}

`cfb_playoff_seeds(standings: 'FrameLike', rankings: 'Optional[FrameLike]' = None, playoff_seeds: 'int' = 12, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, Any]'`

Assign College Football Playoff seeds (current straight-seeding rule).

Implements the 2025 CFP rule: the field is the `playoff_seeds` (12)
best-ranked teams with the 5 highest-ranked conference champions
guaranteed inclusion; seeds are assigned straight by ranking order (no
champion bump to the top four). The rule evolves — it lives in this ONE
function so it can be updated in one place.

When `rankings` is None the ordering falls back to the standings
tiebreaker metrics — `win_pct` desc, then `sov`, `sos`, `pd`
desc, then team name (documented deterministic fallback; a committee
ranking is the intended input).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `standings` | `FrameLike` |  | Output of `cfb_standings` (needs `sim`, `team`, `conf_champ`, `win_pct`, `sov`, `sos`, `pd`). |
| `rankings` | `Optional[FrameLike]` | `None` | Optional frame with columns `team` and `rank` (1 = best). Unranked teams order after ranked ones by the fallback. |
| `playoff_seeds` | `int` | `12` | Field size (default 12). The champion guarantee is `min(5, number of champions, playoff_seeds)`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The standings frame with a `seed` column (Int64; null for teams outside the field), sorted by sim and seed.

| col_name | type | description |
|---|---|---|
| `sim` | integer | Season or simulation identifier the standings row belongs to. |
| `team` | character | Team name (join key across the seedr engine frames). |
| `conference` | character | Conference the team belongs to; null or "FBS Independents" marks an independent. |
| `games` | integer | Total games played across all game types (regular season, conference championship and postseason). |
| `wins` | integer | Wins across all played games (conference championship and postseason included). |
| `losses` | integer | Losses across all played games. |
| `ties` | integer | Ties across all played games. |
| `win_pct` | double | Overall win percentage - (wins + 0.5 * ties) / games, 0.0 when no games have been played. |
| `pd` | double | Point differential (points for minus points against, via game margins) summed over all played games. |
| `conf_games` | integer | Number of conference regular-season games played (both teams in the same conference; CONF_CHAMP games excluded). |
| `conf_wins` | integer | Wins in conference regular-season games. |
| `conf_losses` | integer | Losses in conference regular-season games. |
| `conf_ties` | integer | Ties in conference regular-season games. |
| `conf_pct` | double | Conference win percentage - (conf_wins + 0.5 * conf_ties) / conf_games, 0.0 with no conference games; the primary sort key for conference ranks. |
| `conf_pd` | double | Point differential summed over conference regular-season games only; the POINTS-depth tiebreaker rung. |
| `sov` | double | Strength of victory, conference-REG-scoped (unlike nflseedR's overall games-weighted version) - mean of defeated conference opponents' conference win pct, one term per conference victory; 0.0 for independents or teams without conference wins. |
| `sos` | double | Strength of schedule, conference-REG-scoped (unlike nflseedR's overall games-weighted version) - mean of conference opponents' conference win pct across all conference games played; 0.0 for independents. |
| `conf_rank` | integer | Rank within the conference from the tiebreaker cascade (1 = best); null for independents. |
| `conf_champ` | logical | Whether the team is its conference's champion - the CONF_CHAMP game winner when one was played, otherwise the conference's rank-1 team; always false for independents. |
| `seed` | integer | College Football Playoff seed under the straight-seeding rule (1 = best); null for teams outside the field. The five best-ordered conference champions are guaranteed inclusion. |

**Example**

```python
from sportsdataverse.cfb import cfb_standings, cfb_playoff_seeds
st = cfb_standings(games, teams)
seeded = cfb_playoff_seeds(st, rankings=ranks_df, playoff_seeds=12)
print(seeded.filter(pl.col("seed").is_not_null()))
```

### cfb_resume {#cfb_resume}

`cfb_resume(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Rating-based résumé metrics: SoS, quality wins, game control, wins-above-bubble.

For each team, joins every played opponent to its `cfb_ratings.cfb_ratings`
strength and rolls the games up into:

- `sos` -- mean opponent `adj_net` over played games (rating-based strength
  of schedule; complements the record-based SOV/SOS in `cfb_standings`).
- `quality_wins` -- count of wins over opponents with `adj_net` at or above
  the era `quality_win_threshold`.
- `game_control` -- mean postgame win expectancy `Phi(actual_margin /
  margin_sd)`, i.e. how *dominant* the results were, not just win/loss.
- `wab` -- wins above bubble: actual wins minus the expected wins of a
  bubble-quality team (`bubble_adj_net`) playing the same schedule, using the
  Phase-2 predictors with the HFA applied on the team's actual home/away side.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season or list of seasons. |
| `as_of_date` | `date \| None` | `None` | Leakage boundary forwarded to `cfb_ratings.cfb_ratings` (ratings use only games before this date). `None` uses the full season. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per team: `season`, `team_id` (Utf8), `sos`, `sos_rank` (Int64 dense rank, best = 1), `quality_wins` (Int64), `game_control` (Float64), `wab` (Float64). Zero-row (typed) when no games are available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the résumé covers (null for a pooled multi-season fit). |
| `team_id` | character | Team ESPN id (character join key). |
| `sos` | double | Rating-based strength of schedule - mean opponent adj_net over played games. |
| `sos_rank` | integer | Dense rank on sos descending (toughest schedule = 1). |
| `quality_wins` | integer | Count of wins over opponents with adj_net at or above the era quality-win threshold. |
| `game_control` | double | Mean postgame win expectancy Phi(actual_margin / margin_sd) across played games - how dominant the results were, not just win/loss. |
| `wab` | double | Wins above bubble - actual wins minus a bubble-quality team's expected wins over the same schedule. |

**Example**

```python
from sportsdataverse.cfb.cfb_resume import cfb_resume
resume = cfb_resume(2023)
resume.sort("sos_rank").head()
```

### cfb_returning_production {#cfb_returning_production}

`cfb_returning_production(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Returning production per team-season (offense / defense / overall).

For each requested season S, computes the fraction of season S-1 unit
production attributable to players on the season-S roster (Bill Connelly's
returning-production concept; unit weights from `get_constants`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Target season or list of seasons (production is drawn from S-1). |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per `(season, team_id)`: `off_returning`, `def_returning`, `overall_returning` (Float64 fractions in [0, 1]), `n_returning` (Int64 count of returning contributors), `def_basis`, `overall_basis` (Utf8, below), `is_estimated` (Boolean). `team_id` is the ESPN team id as Utf8 -- BREAKING vs the previous release, which emitted a normalized team NAME under `team` and joined at 57.7%. Zero-row (typed) when the box data is unavailable. `is_estimated` is True for **2004 only**, where season-2003 production is parsed from CFBD play text because ESPN's player box starts in 2004. Back-tested on 2005, where both sources exist, that route tracks the box route at r=0.73 (MAE 0.11) but reads about 0.085 LOW. It ranks teams well; it is not on the same level as its neighbours, so filter on this flag before comparing 2004 against another season. 2004 also carries a null `def_returning` -- 2003 play text has no defensive ids. `def_basis` names the defensive measure: `"participants"` when the production season is 2014+ (tackles, assists, tackles for loss, shared sacks, passes defended -- 92-100% of teams), `"pbp_splash"` for 2004-2013 (sacks, interceptions, pass breakups, forced fumbles; no tackle volume, so not on the same scale), `"box"` when that source's release is missing, null with `def_returning`. `overall_basis` is `"offense+defense"`, or `"offense"` for a team with no defensive value, whose overall then equals `off_returning`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the returning fractions describe (production drawn from the prior season). |
| `team_id` | character | ESPN team id as a string (integer-origin). |
| `off_returning` | double | Fraction of prior-season attributed offensive yardage (passing + rushing + receiving) returning on the current roster. |
| `def_returning` | double | Fraction of prior-season weighted defensive production returning on the current roster; the measure is named in def_basis. |
| `overall_returning` | double | Unit fractions combined with the fitted returning_prod_weights (FBS offense 0.49 / defense 0.51, the 2018-2025 fit in fit_returning_weights.py). |
| `n_returning` | integer | Count of prior-season contributors present on the current roster. |
| `def_basis` | character | Defensive measure behind def_returning: participants (production season 2014+: tackles, assists, tackles for loss, shared sacks, passes defended), pbp_splash (2004-2013: sacks, interceptions, pass breakups, forced fumbles; no tackle volume, so not on the same scale), box (the source release was missing), or null with def_returning. |
| `overall_basis` | character | Units in overall_returning: offense+defense, or offense for a team with no defensive value, whose overall then equals off_returning. |
| `is_estimated` | logical | True for 2004 only, where season-2003 production is parsed from play text; it reads about 0.085 low against the box route. |

**Example**

```python
from sportsdataverse.cfb import cfb_returning_production
rp = cfb_returning_production(2023)
rp.sort("overall_returning", descending=True).head(10)
```

### cfb_season_odds {#cfb_season_odds}

`cfb_season_odds(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, n_sims: 'int' = 10000, playoff_seeds: 'int' = 12, seed: 'int' = 0, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Ratings-driven season Monte Carlo: conference / playoff / championship odds.

Thin wrapper over `cfb_simulations.cfb_simulations` -- it builds the ratings
with `cfb_ratings.cfb_ratings`, converts the schedule to the engine format
with `cfb_standings.cfb_games_from_schedule` (re-keyed on ESPN `team_id` so
the ratings align), and feeds `make_ratings_compute_results` as the sampler.
All season / standings / bracket machinery is reused; unplayed games are simulated,
played games (before `as_of_date`) are kept. Only FBS programs (schedule
`division == "fbs"`) enter the simulated universe; non-FBS opponents stay in the
game set -- their games still count toward FBS records -- but can never reach the
standings, the playoff field, or the output.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (an `int`, or a one-element list). Multiple seasons raise `ValueError` -- the simulation engine is single-season. |
| `as_of_date` | `date \| None` | `None` | Leakage boundary applied to BOTH the ratings vintage and the game set. Ratings are fit only on plays from games with `date < as_of_date` (`cfb_ratings.cfb_ratings`), and schedule results from `start_date` on/after `as_of_date` are masked so those games are simulated instead of replayed; masked postseason rows are dropped (the matchup is itself an outcome) and regenerated from each sim's own standings. `None` uses the full season as-is. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `playoff_seeds` | `int` | `12` | CFP field size. |
| `seed` | `int` | `0` | RNG seed for reproducibility. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per team: `season`, `team_id` (Utf8), `exp_wins`, `conf_title_prob`, `playoff_prob`, `first_round_bye_prob`, `cfp_champ_prob` (Float64 probabilities in [0, 1]). Zero-row (typed) when no ratings/schedule are available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season simulated (null for a pooled multi-season call). |
| `team_id` | character | Team ESPN id (character join key). |
| `exp_wins` | double | Mean wins per simulated season. |
| `conf_title_prob` | double | Share of simulations in which the team won its conference. |
| `playoff_prob` | double | Share of simulations in which the team made the College Football Playoff field. |
| `first_round_bye_prob` | double | Share of simulations in which the team earned a CFP first-round bye. |
| `cfp_champ_prob` | double | Share of simulations in which the team won the College Football Playoff national championship. |

**Example**

```python
from sportsdataverse.cfb.cfb_season_odds import cfb_season_odds
odds = cfb_season_odds(2023, n_sims=2000)
odds.sort("cfp_champ_prob", descending=True).head()
```

### cfb_transfer_impact {#cfb_transfer_impact}

`cfb_transfer_impact(target_season: 'int | list[int]', *, division: 'str' = 'fbs', alpha: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Net transfer talent and its projected win-total impact per team-season.

`pred_win_delta` comes from an on-demand ridge of realized win deltas on
`net_transfer_talent` fitted over strictly-prior seasons (the as-of
boundary is enforced internally per target season).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int \| list[int]` |  | Season (or list) to score. |
| `division` | `str` | `'fbs'` | Division slug for the star-points constants. |
| `alpha` | `float` | `1.0` | Ridge L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per (season, team_id): `net_transfer_talent` (Float64), `pred_win_delta` (Float64). Zero-row (typed) when no data.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the net transfer talent describes. |
| `team_id` | character | ESPN team id as a string (rosters team_id, cast Int64 to Utf8). |
| `net_transfer_talent` | double | Incoming minus outgoing transfer talent points for the season. |
| `pred_win_delta` | double | Ridge-projected win-total change from net transfer talent (as-of fit; weak observed validity - see the strict-xfail gate). |

**Example**

```python
from sportsdataverse.cfb import cfb_transfer_impact
imp = cfb_transfer_impact(2024)
imp.sort("net_transfer_talent", descending=True).head(10)
```

### cfb_transfer_moves {#cfb_transfer_moves}

`cfb_transfer_moves(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Transfer moves inferred from year-over-year roster diffs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Destination season(s) to extract moves for (each compares S-1 -> S). |
| `division` | `str` | `'fbs'` | Division slug for the star-points constants. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per move side: `season` (Int64, the destination season), `team_id` (Utf8 ESPN team id), `player_id` (Utf8 ESPN athlete id), `direction` ("in" | "out"), `prior_team_id` (Utf8 ESPN team id of the season S-1 team), `talent_points` (Float64, name-joined to the `cfb_recruits` release; the 0-star default when the player has no recruit rating). Zero-row (typed) when rosters are unavailable.

| col_name | type | description |
|---|---|---|
| `season` | integer | Destination season of the move (compares rosters S-1 to S). |
| `team_id` | character | ESPN team id (as a string) of the side this row describes (destination for "in", origin for "out"). |
| `player_id` | character | ESPN athlete id as a string. |
| `direction` | character | Move side - "in" (arriving at team_id) or "out" (leaving team_id). |
| `prior_team_id` | character | ESPN team id (as a string) of the season S-1 team. |
| `talent_points` | double | Recruit-star talent points (name-matched to the 247 recruit record; 0-star default when unrated). |

**Example**

```python
from sportsdataverse.cfb import cfb_transfer_moves
moves = cfb_transfer_moves(2024)
moves.filter(pl.col("direction") == "in").group_by("team_id").len()
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
