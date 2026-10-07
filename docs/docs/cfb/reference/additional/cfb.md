---
title: "CFB — additional Python functions — Cfb"
sidebar_label: "Cfb"
sidebar_position: 2
description: "CFB — additional Python functions — Cfb — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Cfb

### cfb_adjusted_epa {#cfb_adjusted_epa}

`cfb_adjusted_epa(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, method: "Literal['current', 'pre598']" = 'current', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season opponent-adjusted per-team EPA from a season's play-by-play.

Fits one ridge of per-play `EPA` on offense-team, defense-team, and
home-field indicators (every team shrunk toward the league average by its own
play count) over the `0.05 <= wp_before_naive <= 0.95` pass
and rush plays, nets each team's per-game raw EPA against the opponent's
fitted strength, and averages to a season figure. In-sample/descriptive (the
fit uses the whole season); for leak-free per-game values use
`cfb_adjusted_epa_by_game`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the columns listed in the module docstring. One season at a time. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty. Under `method="current"` it is per play of a full team season: each team keeps `n / (n + ridge_lambda * 577)` of its own signal for its `n` fit plays and is shrunk toward the league average by the rest (~7% at a full season, most of it on a handful of plays); must be > 0. Under `method="pre598"` it is passed unscaled to the old standardized ridge (the per-observation penalty; no 577 scaling, no positivity check). `None` (default) means 0.075 for `"current"` (the owner's choice, ADJ_EPA_LAMBDA`) and 0.035 for `"pre598"`. |
| `method` | `Literal['current', 'pre598']` | `'current'` | `"current"` (default) or `"pre598"`, the fit this function used before #598 (`0.1 <= wp_before <= 0.9` band, standardized ridge with the first team id as the reference level, lambda 0.035). pre598 reads `wp_before` instead of `wp_before_naive`. It exists for nfl-data's NFL team summaries, is not validated for NFL either, and is kept only for continuity until NFL is validated. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per team (>= 2 valid games): `team_id`, `pos_team`, `valid_games`, `adj_off_epa`, `adj_def_epa`, `off_strength_faced`, `def_strength_faced`, `net_adj_epa` and their `*_rank` columns.

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
cfb.cfb_adjusted_epa(pbp).sort("net_adj_epa_rank").head()

# NFL team summaries (the pre-#598 method; reads wp_before)

cfb.cfb_adjusted_epa(nfl_plays, method="pre598")
```

### cfb_adjusted_epa_by_game {#cfb_adjusted_epa_by_game}

`cfb_adjusted_epa_by_game(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, method: "Literal['current', 'pre598']" = 'current', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Walk-forward (point-in-time) opponent-adjusted EPA, one row per team-game.

For each week `w` the opponent-strength ridge is fit on FIT_WP`-band plays
from **weeks before `w` only**, then that week's games are adjusted with
those as-of strengths -- so the value uses no future information and is valid
as an in-season power-rating / model feature. Week 1 (no prior) yields null
adjustments; not-yet-seen opponents fall back to the league baseline (an
average team), and teams seen on few plays are shrunk most of the way there.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the module-docstring columns **plus** `week`. One season at a time. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty. Under `method="current"` it is per play of a full team season: each team keeps `n / (n + ridge_lambda * 577)` of its own signal for its `n` fit plays and is shrunk toward the league average by the rest (~7% at a full season, most of it on a handful of plays); must be > 0. Under `method="pre598"` it is passed unscaled to the old standardized ridge (the per-observation penalty; no 577 scaling, no positivity check). `None` (default) means 0.075 for `"current"` (the owner's choice, ADJ_EPA_LAMBDA`) and 0.035 for `"pre598"`. |
| `method` | `Literal['current', 'pre598']` | `'current'` | `"current"` (default) or `"pre598"`, the fit this function used before #598 (see `cfb_adjusted_epa`). pre598 also keeps the old week order: it sorts by `week` alone and does not read `seasonType`, so postseason games that restart at week 1 are fit with (and leak into) the regular season, exactly as before. It exists for nfl-data's NFL team summaries, is not validated for NFL either, and is kept only for continuity until NFL is validated. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (game, team), sorted by `week` then `team_id`: `game_id`, `week`, `team_id`, `opponent_id`, `pos_team`, `raw_off_epa`, `adj_off_epa`, `raw_def_epa`, `adj_def_epa`, `off_strength_faced` (opponent offense), `def_strength_faced` (opponent defense), `net_adj_epa`. The `adj_*` / `net` columns are null for week 1 (and any week with no prior fit).

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
tg = cfb.cfb_adjusted_epa_by_game(pbp)
tg.filter(pl.col("week") >= 5).sort("net_adj_epa", descending=True).head()
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

### cfb_odds_events_crosswalk {#cfb_odds_events_crosswalk}

`cfb_odds_events_crosswalk(season: 'Optional[int]' = None, week: 'Optional[int]' = None, *, sport: 'str' = 'americanfootball_ncaaf', api_key: 'Optional[str]' = None, season_type: 'int' = 2, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'`

Match The Odds API CFB events to ESPN game ids.

Pulls the upcoming/live events for `sport` from The Odds API and the ESPN
scoreboard for `(season, week)`, then joins them on the order-independent
team matchup so each odds event id maps to its ESPN `event` id. Because
The Odds API only lists near-term events, this is most useful for the
current/upcoming week.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | ESPN season year for the schedule side. Defaults to the most recent CFB season. |
| `week` | `Optional[int]` | `None` | ESPN schedule week. When `None`, ESPN returns its default (current) slate. |
| `sport` | `str` | `'americanfootball_ncaaf'` | The Odds API sport key. Defaults to `"americanfootball_ncaaf"`. |
| `api_key` | `Optional[str]` | `None` | The Odds API key; falls back to the `ODDS_API_KEY` env var. |
| `season_type` | `int` | `2` | ESPN season type (`2` regular, `3` post-season). Defaults to `2`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`), one row per odds event, with columns `matchup_key`, `odds_event_id`, `espn_game_id`, `home_team`, `away_team`, `commence_time`, `espn_date`, `matched_sources`.

| col_name | type | description |
|---|---|---|
| `matchup_key` | character | Order-independent key for the game: the two normalized team names sorted alphabetically and joined with a pipe (e.g. 'akron zips\|minnesota golden gophers'). Built from the Odds API home_team and away_team names. |
| `odds_event_id` | character | The Odds API event id, a 32-character lowercase hex string (e.g. 'f06e90b4212fb514f3564ded9f190107'); the id column of toa_sports_events. |
| `espn_game_id` | integer |  |
| `home_team` | character | Home team name. |
| `away_team` | character | Away team name. |
| `commence_time` | character | Scheduled kickoff of the Odds API event, an ISO-8601 UTC string with a trailing Z (e.g. '2026-09-19T16:00:00Z'), kept as text. |
| `espn_date` | character | Kickoff date as YYYY-MM-DD: the first ten characters of the matched ESPN game's UTC start timestamp; null when no ESPN game matched. |
| `matched_sources` | character | 'odds+espn' when the Odds API event matched an ESPN game on matchup_key, 'odds' when it did not; all 88 sampled rows were 'odds+espn'. |

**Example**

```python
from sportsdataverse.cfb import cfb_odds_events_crosswalk
xwalk = cfb_odds_events_crosswalk(season=2024, week=5)
matched = xwalk.filter(pl.col("espn_game_id").is_not_null())
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

### cfb_rosters_crosswalk {#cfb_rosters_crosswalk}

`cfb_rosters_crosswalk(espn_team_id: 'Union[int, str]', fox_team_id: 'Union[int, str]', *, season: 'Optional[int]' = None, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'`

Build the ESPN x Fox x Yahoo player-id crosswalk for one team.

Fetches the selected providers' players for the team, matches them on
normalized name (with jersey as a confidence signal), and returns each
player's ESPN, Fox, and Yahoo athlete ids side by side. Use
`cfb_teams_crosswalk` first to translate an ESPN team id into the
matching Fox team id.

ESPN and Fox provide full rosters, so the default is `("espn", "fox")`.
**Yahoo is opt-in** (pass `providers=("espn", "fox", "yahoo")`) because it
has no roster endpoint — its only player feed is the season stat-leaderboard
(`sportsdataverse.cfb.yahoo_cfb_player_season_stats`), which is the
league's top ~200 players (roughly one per team) and frequently includes no
player for a given team at all. When selected, the team is resolved by
matching Yahoo's (abbreviated) team name against the ESPN team's name; if it
can't be resolved, the Yahoo columns are simply null.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `espn_team_id` | `Union[int, str]` |  | ESPN team id (e.g. `194` for Ohio State). |
| `fox_team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `25` for Ohio State). |
| `season` | `Optional[int]` | `None` | Season year for the Yahoo player-stats leg. Defaults to the most recent CFB season. Unused when Yahoo isn't selected. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses `("espn", "fox")`; add `"yahoo"` explicitly for its (sparse) leg, or pass a single source. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `person_key`, `espn_athlete_id`, `fox_athlete_id`, `yahoo_athlete_id`, `name`, `espn_jersey`, `fox_jersey`, `espn_position`, `fox_position`, `yahoo_position`, `match_method`, `matched_sources`. `match_method` reflects the ESPN/Fox jersey agreement: `name_jersey` (agree), `name` (name only), `name_jersey_conflict` (jerseys differ — review), or `unmatched`.

**Example**

```python
from sportsdataverse.cfb import cfb_rosters_crosswalk
xwalk = cfb_rosters_crosswalk(espn_team_id=194, fox_team_id=25, season=2024)
matched = xwalk.filter(pl.col("matched_sources") == "espn+fox")

# Just ESPN vs Fox (skip Yahoo's partial leg)

espn_fox = cfb_rosters_crosswalk(194, 25, providers=("espn", "fox"))
```

### cfb_schedule_crosswalk {#cfb_schedule_crosswalk}

`cfb_schedule_crosswalk(season: 'int', week: 'Optional[int]' = None, *, season_type: 'int' = 2, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'`

Build the ESPN x Fox x Yahoo CFB game-id crosswalk.

Each ESPN game is keyed by its order-independent team matchup, and the Fox
and Yahoo games are mapped onto it, so each row pairs the ESPN `event` id
with the Fox Bifrost event id and the Yahoo dotted game id. Where a provider
has no game, its columns are `None` and `matched_sources` records who
contributed — so regular season, conference championships, bowls, and the
CFP all flow through the same call, degrading gracefully when a source lacks
a game.

Two modes:

* **Full season** (`week` omitted): pulls every ESPN game (regular weeks +
  bowls + CFP), Fox's full season, and Yahoo's full season, and matches on
  team **+ date** (date disambiguates rematches — a regular-season game vs a
  conference-championship or CFP rematch of the same teams).
* **Single week** (`week` given): just that week's slate, matched on team.

Each provider leg is best-effort: a Fox outage, a Yahoo per-week parser
hiccup, or Fox's offseason-projected CFP matchups simply leave that
provider's columns null rather than failing the call.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (e.g. `2024`). |
| `week` | `Optional[int]` | `None` | Schedule week number for single-week mode; omit (`None`) for the whole season. |
| `season_type` | `int` | `2` | ESPN season type for single-week mode — `2` regular, `3` post-season (`week=1` bowls, `week=999` CFP). Ignored in full-season mode. Defaults to `2`. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses all three; pass a subset for a pairwise crosswalk (e.g. `("espn", "fox")`) or a single source. Unselected providers are not fetched and surface as null columns. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `matchup_key`, `espn_game_id`, `fox_game_id`, `yahoo_game_id`, `yahoo_global_game_id`, `home_team`, `away_team`, `espn_date`, `fox_date`, `yahoo_date`, `matched_sources`.

**Example**

```python
from sportsdataverse.cfb import cfb_schedule_crosswalk
full = cfb_schedule_crosswalk(2024)
all_three = full.filter(pl.col("matched_sources") == "espn+fox+yahoo")

# Or just one week

wk5 = cfb_schedule_crosswalk(2024, 5)
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

### cfb_teams_crosswalk {#cfb_teams_crosswalk}

`cfb_teams_crosswalk(*, season: 'Optional[int]' = None, week: 'int' = 1, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'`

Build the ESPN x Fox x Yahoo CFB team-id crosswalk.

Fetches the selected provider team directories, normalizes each team name to
a shared key, and full-outer-joins them so every row carries each provider's
id, name, and abbreviation (`None` where a provider has no match). The
`matched_sources` column records which providers contributed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year used only to fetch Yahoo's embedded team directory (Yahoo has no standalone teams endpoint). Defaults to the most recent CFB season. |
| `week` | `int` | `1` | Schedule week used for the Yahoo scoreboard fetch. Defaults to `1`. The embedded directory is the full league list regardless. |
| `providers` | `Optional[Sequence[str]]` | `None` | Which sources to include — any of `"espn"`, `"fox"`, `"yahoo"`. `None` (default) uses all three; pass a subset for a pairwise crosswalk (e.g. `("espn", "fox")`) or a single source. Unselected providers are not fetched and surface as null columns. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. |

**Returns**

A polars DataFrame (pandas when `return_as_pandas=True`) with columns `norm_key`, `espn_team_id`, `espn_team`, `espn_abbreviation`, `fox_team_id`, `fox_team`, `fox_abbreviation`, `yahoo_team_id`, `yahoo_team`, `yahoo_abbreviation`, `matched_sources`.

| col_name | type | description |
|---|---|---|
| `norm_key` | character | Shared join key across providers: the team name lowercased, ASCII-folded, stripped of punctuation, whitespace-collapsed and alias-mapped (e.g. 'alabama a m bulldogs'). |
| `espn_team_id` | integer |  |
| `espn_team` | character | ESPN's full team display name, school plus mascot (e.g. 'Akron Zips'); null on rows that matched no ESPN team. |
| `espn_abbreviation` | character |  |
| `fox_team_id` | character |  |
| `fox_team` | character | Fox Sports' team name, which that feed ships in all capitals (e.g. 'AIR FORCE FALCONS'); null when no Fox team matched. |
| `fox_abbreviation` | character | Fox Sports' short team code from its teamnav directory (e.g. 'AC', 'AKRON'), which can differ from the Yahoo code for the same school ('AKRON' vs 'AKR'); null when no Fox team matched. |
| `yahoo_team_id` | character |  |
| `yahoo_team` | character | Yahoo Sports' team display name, school plus mascot (e.g. 'Akron Zips'); null when no Yahoo team matched. |
| `yahoo_abbreviation` | character | Yahoo Sports' short team code (e.g. 'ACU', 'AKR'); null when no Yahoo team matched. |
| `matched_sources` | character | Plus-joined provenance tag naming which of espn, fox and yahoo contributed a directory row for this team, e.g. 'espn+fox+yahoo', 'espn', 'fox+yahoo'. |

**Example**

```python
from sportsdataverse.cfb import cfb_teams_crosswalk
xwalk = cfb_teams_crosswalk(season=2024)
row = xwalk.filter(pl.col("espn_team_id") == 194)  # Ohio State

# Pairwise — just ESPN vs Fox

espn_fox = cfb_teams_crosswalk(providers=("espn", "fox"))
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
