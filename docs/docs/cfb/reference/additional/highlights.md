---
title: "CFB — additional Python functions — Highlights"
sidebar_label: "Highlights"
sidebar_position: 1
description: "CFB — additional Python functions — Highlights — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Highlights

### CFBPlayProcess {#CFBPlayProcess}

`CFBPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, odds_override=None, game_roster=None, participants=None, join_participants=True, **kwargs)`

Process ESPN college-football play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN `playbyplay` / `summary` endpoints (or a local JSON dump)
and pipes the result through a chain of feature-engineering steps --
down/distance, play-type flags, EPA, WPA, QBR, drive aggregation, and an
advanced box score. Use `run_processing_pipeline()` for the full feature
set or `run_cleaning_pipeline()` for a lighter clean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` |  | `0` | ESPN game id. |
| `raw` |  | `False` | if True, espn_cfb_pbp() returns the (allowlisted) summary verbatim. |
| `path_to_json` |  | `'/'` | directory for cfb_pbp_disk() offline loads. |
| `return_keys` |  | `None` | optional subset of result keys to return. |
| `odds_override` |  | `None` | optional dict {gameSpread, overUnder, homeFavorite, gameSpreadAvailable} that short-circuits odds resolution (sets odds_source="injected") so offline rebuilds never hit the live core-odds endpoint or fall back to defaults. Validated + coerced here. |
| `game_roster` |  | `None` | optional pre-fetched game roster (the list of athlete records from `~sportsdataverse.cfb.cfb_game_rosters.espn_cfb_game_rosters`, or the `{"data": [...]}` wrapper). Used by attach_player_ids` to resolve a roster-backed `{type}_player_id` for each extracted `{type}_player_name` on games that lack a structured `participants[]` array (pre-2014). Passing it makes offline rebuilds fetch-free; when omitted the live path fetches the roster on demand only if needed. |
| `participants` |  | `None` |  |
| `join_participants` |  | `True` | when True (default) the pipeline coalesces ESPN per-play participant names over the regex-extracted names and resolves a roster-backed `{type}_player_id` -- both of which hit the network (the participants/playbyplay endpoints and the game roster). Set False (`CFBPlayProcess(..., join_participants=False)`) to skip those lookups for a ~20x faster, network-free run. EPA / WPA / CPOE are unaffected (the models key on game state, not player identity); the cost is that `{type}_player_id` columns go null and names fall back to regex-from-text instead of clean ESPN displays. |

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
proc = CFBPlayProcess(gameId=401628334)
proc.espn_cfb_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])

# Offline replay from a JSON dump

proc = CFBPlayProcess(gameId=401628334, path_to_json="./pbp_dump")
proc.cfb_pbp_disk()
result = proc.run_processing_pipeline()
```

**Methods**

#### CFBPlayProcess.add_2pt_probs

`CFBPlayProcess.add_2pt_probs()`

Add the cfb4th two-point-conversion decision surface to the processed plays.

Runs `run_processing_pipeline` first if it hasn't already, then
computes the extra-point vs go-for-2 win-probability options on every
**point-after / two-point conversion** row via
`sportsdataverse.cfb.cfb_two_point.get_2pt_probs`. A row is treated
as a PAT / two-point attempt when `pointAfterAttempt.text` is present
(or the derived `extra_point_result` / `two_point_conv_result` is
non-null). The new columns -- `two_pt_wp`, `xp_wp`, `prob_2pt`,
`two_pt_recommendation` (`"go_for_2"` / `"kick_xp"`) and
`two_pt_wp_diff` (`two_pt_wp - xp_wp`, positive => go for 2) -- are
written back onto `self.plays_json` (and `self.json`'s `plays`);
every other row carries nulls.

**Returns**

`self.plays_json` as a frame with the decision columns appended (also persisted back onto the instance).

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
game.run_processing_pipeline()
out = game.add_2pt_probs()
print(out.filter(pl.col("two_pt_recommendation").is_not_null())
         .select(["two_pt_wp", "xp_wp", "two_pt_recommendation"])
         .head())
```

#### CFBPlayProcess.add_fourth_down_probs

`CFBPlayProcess.add_fourth_down_probs()`

Add the cfb4th 4th-down decision surface to the processed plays.

Runs `run_processing_pipeline` first if it hasn't already, then
computes the go / punt / field-goal win-probability options plus the
max-WP `fourth_down_recommendation` (and per-option `*_wp_diff` and
`go_boost`) on every 4th-down row via
`sportsdataverse.cfb.cfb_fourth_down.get_4th_down_probs`. The new
columns are written back onto `self.plays_json` (and `self.json`'s
`plays`); non-4th-down rows carry nulls for the decision columns.

Field-goal columns (`fg_make_prob` / `make_fg_wp` / `miss_fg_wp` /
`fg_wp`) are null when the cfb4th FG model isn't bundled
(`cfb_fourth_down.FG_MODEL_AVAILABLE` is False) -- the go + punt surface
and the recommendation over the available options are still computed.

**Returns**

`self.plays_json` as a frame with the decision columns appended (also persisted back onto the instance).

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
game.run_processing_pipeline()
fourth = game.add_fourth_down_probs()
print(fourth.filter(pl.col("start.down") == 4)
            .select(["go_wp", "punt_wp", "fg_wp", "fourth_down_recommendation"])
            .head())
```

#### CFBPlayProcess.cfb_pbp_disk

`CFBPlayProcess.cfb_pbp_disk()`

Load a previously cached ESPN summary JSON for this game from disk.

Reads `{path_to_json}/{gameId}.json` where `path_to_json` was passed
to the `CFBPlayProcess` constructor.

**Returns**

Parsed JSON contents, also stored on `self.json`.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334, path_to_json="./cache")
pbp = game.cfb_pbp_disk()
print(list(pbp.keys()))
```

#### CFBPlayProcess.cfb_pbp_json

`CFBPlayProcess.cfb_pbp_json(**kwargs)`

Return the JSON payload currently attached to this `CFBPlayProcess`

instance.

**Returns**

The cached JSON payload (`self.json`).

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
cached = game.cfb_pbp_json()
```

#### CFBPlayProcess.corrupt_pbp_check

`CFBPlayProcess.corrupt_pbp_check()`

Heuristic check for corrupt or incomplete play-by-play.

Flags games with zero plays, fewer than 50 plays for a completed game,
or more than 500 plays for a completed game -- all of which historically
indicate ESPN delivered a malformed PBP payload that should not be
processed downstream.

**Returns**

True if PBP looks corrupt and the processing pipeline should be skipped, False otherwise.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
if not game.corrupt_pbp_check():
    game.run_processing_pipeline()
```

#### CFBPlayProcess.create_box_score

`CFBPlayProcess.create_box_score(play_df)`

Build a per-team and per-player advanced box score from a processed

plays frame.

Triggers `run_processing_pipeline` first if it hasn't already run,
so the input `play_df` is expected to be the post-pipeline plays frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | The plays frame produced by `run_processing_pipeline` (with EPA, WPA and play-type flags already populated). |

**Returns**

Box-score sections, each a list of records — `"pass"` / `"rush"` / `"receiver"` (per-player advanced + EPA lines; the pass and receiver lines carry `AirYds` / `aDOT` / `CompAirYds` / `YAC` / `AirYdsPct`, null when ESPN's text has no catch spot), `"team"` and `"situational"` (per-team), `"defensive"` and `"defensive_players"` (team- and player-level havoc), `"specialists"` (kicking / punting / return players), `"turnover"`, `"drives"`, and the ESPN-sourced `"espn_team"` / `"espn_players"` totals.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
box = game.create_box_score(game.plays_json)
print(list(box.keys()))
```

#### CFBPlayProcess.create_drive_summary

`CFBPlayProcess.create_drive_summary(play_df, drives, periods=None) -> 'dict | None'`

Build the StatBroadcast-style drive summary for this game.

Thin delegate to
`sportsdataverse.cfb.cfb_drive_summary.create_drive_summary`,
with the team ids read from the plays frame -- the drive-level
sibling of `create_box_score`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | the post-pipeline plays frame (`plays_frame`). |
| `drives` | `list[dict]` |  | the ESPN drives grouping in game order. |
| `periods` |  | `None` | optional window (a set of quarter numbers, or `"ot"`). |

**Returns**

the drive summary, or `None` when inputs are unusable.

#### CFBPlayProcess.create_situational_stats

`CFBPlayProcess.create_situational_stats(play_df, window_expr=None) -> 'dict | None'`

Build the situational team-stats block for this game.

Thin delegate to
`sportsdataverse.cfb.cfb_situational_stats.create_situational_stats`,
with the team ids read from the plays frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | the post-pipeline plays frame (`plays_frame`). |
| `window_expr` |  | `None` | optional polars filter windowing the windowable sections (e.g. `pl.col("period") == 3`). |

**Returns**

the situational stats, or `None` when the frame is unusable or the window is empty.

#### CFBPlayProcess.espn_cfb_pbp

`CFBPlayProcess.espn_cfb_pbp(summary=None, **kwargs)`

espn_cfb_pbp() - Pull the game by id. Data from API endpoints: `college-football/playbyplay`,

`college-football/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `summary` | `dict, optional` | `None` | A previously fetched ESPN summary payload. When given, no request is made -- the offline path for committed raw libraries -- and the pipeline joins participants only if `participants=` was passed at construction (it never fetches them, nor a roster, for a supplied summary). |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder", "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
pbp = game.espn_cfb_pbp()
print(list(pbp.keys()))

# Pull only the raw ESPN summary payload (skip cleaning)

raw_pbp = CFBPlayProcess(gameId=401628334, raw=True).espn_cfb_pbp()

# Pipeline next step (run the full processing pipeline for advanced features)

game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()  # adds EPA, WPA, box score
```

#### CFBPlayProcess.run_cleaning_pipeline

`CFBPlayProcess.run_cleaning_pipeline()`

Run the lighter cleaning pipeline (no EPA/WPA/QBR/box-score).

Same per-play feature engineering as `run_processing_pipeline`
through add_spread_time`, but stops short of the modeling steps.
Use this when you only need cleaned plays and don't need expected
points or win probability columns.

**Returns**

Cleaned game payload (no `advBoxScore` key).

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
cleaned = game.run_cleaning_pipeline()
print(len(cleaned["plays"]))
```

#### CFBPlayProcess.run_processing_pipeline

`CFBPlayProcess.run_processing_pipeline(fourth_down_probs: 'bool' = True, two_pt_probs: 'bool' = True, validate: 'bool' = False)`

Run the full play-by-play processing pipeline.

Applies every scoring/feature step in order: down detection, play type
flags, rush/pass flags, team score variables, new play types, penalty
setup, play category flags, yardage cols, player cols, after cols,
spread time, EPA, WPA, drive data, and QBR. Also produces an advanced
box score and stores it under `advBoxScore` on the returned dict.

Idempotent -- subsequent calls return the cached `self.json`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fourth_down_probs` | `bool` | `True` | when True (default), run the cfb4th decision surface (`sportsdataverse.cfb.cfb_fourth_down.get_4th_down_probs`) on the enriched frame and append the go/field-goal/punt WP columns plus the `fourth_down_recommendation` to 4th-down plays (null elsewhere). Pass False to skip it (e.g. to avoid loading the fourth-down model). |
| `two_pt_probs` | `bool` | `True` | when True (default), run the cfb4th two-point decision surface (`sportsdataverse.cfb.cfb_two_point.get_2pt_probs`) and append `two_pt_wp` / `xp_wp` / `prob_2pt` / `two_pt_recommendation` / `two_pt_wp_diff` to point-after / two-point rows (null elsewhere). |
| `validate` | `bool` | `False` | when True, score the processed frame with the packaged per-game gate (`sportsdataverse.validation`) and attach its report dict under the `"validation"` key of the processed game (`{}` when the pipeline produced no plays). Name `"validation"` in `return_keys` to get it back when a subset was requested. Off by default -- the gate costs a few milliseconds and most callers do not read it. |

**Returns**

The fully-processed game payload. If the constructor was given `return_keys`, only those keys are returned.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
print(processed["advBoxScore"].keys())

# Pipeline next step (return only selected keys)

game = CFBPlayProcess(gameId=401628334, return_keys=["plays", "advBoxScore"])
game.espn_cfb_pbp()
trimmed = game.run_processing_pipeline()
```

### cfb_advanced_stats {#cfb_advanced_stats}

`cfb_advanced_stats(seasons: 'Union[int, list[int]]', *, adjust: 'bool' = True, exclude_garbage: 'bool' = True, as_of_date: 'Optional[datetime.date]' = None, config: 'Optional[AdjustConfig]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team-season CFB advanced stats: efficiency, explosiveness, havoc.

Loads play-by-play via `load_cfb_pbp`, builds the garbage-filtered
per-play long frame, aggregates raw per-team offense/defense success
rate, EPA/play, isoPPP (mean EPA on successful plays), explosive rate
and havoc, and (default) opponent-adjusts each metric with the
iterative solver.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | season or list of seasons (hosted pbp covers 2002-2021). |
| `adjust` | `bool` | `True` | add `adj_*` opponent-adjusted columns + EPA ranks. |
| `exclude_garbage` | `bool` | `True` | drop Connelly garbage-time plays. |
| `as_of_date` | `Optional[date]` | `None` | leakage boundary -- only plays strictly before this date contribute. |
| `config` | `Optional[AdjustConfig]` | `None` | `AdjustConfig` for the solver. |
| `return_as_pandas` | `bool` | `False` | return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (season, team_id) with the raw columns (and `adj_*` plus `off_epa_rank`/`def_epa_rank` when `adjust=True`). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the stats cover. |
| `team_id` | character | Team ESPN id (character join key). |
| `plays` | integer | Situation-neutral offensive plays in the aggregate. |
| `off_success_rate` | double | Offensive success rate (yards gained >= 50/70/100 percent of distance by down). |
| `def_success_rate` | double | Success rate allowed (Connelly 50/70/100 yardage rule). |
| `off_epa_play` | double | Raw offensive EPA per play on the garbage-filtered substrate (garbage time excluded by default; pass exclude_garbage=False to keep it). |
| `def_epa_play` | double | Raw EPA allowed per play on the garbage-filtered substrate (garbage time excluded by default; pass exclude_garbage=False to keep it). |
| `off_iso_ppp` | double | Mean EPA on successful offensive plays (Connelly isoPPP explosiveness). |
| `def_iso_ppp` | double | Mean EPA allowed on successful plays faced (isoPPP against). |
| `off_explosive_rate` | double | Share of offensive plays that were explosive (pass EPA >= 2.4, rush EPA >= 1.8). |
| `def_explosive_rate` | double | Share of plays faced that were explosive (pass EPA >= 2.4, rush EPA >= 1.8). |
| `def_havoc` | double | Share of plays faced with a havoc event (TFL, pass breakup, interception, forced fumble). |
| `off_havoc_allowed` | double | Share of offensive plays on which the defense recorded a havoc event. |
| `off_epa_success_rate` | double | Share of offensive plays with EPA > 0 (EPA-based success rate). |
| `adj_off_epa_play` | double | Opponent-adjusted offensive EPA per play (higher is better). |
| `adj_off_success_rate` | double | Opponent-adjusted offensive success rate. |
| `adj_off_explosive_rate` | double | Opponent-adjusted offensive explosive-play rate. |
| `adj_def_epa_play` | double | Opponent-adjusted EPA allowed per play (lower is better). |
| `adj_def_success_rate` | double | Opponent-adjusted success rate allowed. |
| `adj_def_explosive_rate` | double | Opponent-adjusted explosive-play rate allowed. |
| `adj_def_havoc` | double | Opponent-adjusted havoc rate created by the defense. |
| `adj_off_havoc_allowed` | double | Opponent-adjusted havoc rate the offense allows. |
| `off_epa_rank` | integer | Dense rank on adj_off_epa_play descending (best offense = 1). |
| `def_epa_rank` | integer | Dense rank on adj_def_epa_play ascending (fewest EPA allowed = 1). |

**Example**

```python
from sportsdataverse.cfb import cfb_advanced_stats
df = cfb_advanced_stats([2021])
print(df.shape)

# Raw only, garbage time kept

df_raw = cfb_advanced_stats(2021, adjust=False, exclude_garbage=False)

# Pipeline next step (one line)

df.sort("adj_off_epa_play", descending=True).head()
```

### cfb_standings {#cfb_standings}

`cfb_standings(games: 'FrameLike', teams: 'FrameLike', *, tiebreaker_depth: 'str' = 'SOS', playoff_seeds: 'Optional[int]' = None, rankings: 'Optional[FrameLike]' = None, tiebreaker_data: 'Optional[Dict[str, FrameLike]]' = None, return_as_pandas: 'bool' = False, rng: 'Optional[np.random.Generator]' = None) -> 'Union[pl.DataFrame, Any]'`

Compute college football standings with conference ranks and champions.

Engine design adapted from nflseedR (MIT, Sebastian Carl & Lee Sharpe);
see the module docstring for the documented CFB simplifications, and its
"Official per-conference tiebreakers (registry)" section for how
`CONFERENCE_TIEBREAKERS` overrides the generic cascade for the SEC,
Big Ten, Big 12, ACC and MAC.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `FrameLike` |  | Game results with columns `sim` (or `season`), `week`, `game_type` (`REG` \| `CONF_CHAMP` \| `POST`), `home_team`, `away_team`, `result` (home margin: home - away; null = unplayed), optional `neutral` (0/1), and optional `home_points`/`away_points` (per-game scores — feeds the SEC capped-scoring-margin rung; `cfb_games_from_schedule` emits both). Either optional input absent -> that rung is skipped, not an error. |
| `teams` | `FrameLike` |  | Team table with columns `team` and `conference` (null or `"FBS Independents"` marks an independent), and an optional `division` column (`"FBS"`/`"FCS"` or similar — feeds the Big 12 `total_wins` FCS cap; absent -> the cap degrades to uncapped win totals, noted in `tiebreak_notes`). |
| `tiebreaker_depth` | `str` | `'SOS'` | One of `"RANDOM"`, `"PRE-SOV"`, `"SOS"`, `"POINTS"` — the nflseedR depth ladder. Steps beyond the chosen depth are skipped and remaining ties are broken by coin flip. Gates ONLY the generic fallback cascade; registered official conference procedures (below) always run in full. |
| `playoff_seeds` | `Optional[int]` | `None` | If set, adds a `seed` column via `cfb_playoff_seeds` with this field size. |
| `rankings` | `Optional[FrameLike]` | `None` | Optional committee-style rankings frame (`team`, `rank`) forwarded to `cfb_playoff_seeds`. |
| `tiebreaker_data` | `Optional[Dict[str, FrameLike]]` | `None` | Optional external inputs for the registry rungs, as a dict with key `"analytics_ratings"` -> a frame with columns `team` and `rating` (feeds the `analytics_rating` rung used by Big Ten/Big 12/ACC/MAC). A `"cfp_rankings"` key (`team`, `rank`) is accepted for forward compatibility but unused by the current registry (no registered conference has a `cfp_ranking` rung yet). Missing -> the rung is skipped, noted. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |
| `rng` | `Optional[Generator]` | `None` | Optional numpy Generator used only for coin-flip tiebreaks (simulations pass their seeded generator through here). |

**Returns**

A polars (or pandas) DataFrame with one row per (sim, team): overall record (`games`/`wins`/`losses`/`ties`/`win_pct`/ `pd`), conference record (`conf_*`), `sov`, `sos`, `conf_rank` (null for independents), `conf_champ` and, when `playoff_seeds` is set, `seed`. The result also carries a `tiebreak_notes` list of skipped-rung messages (see the module docstring): `result.tiebreak_notes` for a polars frame, `result.attrs["tiebreak_notes"]` for a pandas frame (pandas' own metadata mechanism — avoids its "new attribute" warning).

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

**Example**

```python
import polars as pl
from sportsdataverse.cfb import cfb_standings

games = pl.DataFrame({
    "sim": [2024, 2024], "week": [1, 2],
    "game_type": ["REG", "REG"],
    "home_team": ["A", "B"], "away_team": ["B", "A"],
    "result": [7.0, -3.0], "neutral": [0, 0],
})
teams = pl.DataFrame({"team": ["A", "B"], "conference": ["X", "X"]})
print(cfb_standings(games, teams))

# With CFP seeds from committee rankings

st = cfb_standings(games, teams, playoff_seeds=12, rankings=ranks_df)

# With an official-registry analytics rating input

ratings = pl.DataFrame({"team": ["A", "B"], "rating": [92.1, 88.4]})
st = cfb_standings(games, teams, tiebreaker_data={"analytics_ratings": ratings})
print(st.tiebreak_notes)
```

### espn_cfb_player_stats {#espn_cfb_player_stats}

`espn_cfb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull a college-football athlete's ESPN **season** stat line.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns (for football: `passing_*`, `rushing_*`, `receiving_*`,
`scoring_*`, ...), the athlete / team metadata blocks, and the
`season_type` / `total` parameters. For the richer multi-category
web-v3 payload use `sportsdataverse.cfb.espn_cfb_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN college-football athlete identifier. |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season (4-digit year). |
| `season_type` | character | ESPN season type (2 = regular, 3 = postseason). |
| `total` | logical |  |
| `athlete_id` | integer | ESPN athlete id. |
| `athlete_uid` | character |  |
| `athlete_guid` | character |  |
| `athlete_type` | character |  |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `display_name` | character | Human-readable metric name. |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `weight` | double | Listed weight (lbs). |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `height` | double | Listed height (inches). |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `age` | integer |  |
| `date_of_birth` | character | Player date of birth (if published). |
| `jersey` | character | Jersey number. |
| `slug` | character | URL slug for the team. |
| `active` | logical | `TRUE` if the player was active for the game. |
| `position_id` | integer | ESPN position id. |
| `position_name` | character | Position name (e.g. `Quarterback`). |
| `position_display_name` | character | Human-readable position name. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`). |
| `college_name` | character |  |
| `status_id` | integer | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `general_fumbles` | double | Total number of fumbles committed by the player across all offensive and special-teams plays. |
| `general_fumbles_lost` | double | Number of fumbles the player committed that were recovered by the opposing team. |
| `general_fumbles_touchdowns` | double | Total touchdowns scored by the player as a result of fumble recoveries, combining offensive and defensive occurrences. |
| `general_games_played` | double |  |
| `general_offensive_two_pt_returns` | double | Number of two-point conversions the player scored by returning a blocked or intercepted two-point attempt on the offensive side. |
| `general_offensive_fumbles_touchdowns` | double | Number of touchdowns scored by the player on fumble recoveries credited to the offensive category. |
| `general_defensive_fumbles_touchdowns` | double | Number of touchdowns scored by the player on fumble recoveries attributed to the defensive category. |
| `passing_avg_gain` | double | Average yards gained per passing play attempt by the quarterback in the passing category. |
| `passing_completion_pct` | double | Percentage of pass attempts thrown by the quarterback that were completed, calculated as completions divided by attempts. |
| `passing_completions` | double | Pass completions (split from CFBD's `C/ATT` field). |
| `passing_espnqb_rating` | double | ESPN's proprietary quarterback rating for the player's passing performance, factoring in efficiency metrics beyond traditional passer rating. |
| `passing_interception_pct` | double | Percentage of pass attempts that resulted in an interception, calculated as interceptions divided by passing attempts. |
| `passing_interceptions` | double | Total number of passes thrown by the quarterback that were intercepted by the defense. |
| `passing_long_passing` | double | Longest single completed pass in yards recorded by the quarterback during the stat period. |
| `passing_net_passing_yards` | double | Net passing yards gained by the quarterback after subtracting yardage lost on sacks from gross passing yards. |
| `passing_net_passing_yards_per_game` | double | Net passing yards per game for the quarterback, computed as net passing yards divided by games played. |
| `passing_net_total_yards` | double | Combined net yardage from passing and rushing for a quarterback, accounting for sack yardage lost in the passing category. |
| `passing_net_yards_per_game` | double | Net total yards gained per game for the player as recorded in the passing category context. |
| `passing_passing_attempts` | double | Total number of pass attempts thrown by the quarterback, including completions, incompletions, and interceptions. |
| `passing_passing_big_plays` | double | Number of passing plays that gained 20 or more yards as recorded for the quarterback. |
| `passing_passing_first_downs` | double | Number of first downs gained by the team on passing plays thrown by the quarterback. |
| `passing_passing_fumbles` | double | Number of fumbles the quarterback committed during passing plays, including fumbled snaps and sack fumbles. |
| `passing_passing_fumbles_lost` | double | Number of fumbles the quarterback committed on passing plays that were recovered by the opposing team. |
| `passing_passing_touchdown_pct` | double | Percentage of pass attempts that resulted in a passing touchdown, calculated as touchdowns divided by attempts. |
| `passing_passing_touchdowns` | double | Total number of touchdown passes thrown by the quarterback. |
| `passing_passing_yards` | double | Gross passing yards gained by the quarterback on completed passes. |
| `passing_passing_yards_after_catch` | double | Total yards gained by receivers after the catch on passes thrown by the quarterback. |
| `passing_passing_yards_at_catch` | double | Total yards gained at the point of the catch (air yards) on passes thrown by the quarterback, before any yards after catch. |
| `passing_passing_yards_per_game` | double | Gross passing yards per game for the quarterback, computed as passing yards divided by games played. |
| `passing_qb_rating` | double | Traditional NCAA passer rating for the quarterback, calculated from completion percentage, yards per attempt, touchdown rate, and interception rate. |
| `passing_sacks` | double | Total number of times the quarterback was sacked (tackled behind the line of scrimmage on a passing play). |
| `passing_sack_yards_lost` | double | Total yards lost by the quarterback as a result of being sacked, subtracted when computing net passing yards. |
| `passing_team_games_played` | double | Number of team games played during the stat period, used as the denominator for per-game passing rate statistics. |
| `passing_total_offensive_plays` | double | Total number of offensive plays (pass attempts plus rushes) for the team during the stat period, recorded in the passing category context. |
| `passing_total_points_per_game` | double | Average total points scored per game by the player's team as recorded alongside passing statistics. |
| `passing_total_touchdowns` | double | Total touchdowns accounted for by the quarterback across passing and rushing in the passing category context. |
| `passing_total_yards` | double | Total offensive yardage (passing plus rushing) accumulated by the quarterback as reported in the passing category. |
| `passing_total_yards_from_scrimmage` | double | Total yards from scrimmage accumulated by the quarterback (passing plus rushing yards) in the passing category context. |
| `passing_two_point_pass_convs` | double | Number of successful two-point conversions the quarterback converted via a passing play. |
| `passing_two_pt_pass` | double | Indicator or count of two-point conversion passing attempts recorded for the quarterback. |
| `passing_two_pt_pass_attempts` | double | Total number of two-point conversion attempts the quarterback made via a passing play. |
| `passing_yards_from_scrimmage_per_game` | double | Average yards from scrimmage per game for the quarterback as reported in the passing category. |
| `passing_yards_per_completion` | double | Average yards gained per completed pass by the quarterback, calculated as passing yards divided by completions. |
| `passing_yards_per_game` | double | Average gross passing yards per game for the quarterback, equivalent to passing_passing_yards_per_game. |
| `passing_yards_per_pass_attempt` | double | Average yards gained per pass attempt by the quarterback, calculated as passing yards divided by attempts. |
| `passing_net_yards_per_pass_attempt` | double | Net passing yards divided by total pass attempts, including sack yardage lost in the denominator's context. |
| `passing_qbr` | double | ESPN Quarterback Rating (QBR) for the player in this game. |
| `passing_adj_qbr` | double | ESPN's adjusted Total Quarterback Rating (QBR) for the player's passing performance, controlling for opponent difficulty and game situation. |
| `passing_quarterback_rating` | double | Traditional passer rating for the quarterback, equivalent to passing_qb_rating, using the standard NCAA formula. |
| `passing_offensive_snap_pct` | double | ESPN's offensiveSnapPct stat (described upstream as '% of plays the player was on the field'); 0.0 for every college player checked, so ESPN does not appear to populate it for college football. |
| `passing_target_share_pct` | double | ESPN's targetSharePct stat (described upstream as '% of total team targets'); 0.0 for every college player checked, so ESPN does not appear to populate it for college football. |
| `passing_yards_per_route_run` | double | ESPN's yardsPerRouteRun stat (yards per route run, YPRR) under the passing category; 0.0 for every college player checked, so ESPN does not appear to populate it for college football. |
| `passing_avg_depth_of_target` | double | ESPN's avgDepthOfTarget stat (average depth of target, aDOT) under the passing category; 0.0 for every college player checked, so ESPN does not appear to populate it for college football. |
| `rushing_avg_gain` | double | Average yards gained per rushing attempt for the player in the rushing category. |
| `rushing_espnrb_rating` | double | ESPN's proprietary running back rating for the player's rushing performance. |
| `rushing_long_rushing` | double | Longest single rushing carry in yards recorded by the player during the stat period. |
| `rushing_net_total_yards` | double | Net total yardage accumulated by the player from rushing and any receiving contributions as reported in the rushing category. |
| `rushing_net_yards_per_game` | double | Net total yards per game for the player as reported in the rushing category context. |
| `rushing_rushing_attempts` | double | Total number of rushing attempts (carries) credited to the player. |
| `rushing_rushing_big_plays` | double | Number of rushing plays that gained 10 or more yards for the player. |
| `rushing_rushing_first_downs` | double | Number of first downs gained by the player via rushing plays. |
| `rushing_rushing_fumbles` | double | Number of fumbles the player committed on rushing plays. |
| `rushing_rushing_fumbles_lost` | double | Number of fumbles the player committed on rushing plays that were recovered by the opposing team. |
| `rushing_rushing_touchdowns` | double | Total number of rushing touchdowns scored by the player. |
| `rushing_rushing_yards` | double | Total yards gained by the player on rushing attempts. |
| `rushing_rushing_yards_per_game` | double | Average rushing yards per game for the player, calculated as rushing yards divided by games played. |
| `rushing_stuffs` | double | Number of rushing attempts in which the player was stopped at or behind the line of scrimmage. |
| `rushing_stuff_yards_lost` | double | Total yards lost by the player on stuffed rushing plays (carries stopped at or behind the line of scrimmage). |
| `rushing_team_games_played` | double | Number of team games played during the stat period, used as the denominator for per-game rushing rate statistics. |
| `rushing_total_offensive_plays` | double | Total number of offensive plays for the team during the stat period, recorded in the rushing category context. |
| `rushing_total_points_per_game` | double | Average total points scored per game by the player's team as recorded alongside rushing statistics. |
| `rushing_total_touchdowns` | double | Total touchdowns scored by the player across all methods as reported in the rushing category context. |
| `rushing_total_yards` | double | Total offensive yardage accumulated by the player as reported in the rushing category. |
| `rushing_total_yards_from_scrimmage` | double | Total yards from scrimmage for the player (rushing plus receiving yards) as reported in the rushing category. |
| `rushing_two_point_rush_convs` | double | Number of successful two-point conversions the player converted via a rushing play. |
| `rushing_two_pt_rush` | double | Indicator or count of two-point conversion rushing attempts recorded for the player. |
| `rushing_two_pt_rush_attempts` | double | Total number of two-point conversion attempts the player made via a rushing play. |
| `rushing_yards_from_scrimmage_per_game` | double | Average yards from scrimmage per game for the player as reported in the rushing category. |
| `rushing_yards_per_game` | double | Average rushing yards per game for the player, equivalent to rushing_rushing_yards_per_game. |
| `rushing_yards_per_rush_attempt` | double | Average yards gained per rushing attempt for the player, calculated as rushing yards divided by attempts. |
| `receiving_avg_gain` | double | Average yards gained per reception for the player in the receiving category. |
| `receiving_espnwr_rating` | double | ESPN's proprietary wide receiver / pass-catcher rating for the player's receiving performance. |
| `receiving_long_reception` | double | Longest single reception in yards recorded by the player during the stat period. |
| `receiving_net_total_yards` | double | Net total yardage accumulated by the player from receiving and any rushing contributions as reported in the receiving category. |
| `receiving_net_yards_per_game` | double | Net total yards per game for the player as reported in the receiving category context. |
| `receiving_receiving_big_plays` | double | Number of receiving plays that gained 20 or more yards for the player. |
| `receiving_receiving_first_downs` | double | Number of first downs gained by the player via receptions. |
| `receiving_receiving_fumbles` | double | Number of fumbles the player committed after catching a pass. |
| `receiving_receiving_fumbles_lost` | double | Number of fumbles the player committed on receiving plays that were recovered by the opposing team. |
| `receiving_receiving_targets` | double | Total number of times the player was targeted as the intended receiver on a pass play. |
| `receiving_receiving_touchdowns` | double | Total number of touchdown receptions scored by the player. |
| `receiving_receiving_yards` | double | Total yards gained by the player on completed receptions. |
| `receiving_receiving_yards_after_catch` | double | Total yards gained by the player after the catch on receiving plays. |
| `receiving_receiving_yards_at_catch` | double | Total air yards gained at the point of the catch on receiving plays, before any yards after catch. |
| `receiving_receiving_yards_per_game` | double | Average receiving yards per game for the player, calculated as receiving yards divided by games played. |
| `receiving_receptions` | double | Total number of completed receptions (catches) recorded by the player. |
| `receiving_team_games_played` | double | Number of team games played during the stat period, used as the denominator for per-game receiving rate statistics. |
| `receiving_total_offensive_plays` | double | Total number of offensive plays for the team during the stat period, recorded in the receiving category context. |
| `receiving_total_points_per_game` | double | Average total points scored per game by the player's team as recorded alongside receiving statistics. |
| `receiving_total_touchdowns` | double | Total touchdowns scored by the player across all methods as reported in the receiving category context. |
| `receiving_total_yards` | double | Total offensive yardage accumulated by the player as reported in the receiving category. |
| `receiving_total_yards_from_scrimmage` | double | Total yards from scrimmage for the player (receiving plus rushing yards) as reported in the receiving category. |
| `receiving_two_point_rec_convs` | double | Number of successful two-point conversions the player converted via a reception. |
| `receiving_two_pt_reception` | double | Indicator or count of two-point conversion receptions recorded for the player. |
| `receiving_two_pt_reception_attempts` | double | Total number of two-point conversion attempts the player made via a receiving play. |
| `receiving_yards_from_scrimmage_per_game` | double | Average yards from scrimmage per game for the player as reported in the receiving category. |
| `receiving_yards_per_game` | double | Average receiving yards per game for the player, equivalent to receiving_receiving_yards_per_game. |
| `receiving_yards_per_reception` | double | Average yards gained per reception for the player, calculated as receiving yards divided by receptions. |
| `scoring_defensive_points` | double | Total points scored by the player through defensive plays such as defensive touchdowns, safeties, or fumble-return scores. |
| `scoring_field_goals` | double | Total number of field goals made by the player in the scoring category. |
| `scoring_kick_extra_points` | double | Total number of extra point attempts kicked by the player. |
| `scoring_kick_extra_points_made` | double | Total number of successful extra points (PATs) kicked by the player. |
| `scoring_misc_points` | double | Points scored by the player through miscellaneous means not captured by standard scoring categories. |
| `scoring_passing_touchdowns` | double | Total touchdown passes thrown by the player as counted in the scoring category. |
| `scoring_receiving_touchdowns` | double | Total touchdown receptions scored by the player as counted in the scoring category. |
| `scoring_return_touchdowns` | double | Total touchdowns scored by the player on kick or punt returns as counted in the scoring category. |
| `scoring_rushing_touchdowns` | double | Total rushing touchdowns scored by the player as counted in the scoring category. |
| `scoring_total_points` | double | Total points scored by the player across all scoring methods during the stat period. |
| `scoring_total_points_per_game` | double | Average total points scored by the player per game during the stat period. |
| `scoring_total_touchdowns` | double | Total touchdowns scored by the player across all methods (passing, rushing, receiving, and return) in the scoring category. |
| `scoring_total_two_point_convs` | double | Total number of successful two-point conversions scored by the player across passing, rushing, and receiving attempts. |
| `scoring_two_point_pass_convs` | double | Number of successful two-point conversions the player scored via a passing play, as counted in the scoring category. |
| `scoring_two_point_rec_convs` | double | Number of successful two-point conversions the player scored via a reception, as counted in the scoring category. |
| `scoring_two_point_rush_convs` | double | Number of successful two-point conversions the player scored via a rushing play, as counted in the scoring category. |
| `scoring_one_pt_safeties_made` | double | Number of one-point safeties scored by the player's team, credited in the scoring category. |
| `team_id` | integer | ESPN team id. |
| `team_uid` | character |  |
| `team_guid` | character |  |
| `team_slug` | character | Team slug for the stat row. |
| `team_location` | character | Team location / school name. |
| `team_name` | character | Team nickname. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Full team display name. |
| `team_short_display_name` | character | Short team display name. |
| `team_color` | character | Primary team color. |
| `team_alternate_color` | character | Alternate team color. |
| `team_is_active` | logical |  |
| `team_logo_href` | character | Default team logo URL. |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_player_stats
df = espn_cfb_player_stats(athlete_id=4426338, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### espn_cfb_schedule {#espn_cfb_schedule}

`espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_cfb_schedule - look up the college football schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `week` | `int` | `None` | Week of the schedule. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `groups` | `int` | `None` | Used to define different divisions. 80 is FBS, 81 is FCS. |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. Returns None if no games

| col_name | type | description |
|---|---|---|
| `id` | character | 247Sports referencing id for the recruit. |
| `uid` | character | ESPN global unique identifier. |
| `date` | character | Date of the poll release. |
| `attendance` | integer | Reported attendance at the game. |
| `time_valid` | logical |  |
| `date_valid` | logical | Boolean flag indicating whether the game's scheduled date is confirmed and valid. |
| `neutral_site` | logical | TRUE/FALSE flag for if the game took place at a neutral site. |
| `conference_competition` | logical |  |
| `play_by_play_available` | logical |  |
| `recent` | logical |  |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |
| `broadcast` | character | Broadcast network short name. |
| `highlights` | integer | Game highlight urls. |
| `notes_type` | character |  |
| `notes_headline` | character |  |
| `broadcast_market` | character |  |
| `broadcast_name` | character |  |
| `type_id` | character | Play-type id. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `venue_id` | character | Referencing venue id. |
| `venue_full_name` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_address_country` | character | Country in which the game venue is located, as provided by ESPN's venue data. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `status_clock` | double |  |
| `status_display_clock` | character |  |
| `status_period` | integer |  |
| `status_type_id` | character |  |
| `status_type_name` | character |  |
| `status_type_state` | character |  |
| `status_type_completed` | logical |  |
| `status_type_description` | character |  |
| `status_type_detail` | character |  |
| `status_type_short_detail` | character |  |
| `groups_id` | character |  |
| `groups_name` | character |  |
| `groups_short_name` | character |  |
| `groups_is_conference` | logical |  |
| `format_regulation_periods` | integer |  |
| `home_id` | character | Home team referencing id. |
| `home_uid` | character |  |
| `home_location` | character |  |
| `home_name` | character |  |
| `home_abbreviation` | character |  |
| `home_display_name` | character |  |
| `home_short_display_name` | character |  |
| `home_color` | character |  |
| `home_alternate_color` | character |  |
| `home_is_active` | logical |  |
| `home_venue_id` | character |  |
| `home_logo` | character |  |
| `home_conference_id` | character |  |
| `home_score` | character | Home-team score after the play. |
| `home_current_rank` | integer | AP or Coaches Poll ranking of the home team at the time of the game (null if unranked). |
| `home_linescores` | list | Per-period point totals for the home team, stored as an array of quarter/overtime scores. |
| `home_records` | character | Win-loss record of the home team at the time of the game, as reported by ESPN (e.g., overall or conference record). |
| `away_id` | character | Away team referencing id. |
| `away_uid` | character |  |
| `away_location` | character |  |
| `away_name` | character |  |
| `away_abbreviation` | character |  |
| `away_display_name` | character |  |
| `away_short_display_name` | character |  |
| `away_color` | character |  |
| `away_alternate_color` | character |  |
| `away_is_active` | logical |  |
| `away_venue_id` | character |  |
| `away_logo` | character |  |
| `away_conference_id` | character |  |
| `away_score` | character | Away-team score after the play. |
| `away_current_rank` | integer | AP or Coaches Poll ranking of the away team at the time of the game (null if unranked). |
| `away_linescores` | list | Per-period point totals for the away team, stored as an array of quarter/overtime scores. |
| `away_records` | character | Win-loss record of the away team at the time of the game, as reported by ESPN (e.g., overall or conference record). |
| `game_id` | integer | ESPN game identifier. |
| `season` | integer | Season (4-digit year). |
| `season_type` | integer | ESPN season type (2 = regular, 3 = postseason). |
| `week` | integer | Game week of the season. |
| `home_logo_dark` | character |  |
| `away_logo_dark` | character |  |
| `home_winner` | logical |  |
| `away_winner` | logical |  |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_schedule
slate = espn_cfb_schedule()
print(slate.shape if slate is not None else "no games")

# Pull a specific week of FBS games

week5 = espn_cfb_schedule(dates=2023, week=5, season_type=2)

# Pipeline next step (extract finals only)

import polars as pl
finals = espn_cfb_schedule(dates=2023, week=5).filter(
    pl.col("status_type_completed") == True
)
```

### get_cfb_teams {#get_cfb_teams}

`get_cfb_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load college football team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `school` | character | Team name. |
| `mascot` | character | Team mascot. |
| `abbreviation` | character | Metric abbreviation. |
| `alt_name1` | character | Team alternate name 1 (as it appears in `play_text`). |
| `alt_name2` | character | Team alternate name 2 (as it appears in `play_text`). |
| `alt_name3` | character | Team alternate name 3 (as it appears in `play_text`). |
| `conference` | character | Conference of the team. |
| `division` | character | Division in the conference for the team. |
| `color` | character | Primary team color (hex, no `#`). |
| `alt_color` | character | Team color (alternate). |
| `logo` | character |  |
| `logo_dark` | character |  |

**Example**

```python
from sportsdataverse.cfb import get_cfb_teams
teams = get_cfb_teams()
print(teams.shape)

# Pandas round-trip

teams_pd = get_cfb_teams(return_as_pandas=True)
teams_pd.head()

# Pipeline next step (build a team_id to logo URL map)

teams = get_cfb_teams()
logo_map = dict(zip(teams["team_id"], teams["logo"]))
```

### most_recent_cfb_season {#most_recent_cfb_season}

`most_recent_cfb_season()`

Return the most recent college football season year based on today's date.

The college football season starts in mid-August. If today is on or after
August 15 (or any day in September or later), this returns the current
calendar year. Otherwise, it returns the previous calendar year.

**Returns**

The most recent CFB season year.

**Example**

```python
from sportsdataverse.cfb import most_recent_cfb_season
year = most_recent_cfb_season()
print(year)

# Combine with the loaders for a "current season" pull

from sportsdataverse.cfb import load_cfb_schedule, most_recent_cfb_season
sched = load_cfb_schedule(seasons=[most_recent_cfb_season()])
```

### to_cfbfastr {#to_cfbfastr}

`to_cfbfastr(pbp: 'pl.DataFrame', *, season: "'Optional[int]'" = None, week: "'Optional[int]'" = None, drives: "'Optional[pl.DataFrame]'" = None, linescore: "'Optional[pl.DataFrame]'" = None, drive_titles: "'Optional[pl.DataFrame]'" = None, ot_drives: "'Optional[pl.DataFrame]'" = None, scoring_summary: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

cfbfastR-named play frame from the NCAA structural pbp frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Output of `sportsdataverse.cfb.cfb_ncaa_pbp.parse_cfb_ncaa_pbp` (one game). |
| `season` | `Optional[int]` | `None` | Season year (2025 = fall-2025), written to `season`/`year`. |
| `week` | `Optional[int]` | `None` | Optional week number (from the schedule master). |
| `drives` | `Optional[DataFrame]` | `None` | Optional `sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_drives` frame -- refines `period` per drive when quarter markers are missing from the pbp page. |
| `linescore` | `Optional[DataFrame]` | `None` | Optional `sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_linescore` frame -- provides `home`/`away` team names and the official per-team finals. |
| `drive_titles` | `Optional[DataFrame]` | `None` | Optional `sportsdataverse.cfb.cfb_ncaa_pbp.parse_cfb_ncaa_drive_titles` frame -- authoritative per-drive team labels (fixes graduated-parser team truncation) and running-score checkpoints the play-level score snaps to at each drive boundary (self-heals OT scoring rules + missed events). |
| `ot_drives` | `Optional[DataFrame]` | `None` | Optional `sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_drives` frame for the OT-synthesis pass -- overtime rows (`period > 4`) are selected internally, so the full drives frame can be passed as-is. |
| `scoring_summary` | `Optional[DataFrame]` | `None` | Optional `sportsdataverse.cfb.cfb_ncaa_box.parse_cfb_ncaa_scoring_summary` frame -- running-score checkpoints for the synthesized OT rows and the final-drive snap. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of `polars`. |

**Returns**

A `polars.DataFrame` (or `pandas.DataFrame` when `return_as_pandas`) with one row per play (markers/furniture dropped) and the columns of `CFBFASTR_SCHEMA`. Empty input returns a **zero-row frame carrying the documented schema**. Two conventions the NCAA page forces, both matching the ESPN processor: a play wiped out by a penalty ("... NO PLAY.") is typed `"Penalty"` with every outcome flag `False` and every yardage column null (it keeps its participants, its `penalty_*` columns and its spot) -- except `fg_made`, which is null there as on every row that is not a field-goal attempt; and a try is attributed to the team that scored the touchdown, so a block-printed pair of tries carries a different `pos_team` per row.

No returns table is published for this function: no capture: its one-game stats.ncaa.org input comes only from a season-sized release or from raw HTML, and the season load runs longer than the capture allows.

**Example**

```python
from sportsdataverse.cfb import parse_cfb_ncaa_pbp, to_cfbfastr
pbp = parse_cfb_ncaa_pbp(open("play_by_play_5362431.html").read(), contest_id=5362431)
df = to_cfbfastr(pbp, season=2024)
print(df.shape)

# Final score from the running-score columns

df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
```
