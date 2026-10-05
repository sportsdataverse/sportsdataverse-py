---
title: "NFL — additional Python functions — Nfl"
sidebar_label: "Nfl"
sidebar_position: 8
description: "NFL — additional Python functions — Nfl — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Nfl

### nfl_availability_projection {#nfl_availability_projection}

`nfl_availability_projection(seasons: 'List[int]', target_season: 'int', *, team_games: 'int' = 17, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

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
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only). |
| `position` | character | Roster position from the most recent visible season. |
| `proj_availability` | double | Projected availability rate in [0, 1] - empirical-Bayes shrinkage of historical snap-based availability toward the fitted position base rate, then the fold-fit linear recalibration. |
| `proj_games` | double | Expected games available - proj_availability x team_games (17). |
| `proj_games_missed` | double | Expected games missed - team_games minus proj_games. |

**Example**

```python
from sportsdataverse.nfl.nfl_availability import nfl_availability_projection
avail = nfl_availability_projection([2021, 2022, 2023], 2024)
avail.sort("proj_games").head()
```

### nfl_clear_token_cache {#nfl_clear_token_cache}

`nfl_clear_token_cache() -> 'None'`

Drop the cached `api.nfl.com` token (forces a fresh mint on the next call).

### nfl_compute_results {#nfl_compute_results}

`nfl_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'Union[str, int]', *, rng: 'Optional[np.random.Generator]' = None, elo: 'Optional[Mapping[str, float]]' = None, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame]'`

Compute NFL game results for one week of a season simulation.

Faithful port of `nflseedR_compute_results` (simulations_utils.R
L183-290) — the 538-style dynamic ELO model initially coded by Lee
Sharpe and rewritten by Sebastian Carl: home/away ELO difference plus
rest (+25 per extra week), home field (+20), and a 1.2x postseason
multiplier produce a win probability and a point spread `estimate`
(`elo_diff / 25`); missing results for `week_num` are drawn from
`Normal(estimate, 13)` and rounded away from zero. ELO ratings are
updated from all of the week's results and carried to the next week
via the returned `teams` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `DataFrame` |  | Teams frame with `sim` and `team` columns. An `elo` column is added on first call (from `elo` or random `Normal(1500, 150)` initial ratings shared across sims) and must be carried between calls. |
| `games` | `DataFrame` |  | Games frame with `sim`, `week`, `game_type`, `location`, `home_team`/`away_team`, `home_rest`/ `away_rest`, and `result` columns. |
| `week_num` | `Union[str, int]` |  | The week to simulate. Only rows with `week == week_num` and a missing `result` are filled. |
| `rng` | `Optional[Generator]` | `None` | numpy random generator; a fresh one is created when `None`. |
| `elo` | `Optional[Mapping[str, float]]` | `None` | Optional mapping of team abbreviation to initial ELO rating. |

**Returns**

`{"teams": teams, "games": games}` with updated ELO ratings and filled results.

| col_name | type | description |
|---|---|---|
| `teams.sim` | integer | Simulated season identifier the team row belongs to, carried through from the input teams frame. |
| `teams.team` | character | Team abbreviation, carried through from the input teams frame. |
| `teams.conf` | character | Conference of the team (AFC or NFC), carried through from the input teams frame. |
| `teams.division` | character | Division of the team (e.g. "AFC East"), carried through from the input teams frame. |
| `teams.elo` | double | Dynamic ELO rating after applying the shifts from the simulated week's results; carried into the next week's call so ratings evolve over the simulated season. |
| `games.sim` | integer | Simulated season identifier the game row belongs to. |
| `games.game_type` | character | Game type of the row - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `games.week` | character | Week key used by the simulation engine - regular season week numbers as strings and postseason rounds as WC/DIV/CON/SB. |
| `games.away_team` | character | Team abbreviation of the away team. |
| `games.home_team` | character | Team abbreviation of the home team. |
| `games.away_rest` | integer | Days of rest for the away team before the game (feeds the ELO rest adjustment of 25 points per extra week). |
| `games.home_rest` | integer | Days of rest for the home team before the game. |
| `games.location` | character | Game site indicator - "Home" applies the +20 ELO home-field adjustment, "Neutral" (Super Bowl) does not. |
| `games.result` | integer | Home margin (home score minus away score). Rows of the simulated week that were missing are filled from Normal(estimate, 13) rounded away from zero; all other rows pass through unchanged. |

**Example**

```python
from sportsdataverse.nfl.nfl_simulations import nfl_compute_results
out = nfl_compute_results(teams, games, week_num="5")
teams, games = out["teams"], out["games"]
```

### nfl_draft_projection {#nfl_draft_projection}

`nfl_draft_projection(seasons: 'List[int]', target_class: 'int', *, lam: 'float' = 100.0, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Draft outcome projection for one draft class.

Trains the closed-form ridge (expected `car_av`) and the IRLS logistic
(`hit_prob` = P(`seasons_started >= 3`)) on **matured** classes
(`season <= target_class - 5`) and scores the `target_class`
prospects. Features: standardized combine measurables (+ imputation
flags), draft `round`/`pick`/`log(pick)`, position one-hots.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Draft classes to load (training classes beyond the maturity boundary are filtered out automatically). |
| `target_class` | `int` |  | The draft class to score. |
| `lam` | `float` | `100.0` | Ridge regularization strength. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

One row per `target_class` prospect: `gsis_id:Utf8, target_class:Int64, position:Utf8, pred_car_av:Float64, hit_prob:Float64, outcome_rank:Int64` (dense rank, best first). Empty training or prediction slice returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `gsis_id` | character | nflverse gsis player id of the drafted prospect (character join key). |
| `target_class` | integer | The draft class scored (training uses matured classes <= target_class - 5). |
| `position` | character | Draft position group of the prospect. |
| `pred_car_av` | double | Predicted career value - closed-form ridge on standardized combine measurables + round/pick/log(pick) + position one-hots; the label is nflverse w_av (PFR weighted career Approximate Value). |
| `hit_prob` | double | P(multi-year starter) - ridge-regularized IRLS logistic on the same features, hit := seasons_started >= 3. |
| `outcome_rank` | integer | Dense rank of pred_car_av within the class (best prospect = 1). |

**Example**

```python
from sportsdataverse.nfl.nfl_draft_model import nfl_draft_projection
proj = nfl_draft_projection(list(range(2000, 2020)), 2019)
proj.sort("outcome_rank").head()
```

### nfl_fantasy_projection {#nfl_fantasy_projection}

`nfl_fantasy_projection(seasons: 'List[int]', target_season: 'int', *, scoring: 'Union[Dict[str, float], str]' = 'ppr', calibrate: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Fantasy-points projection: deterministic scoring of the Marcel component

stats plus a fitted per-position linear calibration.

Scores `nfl_player_projection`'s projected component *counting* stats
(rate x projected games) under the scoring format, then applies the fitted
`fp_calibration` `(a, b)` from `POSITION_CONSTANTS`
(`calibrated = a + b * raw`). The FantasyPros consensus is used only as a
concurrent-validity oracle in the tests — never as an input.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load. |
| `target_season` | `int` |  | The season being projected. |
| `scoring` | `Union[Dict[str, float], str]` | `'ppr'` | `"ppr"` / `"half"` / `"standard"` or a custom points-per-unit dict. |
| `calibrate` | `bool` | `True` | Apply the fitted per-position calibration. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

`player_id:Utf8, target_season:Int64, position_group:Utf8, proj_fantasy_points:Float64, proj_fantasy_points_per_game:Float64, position_rank:Int64`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only). |
| `position_group` | character | nflverse offensive position group (QB/RB/WR/TE plus fringe groups). |
| `proj_fantasy_points` | double | Projected season fantasy points - the Marcel component rates x projected games scored under the scoring format, with the fitted per-position linear calibration applied by default. |
| `proj_fantasy_points_per_game` | double | Projected fantasy points per game (proj_fantasy_points / projected games). |
| `position_rank` | integer | Dense rank of proj_fantasy_points within the position group (best = 1). |

**Example**

```python
from sportsdataverse.nfl.nfl_projection import nfl_fantasy_projection
fp = nfl_fantasy_projection([2021, 2022, 2023], 2024)
fp.filter(pl.col("position_group") == "WR").head()

# Custom scoring

fp_std = nfl_fantasy_projection([2021, 2022, 2023], 2024, scoring="standard")
```

### nfl_game_details {#nfl_game_details}

`nfl_game_details(game_id: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, raw: 'bool' = False) -> 'Dict'`

Pull full `api.nfl.com` game details (drives + plays) by game id.

Hits `/experience/v1/gamedetails/{game_id}`; the payload is the shield
`data.viewer.gameDetail` object (plays, drives, scoring summaries, line
scores, possession, weather, attendance, ...).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` | `None` | the uuid game id from `nfl_game_schedule` (e.g. `'7d3e8f84-1312-11ef-afd1-646009f18b2e'`). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict. Defaults to a fresh `nfl_headers_gen` call. |
| `raw` | `bool` | `False` | If True, return the full envelope (`{"data": {...}}`) untouched. If False (default), unwrap to the `gameDetail` object. |

**Returns**

the `gameDetail` object (or the raw envelope when `raw=True`). Empty `dict` if the game has no detail payload.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_details
detail = nfl_game_details(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e")
len(detail["plays"]), len(detail["drives"])

# Reuse headers across many calls (avoids re-minting tokens)

from sportsdataverse.nfl.nfl_games import nfl_game_details, nfl_headers_gen
hdrs = nfl_headers_gen()
detail = nfl_game_details(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e", headers=hdrs)
```

### nfl_game_pbp {#nfl_game_pbp}

`nfl_game_pbp(game_id: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_as_pandas: 'bool' = False)`

Parsed `api.nfl.com` play-by-play -- one row per play (polars/pandas frame).

Tidy wrapper over `nfl_game_details`: flattens `gameDetail.plays` into a
DataFrame (`playId`, `quarter`, `down`, `yardsToGo`, `yardLine`,
`playType`, `playDescription`, `possessionTeam_*`, ...) and prepends the
game context (`game_id`, `home_team`, `visitor_team`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` | `None` | uuid game id from `nfl_week_games` / `nfl_game_schedule`. |
| `headers` | `Optional[Dict[str, str]]` | `None` | reuse a `nfl_headers_gen` dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per play (empty frame if the game has no play-by-play yet).

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `visitor_team` | character | Abbreviation or name of the visiting (away) team in this game. |
| `clockTime` | character | Game clock time at the start of the play, formatted as MM:SS within the quarter. |
| `down` | integer | The down for the given play. |
| `driveNetYards` | integer | Net yards gained on the current drive up to and including this play. |
| `drivePlayCount` | integer | Number of offensive plays run on the current drive up to and including this play. |
| `driveSequenceNumber` | integer | Sequential identifier for the drive within the game, starting at 1. |
| `driveTimeOfPossession` | character | Elapsed time of possession for the current drive, formatted as MM:SS. |
| `endClockTime` | character | Game clock time at the end of the play, formatted as MM:SS within the quarter. |
| `endYardLine` | character | Yard line where the ball was placed at the conclusion of the play. |
| `firstDown` | logical | Indicates whether the play resulted in a first down. |
| `goalToGo` | logical | Indicates whether the offensive team is in a goal-to-go situation at the start of the play. |
| `isBigPlay` | character | Classification label indicating whether this play is designated as a big play by the NFL. |
| `latestPlay` | character | Indicates whether this play is the most recent play recorded in the live feed. |
| `nextPlayIsGoalToGo` | logical | Indicates whether the next play will begin in a goal-to-go situation. |
| `nextPlayType` | character | Play type category anticipated or assigned to the next play in sequence. |
| `orderSequence` | integer | Sequential ordering value used to sort plays within the game in chronological order. |
| `penaltyOnPlay` | logical | Indicates whether a penalty was called on this play. |
| `playClock` | integer | Play clock value in seconds at the snap of the ball. |
| `playReviewStatus` | character | Status of any official review of the play (e.g., confirmed, overturned, stands). |
| `playDeleted` | logical | Indicates whether this play record has been marked as deleted or voided. |
| `playDescription` | character | Official text description of the play as provided by the NFL. |
| `playDescriptionWithJerseyNumbers` | character | Play description text augmented with player jersey numbers for participant identification. |
| `playId` | integer | Unique play event identifier (UUID). |
| `playStats` | integer | Internal NFL stat identifier linking this play to associated statistical records. |
| `playType` | character | Categorical classification of the play type (e.g., PASS, RUSH, PUNT, KICKOFF). |
| `prePlayByPlay` | character | Narrative text describing the game situation or setup immediately before this play. |
| `quarter` | integer | Game quarter in which the play occurred (1–4 for regulation, 5 for overtime). |
| `scoringPlay` | logical | Indicates whether this play resulted in points being scored. |
| `scoringPlayType` | character | Type of scoring event on this play (e.g., TD, FG, SAFETY, PAT). |
| `scoringTeam` | character | Abbreviation or identifier of the team that scored on this play, if applicable. |
| `shortDescription` | character | Short narrative summary of the play outcome (e.g., 'PENALTY', 'TOUCHDOWN'). |
| `specialTeamsPlay` | logical | Indicates whether this play was a special teams play. |
| `stPlayType` | character | Special teams play sub-type classification (e.g., PUNT, KICKOFF, FG_ATTEMPT) when applicable. |
| `timeOfDay` | character | Wall-clock time of day when the play occurred, typically in local or Eastern time. |
| `yardLine` | character | Yard line on the field where the ball was placed at the start of the play. |
| `yards` | integer | The number of receiving yards |
| `yardsToGo` | integer | Number of yards needed to gain a first down at the start of the play. |
| `possessionTeam_id` | character | NFL.com Shield API identifier for the team with offensive possession on this play. |
| `possessionTeam_abbreviation` | character | Abbreviated team name for the team with offensive possession on this play. |
| `possessionTeam_nickName` | character | Nickname (mascot name) of the team with offensive possession on this play. |
| `possessionTeam_franchise_primaryColor` | character | Primary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_secondaryColor` | character | Secondary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_tertiaryColor` | character | Tertiary brand color for the possessing team's franchise, expressed as a hex color code. |
| `possessionTeam_franchise_currentLogo` | character | URL of the current primary logo for the possessing team's franchise. |
| `possessionTeam_franchise_largeTypeColor` | character | Large-type display color for the possessing team's franchise branding, as a hex code. |
| `possessionTeam_franchise_decorativeElementsColor` | character | Decorative elements color for the possessing team's franchise branding, as a hex code. |
| `scoringTeam_id` | character | NFL.com Shield API identifier for the team that scored on this play. |
| `scoringTeam_abbreviation` | character | Abbreviated team name for the team that scored on this play. |
| `scoringTeam_nickName` | character | Nickname (mascot name) of the team that scored on this play. |
| `scoringTeam_franchise_primaryColor` | character | Primary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_secondaryColor` | character | Secondary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_tertiaryColor` | character | Tertiary brand color for the scoring team's franchise, expressed as a hex color code. |
| `scoringTeam_franchise_currentLogo` | character | URL of the current primary logo for the scoring team's franchise. |
| `scoringTeam_franchise_largeTypeColor` | character | Large-type display color for the scoring team's franchise branding, as a hex code. |
| `scoringTeam_franchise_decorativeElementsColor` | character | Decorative elements color for the scoring team's franchise branding, as a hex code. |

**Example**

```python
from sportsdataverse.nfl import nfl_game_pbp
pbp = nfl_game_pbp(game_id="7d3e8f84-1312-11ef-afd1-646009f18b2e")
pbp.select(["quarter", "down", "yardsToGo", "playType", "playDescription"]).head()
```

### nfl_game_schedule {#nfl_game_schedule}

`nfl_game_schedule(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'int' = 1, headers: 'Optional[Dict[str, str]]' = None, raw: 'bool' = False) -> 'Dict'`

List `api.nfl.com` games for a season/week slice (`/football/v2/games`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year (e.g. `2024`). |
| `season_type` | `str` | `'REG'` | season type. One of `"PRE"`, `"REG"`, `"POST"`. |
| `week` | `int` | `1` | week number (1-18 regular season, 1-4 post-season). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict (skip the auth roundtrip). Defaults to a fresh `nfl_headers_gen` call. |
| `raw` | `bool` | `False` | currently ignored; the function returns the parsed JSON payload. |

**Returns**

payload with the games list under `"games"` plus `"pagination"`. Each game carries `id` (the uuid game id used by `nfl_game_details`), `homeTeam`/`awayTeam`, `date`, `status`, `externalIds` (gsis etc.).

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_schedule
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1)
first_id = week_one["games"][0]["id"]
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
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `games` | integer | Games included in the aggregate. |
| `off_plays_pg` | double | Realized offensive plays per game. |
| `sec_per_play` | double | Season mean of the per-game sec_per_play. |
| `neutral_sec_per_play` | double | Season mean neutral-situation seconds per play (lower = faster). |
| `proe` | double | Season pass-rate over expected, dropback-weighted so it reconciles exactly with the pbp pass_oe aggregate. |
| `exp_plays_pg` | double | Expected plays per game from the fitted PACE_CONSTANTS OLS (own pace, opponent pace, market total). |
| `plays_oe` | double | Realized minus expected plays per game. |
| `pace_rank` | integer | Rank of neutral pace within the season (1 = fastest). |

**Example**

```python
from sportsdataverse.nfl.nfl_gamescript import nfl_game_script
gs = nfl_game_script([2023])
print(gs.sort("proe", descending=True).head())

# Pipeline next step

gs.filter(pl.col("plays_oe") > 0).sort("plays_oe", descending=True).head()
```

### nfl_headers_gen {#nfl_headers_gen}

`nfl_headers_gen(token: 'Optional[str]' = None) -> 'Dict[str, str]'`

Build the request-header dict expected by `api.nfl.com`.

Obtains a bearer token via `nfl_token_gen` (which caches + auto-renews,
or honors `NFL_ACCESS_TOKEN`) unless `token` is supplied, and combines it
with the browser-style headers the NFL.com web app sends. Token caching already
avoids re-minting, so callers rarely need to thread `token`/`headers` by hand.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `token` | `Optional[str]` | `None` | An existing access token to reuse; uses the cached/minted one when `None`. |

**Returns**

Header dict ready to drop into `requests.get`.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_headers_gen, nfl_game_schedule
hdrs = nfl_headers_gen()
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1, headers=hdrs)
week_two = nfl_game_schedule(season=2024, season_type="REG", week=2, headers=hdrs)
```

### nfl_kicker_rating {#nfl_kicker_rating}

`nfl_kicker_rating(seasons: 'Union[int, List[int]]', *, as_of: 'Optional[Tuple[int, int]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Environment-adjusted kicker FG-over-expected ratings.

Loads pbp FG attempts for `seasons`, computes the environment-adjusted
expected make probability per kick, and aggregates to per
`(season, kicker)` FGOE (raw + EB-shrunk with the fitted `K_fg`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons. |
| `as_of` | `Optional[Tuple[int, int]]` | `None` | Optional `(season, week)`; uses only kicks strictly before that point (the as-of leakage boundary for mid-season ratings). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, kicker_player_id)`: `kicker`, `team`, `fg_att`, `fg_made`, `exp_made`, `fgoe`, `fgoe_per_att`, `fgoe_shrunk`, `rating` (100 +/- 15 z of `fgoe_shrunk`). Empty seasons yield a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the rating. |
| `kicker_player_id` | character | nflverse kicker GSIS id (Utf8 join key). |
| `kicker` | character | Display name of the kicker (e.g. J.Tucker), from kicker_player_name. |
| `team` | character | Team of the kicker's most recent attempt in the window. |
| `fg_att` | integer | Field-goal attempts. |
| `fg_made` | integer | Field goals made. |
| `exp_made` | double | Sum of environment-adjusted make probabilities (expected makes). |
| `fgoe` | double | Field goals made over expected (fg_made - exp_made). |
| `fgoe_per_att` | double | FGOE per attempt. |
| `fgoe_shrunk` | double | Empirical-Bayes shrunk FGOE per attempt, fgoe_per_att * att / (att + K_fg). |
| `rating` | double | 100 +/- 15 z-score of fgoe_shrunk within the frame. |

**Example**

```python
from sportsdataverse.nfl.nfl_kicker_rating import nfl_kicker_rating
r = nfl_kicker_rating([2023])
print(r.head())

# Mid-season as-of rating

r = nfl_kicker_rating([2023], as_of=(2023, 10))
```

### nfl_line_grades {#nfl_line_grades}

`nfl_line_grades(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Team-season OL pass-block + DL pass-rush grades (opponent-adjusted, EB-shrunk).

Loads pbp, builds the matchup pressure grid, opponent-adjusts it, grades
both units on a 0-100 board (`50 + 15*z*n/(n+K_pressure)`), and joins
PFR's independent team pressure measurement
(`load_nfl_pfr_advstats(stat_type="def", summary_level="season")`,
`prss` summed to team / pbp dropbacks faced) as `pfr_pressure_pct`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons (PFR advstats coverage is 2018+). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, team)`: raw + adjusted pressure rates and dropback counts, `ol_pass_block_grade`, `dl_pass_rush_grade`, `pfr_pressure_pct`. Empty seasons yield a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the grade. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off (raw). |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def (raw). |
| `adj_pressure_rate_allowed` | double | Opponent-adjusted allowed pressure rate (additive fixed point, league-mean-centered). |
| `adj_pressure_rate_generated` | double | Opponent-adjusted generated pressure rate (additive fixed point, league-mean-centered). |
| `ol_pass_block_grade` | double | OL pass-block grade, 50 + 15 * z * n/(n + K_pressure) on the inverted adjusted allowed rate. |
| `dl_pass_rush_grade` | double | DL pass-rush grade, 50 + 15 * z * n/(n + K_pressure) on the adjusted generated rate. |
| `pfr_pressure_pct` | double | PFR team pressures (prss summed, traded 2TM/3TM rows excluded) divided by pbp dropbacks faced. |

**Example**

```python
from sportsdataverse.nfl.nfl_line_grades import nfl_line_grades
g = nfl_line_grades([2023])
print(g.sort("dl_pass_rush_grade", descending=True).head())
```

### nfl_ngs_gamecenter_overview {#nfl_ngs_gamecenter_overview}

`nfl_ngs_gamecenter_overview(game_id, group: 'str' = 'passers', return_as_pandas: 'bool' = False)`

NGS gamecenter overview for one game -- one row per player on a side.

Wraps `/api/gamecenter/overview` (keyed by NGS `gameId`). The payload
splits each stat `group` into `home` and `visitor` entries; this function
stacks both and tags every row with `side` (`"home"`/`"visitor"`) plus the
game's `gameId`. Note `passers` carries a single primary QB per side (two
rows total) while `rushers`/`receivers`/`passRushers` are full lists.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  | NGS `gameId` (e.g. `"2024090500"`) from `nfl_ngs_league_schedule`. |
| `group` | `str` | `'passers'` | which player group -- one of `"passers"`, `"rushers"`, `"receivers"`, `"passRushers"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per player (both teams), with `side` and `gameId` columns prepended.

| col_name | type | description |
|---|---|---|
| `side` | character | "home" or "visitor" -- which team's roster the row belongs to. |
| `gameId` | character | Unique identifier for the NFL game in the NGS/Shield data system. |
| `esbId` | character | Elias Sports Bureau identifier for the player, used to link to official NFL records and statistics. |
| `teamId` | character | Unique numeric identifier for the player's team in the NFL NGS/Shield data system. |
| `teamAbbr` | character | Two- or three-letter abbreviation identifying the player's team (e.g., 'KC', 'PHI'). |
| `shortName` | character | Abbreviated display name of the player (e.g., 'P.Mahomes') used in compact UI contexts. |
| `position` | character | Primary position as reported by NFL.com |
| `jerseyNumber` | integer | Jersey number worn by the player during the game. |
| `playerName` | character | Full display name of the player as shown in the NFL Next Gen Stats gamecenter. |
| `zones` | integer | Serialized or structured representation of field zones targeted or covered by the player during the game. |
| `passYards` | integer | Total passing yards accumulated by the player (relevant for quarterbacks) in the NGS gamecenter overview. |
| `touchdowns` | integer | Total number of touchdowns scored or thrown by the player in the NGS gamecenter overview. |
| `interceptions` | integer | The number of interceptions thrown. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `completions` | integer | The number of completed passes. |
| `headshot` | character | NFL headshot url for player |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_gamecenter_overview
ov = nfl_ngs_gamecenter_overview(game_id="2024090500", group="passers")
ov.select(["side", "playerName", "position"]).head()
```

### nfl_ngs_leaders {#nfl_ngs_leaders}

`nfl_ngs_leaders(category: 'str' = 'speed', season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)`

NGS top-N "leaders" board for a single category (one row per leader play).

One parameterized wrapper over the single-list leader endpoints. Each record
nests a `leader` (player/stat) block and a `play` (the play that produced
the highlight) block, flattened to `leader_*` / `play_*` columns.

Categories (`category=` value -> endpoint):

* `"speed"` -> `/leaders/speed/ballCarrier` (fastest ball-carrier speeds)
* `"distance_ballcarrier"` -> `/leaders/distance/ballCarrier`
* `"distance_tackle"` -> `/leaders/distance/tackle`
* `"time_sack"` -> `/leaders/time/sack`
* `"completion_season"` / `"completion_week"` ->
  `/leaders/expectation/completion/{season,week}` (most-improbable completions)
* `"ery_season"` / `"ery_week"` -> `/leaders/expectation/ery/{season,week}`
  (expected rush yards over expectation)
* `"yac_season"` / `"yac_week"` -> `/leaders/expectation/yac/{season,week}`
  (yards after catch over expectation)

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'speed'` | one of the keys above. Defaults to `"speed"`. |
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | week filter -- required (and only used) by the `*_week` categories; ignored by season/non-expectation boards. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per leader entry.

| col_name | type | description |
|---|---|---|
| `leader_esbId` | character | Elias Sports Bureau identifier for the statistical leader, used to link to official NFL records. |
| `leader_firstName` | character | First name of the statistical leader player. |
| `leader_gsisId` | character | NFL GSIS (Game Statistics and Information System) identifier for the statistical leader player. |
| `leader_jerseyNumber` | integer | Jersey number worn by the statistical leader player. |
| `leader_lastName` | character | Last name of the statistical leader player. |
| `leader_playerName` | character | Full display name of the statistical leader player as shown in NFL NGS. |
| `leader_position` | character | Specific position designation of the statistical leader (e.g., 'QB', 'WR', 'CB'). |
| `leader_positionGroup` | character | Broader position group of the statistical leader (e.g., 'Offense', 'Defense', 'Special Teams'). |
| `leader_shortName` | character | Abbreviated display name of the statistical leader (e.g., 'P.Mahomes') for compact UI usage. |
| `leader_teamAbbr` | character | Two- or three-letter abbreviation identifying the statistical leader's team. |
| `leader_teamId` | character | Unique numeric identifier for the statistical leader's team in the NFL NGS/Shield system. |
| `leader_week` | integer | NFL week number during which the statistical leader achieved the highlighted performance. |
| `leader_yards` | integer | Total yards associated with the statistical leader's highlighted play or performance metric. |
| `leader_inPlayDist` | double | Total in-play distance traveled (in yards) by the statistical leader during the highlighted play. |
| `leader_maxSpeed` | double | Maximum recorded speed (in miles per hour) reached by the statistical leader during the highlighted play. |
| `leader_headshot` | character | URL to the player's official headshot image as provided by the NFL NGS system. |
| `play_gameId` | integer | Unique identifier for the NFL game in which the highlighted play occurred. |
| `play_playId` | integer | Unique identifier for the specific play within the game in the NFL NGS/Shield system. |
| `play_sequence` | integer | Sequential order number of the play within the game or drive as recorded in the NGS system. |
| `play_down` | integer | Down number (1–4) on which the highlighted play occurred. |
| `play_gameClock` | character | Game clock time (MM:SS) at the start of the highlighted play. |
| `play_gameKey` | integer | Alternate numeric key for the NFL game in which the highlighted play occurred, used in official NFL systems. |
| `play_health_playerTracking` | character | Indicator of whether player-tracking data from the NGS health/tracking system is available for this play. |
| `play_health_ballTracking` | character | Indicator of whether ball-tracking data from the NGS health/tracking system is available for this play. |
| `play_homeScore` | integer | Home team's score at the time of the highlighted play. |
| `play_isBigPlay` | logical | Boolean flag indicating whether the highlighted play is classified as a 'big play' in the NGS system. |
| `play_isEndQuarter` | logical | Boolean flag indicating whether the highlighted play occurred at the end of a quarter. |
| `play_isGoalToGo` | logical | Boolean flag indicating whether the highlighted play occurred in a goal-to-go situation. |
| `play_isPenalty` | logical | Boolean flag indicating whether the highlighted play involved a penalty. |
| `play_isSTPlay` | logical | Boolean flag indicating whether the highlighted play was a special teams play. |
| `play_isScoring` | logical | Boolean flag indicating whether the highlighted play resulted in a score. |
| `play_playDescription` | character | Full text description of the highlighted play as recorded by official NFL scorers. |
| `play_playState` | character | State or status of the highlighted play (e.g., 'APPROVED', 'CHALLENGED') in the NGS system. |
| `play_playStats` | integer | Serialized statistical outcomes or stat codes associated with the highlighted play. |
| `play_playType` | character | Text label describing the type of the highlighted play (e.g., 'PASS', 'RUSH', 'KICK'). |
| `play_playTypeCode` | integer | Numeric or short code identifying the play type of the highlighted play in the NGS system. |
| `play_possessionTeamId` | character | Unique identifier for the team that had possession of the ball during the highlighted play. |
| `play_preSnapHomeScore` | integer | Home team's score immediately before the snap on the highlighted play. |
| `play_preSnapVisitorScore` | integer | Visitor team's score immediately before the snap on the highlighted play. |
| `play_quarter` | integer | Quarter (1–4, or 5 for overtime) in which the highlighted play occurred. |
| `play_timeOfDayUTC` | character | Wall-clock timestamp in UTC representing the real-world time the highlighted play occurred. |
| `play_visitorScore` | integer | Visitor team's score at the time of the highlighted play. |
| `play_yardline` | character | Formatted yard line string (e.g., 'KC 35') indicating the field position of the highlighted play. |
| `play_yardlineNumber` | integer | Numeric yard line (1–50) indicating the field position of the highlighted play. |
| `play_yardlineSide` | character | Team abbreviation indicating which team's side of the field the highlighted play occurred on. |
| `play_yardsToGo` | integer | Number of yards needed for a first down at the start of the highlighted play. |
| `play_absoluteYardlineNumber` | integer | Absolute yard line number (1–100) on the field where the highlighted play occurred. |
| `play_actualYardlineForFirstDown` | double | Yard line the offense must reach to convert a first down on the highlighted play. |
| `play_actualYardsToGo` | double | Actual distance in yards the offense needed to gain a first down on the highlighted play. |
| `play_endGameClock` | character | Game clock time (MM:SS) at the end of the highlighted play. |
| `play_isChangeOfPossession` | logical | Boolean flag indicating whether the highlighted play resulted in a change of ball possession. |
| `play_playDirection` | character | Direction of the highlighted play on the field (e.g., left, middle, right). |
| `play_startGameClock` | character | Game clock time (MM:SS) at the start of the highlighted play. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_leaders
fast = nfl_ngs_leaders(category="speed", season=2024, season_type="REG")
fast.select(["leader_playerName", "leader_maxSpeed", "play_playDescription"]).head()
```

### nfl_ngs_league_schedule {#nfl_ngs_league_schedule}

`nfl_ngs_league_schedule(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)`

NGS league schedule -- one row per game; source of NGS `gameId` values.

Wraps `/api/league/schedule` (which returns a top-level list of games).
Each row carries `gameId` (the `YYYYMMDDNN` id used by the game-scoped
functions here), `gameKey`, `smartId` (the api.nfl.com uuid), team
abbreviations/ids/names, kickoff times, `ngsGame` (tracking-data flag) and
`season`/`seasonType`/`week`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | optional single-week filter. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per scheduled game.

| col_name | type | description |
|---|---|---|
| `gameKey` | integer | Legacy NFL game key used as an alternative identifier in the NGS scheduling system. |
| `gameDate` | character | Game date-time (ISO 8601, UTC). |
| `gameId` | integer | NFL Next Gen Stats integer identifier for the game. |
| `gameTime` | character | Scheduled kickoff time for the game in local or UTC format. |
| `gameTimeEastern` | character | Scheduled kickoff time for the game expressed in Eastern Time. |
| `gameType` | character | Game type identifier (3 for playoffs). |
| `homeDisplayName` | character | Full display name of the home team (e.g., 'Kansas City Chiefs'). |
| `homeNickname` | character | Nickname (mascot name) of the home team (e.g., 'Chiefs'). |
| `homeTeamAbbr` | character | Abbreviated team name for the home team at the schedule row level. |
| `homeTeamId` | character | NFL Next Gen Stats team identifier for the home team at the schedule row level. |
| `isoTime` | integer | Scheduled kickoff time expressed as a Unix timestamp (milliseconds since epoch). |
| `networkChannel` | character | Television network or channel broadcasting this game (e.g., 'NBC', 'ESPN'). |
| `ngsGame` | logical | Indicates whether this game has full Next Gen Stats data collection enabled. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Season segment in which the game occurs (e.g., 'REG' for regular season, 'POST' for playoffs). |
| `smartId` | character | NFL Next Gen Stats smart (UUID-style) identifier for this scheduled game. |
| `visitorDisplayName` | character | Full display name of the visiting (away) team (e.g., 'San Francisco 49ers'). |
| `visitorNickname` | character | Nickname (mascot name) of the visiting team (e.g., '49ers'). |
| `visitorTeamAbbr` | character | Abbreviated team name for the visiting team at the schedule row level. |
| `visitorTeamId` | character | NFL Next Gen Stats team identifier for the visiting team at the schedule row level. |
| `week` | integer | Season week. |
| `weekNameAbbr` | character | Abbreviated label for the week of the season (e.g., 'WK1', 'WC' for Wild Card). |
| `liveDotsGame` | logical | Indicates whether this game has live player-tracking dot data available via NGS. |
| `validated` | logical | Indicates whether the schedule entry has been validated and confirmed by the NFL. |
| `releasedToClubs` | logical | Indicates whether NGS data for this game has been released to team personnel. |
| `homeTeam_teamId` | character | NFL Next Gen Stats integer team identifier for the home team from the nested team object. |
| `homeTeam_smartId` | character | NFL Next Gen Stats smart (UUID-style) identifier for the home team. |
| `homeTeam_logo` | character | URL of the home team's logo from the nested team object. |
| `homeTeam_abbr` | character | Abbreviated team name for the home team from the nested team object. |
| `homeTeam_cityState` | character | City and state of the home team as provided in the nested team object. |
| `homeTeam_fullName` | character | Full official name of the home team from the nested team object. |
| `homeTeam_nick` | character | Nickname of the home team from the nested team object. |
| `homeTeam_teamType` | character | Classification of the home team type (e.g., 'NFL' for active franchises). |
| `homeTeam_conferenceAbbr` | character | Conference abbreviation for the home team from the nested team object (e.g., 'AFC'). |
| `homeTeam_divisionAbbr` | character | Division abbreviation for the home team from the nested team object (e.g., 'AFC West'). |
| `site_smartId` | character | NFL Next Gen Stats smart (UUID-style) identifier for the game venue. |
| `site_siteId` | integer | NFL Next Gen Stats integer identifier for the game venue. |
| `site_siteFullName` | character | Full official name of the game venue (e.g., 'Arrowhead Stadium'). |
| `site_siteCity` | character | City where the game venue is located. |
| `site_siteState` | character | State (or country for international games) where the game venue is located. |
| `site_postalCode` | character | Postal (ZIP) code of the venue where the game is played. |
| `site_roofType` | character | Roof type of the game venue (e.g., 'OUTDOOR', 'DOME', 'RETRACTABLE'). |
| `visitorTeam_teamId` | character | NFL Next Gen Stats integer team identifier for the visiting team from the nested team object. |
| `visitorTeam_smartId` | character | NFL Next Gen Stats smart (UUID-style) identifier for the visiting team. |
| `visitorTeam_logo` | character | URL of the visiting team's logo from the nested team object. |
| `visitorTeam_abbr` | character | Abbreviated team name for the visiting team from the nested team object. |
| `visitorTeam_cityState` | character | City and state of the visiting team as provided in the nested team object. |
| `visitorTeam_fullName` | character | Full official name of the visiting team from the nested team object. |
| `visitorTeam_nick` | character | Nickname of the visiting team from the nested team object. |
| `visitorTeam_teamType` | character | Classification of the visiting team type (e.g., 'NFL' for active franchises). |
| `visitorTeam_conferenceAbbr` | character | Conference abbreviation for the visiting team from the nested team object (e.g., 'NFC'). |
| `visitorTeam_divisionAbbr` | character | Division abbreviation for the visiting team from the nested team object (e.g., 'NFC West'). |
| `score_time` | character | Game clock time at the current state as reported by the live score sub-object. |
| `score_phase` | character | Current phase or status of the game as reported by the live score sub-object (e.g., 'FINAL', 'IN_PROGRESS'). |
| `score_visitorTeamScore_pointTotal` | integer | Visitor team total points scored through the current game state, from the live score sub-object. |
| `score_visitorTeamScore_pointQ1` | integer | Visitor team points scored in the first quarter, from the live score sub-object. |
| `score_visitorTeamScore_pointQ2` | integer | Visitor team points scored in the second quarter, from the live score sub-object. |
| `score_visitorTeamScore_pointQ3` | integer | Visitor team points scored in the third quarter, from the live score sub-object. |
| `score_visitorTeamScore_pointQ4` | integer | Visitor team points scored in the fourth quarter, from the live score sub-object. |
| `score_visitorTeamScore_pointOT` | integer | Visitor team points scored in overtime, from the live score sub-object. |
| `score_visitorTeamScore_timeoutsRemaining` | integer | Number of timeouts remaining for the visitor team, from the live score sub-object. |
| `score_homeTeamScore_pointTotal` | integer | Home team total points scored through the current game state, from the live score sub-object. |
| `score_homeTeamScore_pointQ1` | integer | Home team points scored in the first quarter, from the live score sub-object. |
| `score_homeTeamScore_pointQ2` | integer | Home team points scored in the second quarter, from the live score sub-object. |
| `score_homeTeamScore_pointQ3` | integer | Home team points scored in the third quarter, from the live score sub-object. |
| `score_homeTeamScore_pointQ4` | integer | Home team points scored in the fourth quarter, from the live score sub-object. |
| `score_homeTeamScore_pointOT` | integer | Home team points scored in overtime, from the live score sub-object. |
| `score_homeTeamScore_timeoutsRemaining` | integer | Number of timeouts remaining for the home team, from the live score sub-object. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_league_schedule
sched = nfl_ngs_league_schedule(season=2024, season_type="REG", week=1)
first_game_id = sched["gameId"][0]
```

### nfl_ngs_league_schedule_current {#nfl_ngs_league_schedule_current}

`nfl_ngs_league_schedule_current(return_as_pandas: 'bool' = False)`

NGS schedule for the *current* week -- one row per game.

Wraps `/api/league/schedule/current`; the games are under the `games` key
(alongside scalar `season`/`seasonType`/`week` describing the slice).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per game in the current week.

| col_name | type | description |
|---|---|---|
| `gameKey` | integer | Alternate numeric key for the NFL game used in official NFL NGS record-keeping. |
| `gameDate` | character | Game date-time (ISO 8601, UTC). |
| `gameId` | integer | Unique identifier for the NFL game in the NGS/Shield data system. |
| `gameTime` | character | Scheduled kickoff time for the game in local or ET representation. |
| `gameTimeEastern` | character | Scheduled kickoff time for the game in Eastern Time (ET), as published by the NFL. |
| `gameType` | character | Game type identifier (3 for playoffs). |
| `homeDisplayName` | character | Full display name of the home team (e.g., 'Kansas City Chiefs') for the scheduled game. |
| `homeNickname` | character | Nickname of the home team (e.g., 'Chiefs') for the scheduled game. |
| `homeTeamAbbr` | character | Two- or three-letter abbreviation identifying the home team at the top-level game record. |
| `homeTeamId` | character | Unique numeric identifier for the home team at the top-level game record. |
| `isoTime` | integer | ISO 8601 formatted datetime string representing the scheduled kickoff time of the game. |
| `networkChannel` | character | Broadcast network or channel airing the game (e.g., 'NBC', 'ESPN', 'FOX'). |
| `ngsGame` | logical | Boolean or indicator flag marking whether this game entry is an official NGS-tracked game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Season type classification for the game (e.g., 'REG' for regular season, 'POST' for playoffs, 'PRE' for preseason). |
| `smartId` | character | Smart ID (NGS/Shield internal identifier) for the game record itself. |
| `visitorDisplayName` | character | Full display name of the visiting team (e.g., 'Philadelphia Eagles') for the scheduled game. |
| `visitorNickname` | character | Nickname of the visiting team (e.g., 'Eagles') for the scheduled game. |
| `visitorTeamAbbr` | character | Two- or three-letter abbreviation identifying the visiting team at the top-level game record. |
| `visitorTeamId` | character | Unique numeric identifier for the visiting team at the top-level game record. |
| `week` | integer | Season week. |
| `weekNameAbbr` | character | Abbreviated name or label for the NFL week (e.g., 'WK1', 'WLD' for Wild Card) in which the game is scheduled. |
| `releasedToClubs` | logical | Boolean flag indicating whether the schedule entry has been officially released to NFL clubs. |
| `validated` | logical | Boolean flag indicating whether the schedule entry has been validated by NFL operations. |
| `homeTeam_teamId` | character | Unique numeric identifier for the home team in the NFL NGS/Shield data system. |
| `homeTeam_smartId` | character | Smart ID (NGS/Shield internal identifier) for the home team. |
| `homeTeam_logo` | character | URL to the home team's official logo image as provided in the NGS schedule feed. |
| `homeTeam_abbr` | character | Two- or three-letter abbreviation for the home team (e.g., 'KC'). |
| `homeTeam_cityState` | character | City and state string for the home team's primary market (e.g., 'Kansas City, MO'). |
| `homeTeam_fullName` | character | Full official name of the home team (e.g., 'Kansas City Chiefs'). |
| `homeTeam_nick` | character | Short nickname of the home team (e.g., 'Chiefs') as used in the NGS system. |
| `homeTeam_teamType` | character | Classification of the home team type (e.g., 'NFL' for a standard league franchise). |
| `homeTeam_conferenceAbbr` | character | Conference abbreviation for the home team (e.g., 'AFC' or 'NFC'). |
| `homeTeam_divisionAbbr` | character | Division abbreviation for the home team (e.g., 'AFC West'). |
| `site_smartId` | character | Smart ID (NGS/Shield internal identifier) for the game venue. |
| `site_siteId` | integer | Unique identifier for the venue or stadium in the NFL NGS/Shield data system. |
| `site_siteFullName` | character | Full official name of the stadium or venue where the game is scheduled (e.g., 'Arrowhead Stadium'). |
| `site_siteCity` | character | City in which the game's venue (stadium) is located. |
| `site_siteState` | character | State (or country) in which the game's venue is located. |
| `site_postalCode` | character | Postal (ZIP) code of the stadium or venue where the game is scheduled to be played. |
| `site_roofType` | character | Roof type of the game venue (e.g., 'OPEN', 'DOME', 'RETRACTABLE') affecting playing conditions. |
| `visitorTeam_teamId` | character | Unique numeric identifier for the visiting team in the NFL NGS/Shield data system. |
| `visitorTeam_smartId` | character | Smart ID (NGS/Shield internal identifier) for the visiting team. |
| `visitorTeam_logo` | character | URL to the visiting team's official logo image as provided in the NGS schedule feed. |
| `visitorTeam_abbr` | character | Two- or three-letter abbreviation for the visiting team (e.g., 'PHI'). |
| `visitorTeam_cityState` | character | City and state string for the visiting team's primary market (e.g., 'Philadelphia, PA'). |
| `visitorTeam_fullName` | character | Full official name of the visiting team (e.g., 'Philadelphia Eagles'). |
| `visitorTeam_nick` | character | Short nickname of the visiting team (e.g., 'Eagles') as used in the NGS system. |
| `visitorTeam_teamType` | character | Classification of the visiting team type (e.g., 'NFL' for a standard league franchise). |
| `visitorTeam_conferenceAbbr` | character | Conference abbreviation for the visiting team (e.g., 'AFC' or 'NFC'). |
| `visitorTeam_divisionAbbr` | character | Division abbreviation for the visiting team (e.g., 'NFC East'). |
| `score_time` | character | Game clock time or elapsed time associated with the current scoring state snapshot. |
| `score_phase` | character | Current phase or status of the game (e.g., 'FINAL', 'IN_PROGRESS', 'PREGAME'). |
| `score_visitorTeamScore_pointTotal` | integer | Visitor team's total final score (sum of all quarters and overtime) for the game. |
| `score_visitorTeamScore_pointQ1` | integer | Visitor team's points scored in the first quarter of the game. |
| `score_visitorTeamScore_pointQ2` | integer | Visitor team's points scored in the second quarter of the game. |
| `score_visitorTeamScore_pointQ3` | integer | Visitor team's points scored in the third quarter of the game. |
| `score_visitorTeamScore_pointQ4` | integer | Visitor team's points scored in the fourth quarter of the game. |
| `score_visitorTeamScore_pointOT` | integer | Visitor team's points scored in overtime during the game, if applicable. |
| `score_visitorTeamScore_timeoutsRemaining` | integer | Number of timeouts remaining for the visitor team at the current game state. |
| `score_homeTeamScore_pointTotal` | integer | Home team's total final score (sum of all quarters and overtime) for the game. |
| `score_homeTeamScore_pointQ1` | integer | Home team's points scored in the first quarter of the game. |
| `score_homeTeamScore_pointQ2` | integer | Home team's points scored in the second quarter of the game. |
| `score_homeTeamScore_pointQ3` | integer | Home team's points scored in the third quarter of the game. |
| `score_homeTeamScore_pointQ4` | integer | Home team's points scored in the fourth quarter of the game. |
| `score_homeTeamScore_pointOT` | integer | Home team's points scored in overtime during the game, if applicable. |
| `score_homeTeamScore_timeoutsRemaining` | integer | Number of timeouts remaining for the home team at the current game state. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_league_schedule_current
cur = nfl_ngs_league_schedule_current()
cur.select(["gameId", "homeTeamAbbr", "visitorTeamAbbr"]).head()
```

### nfl_ngs_league_teams {#nfl_ngs_league_teams}

`nfl_ngs_league_teams(return_as_pandas: 'bool' = False)`

NGS team directory -- one row per team.

Wraps `/api/league/teams` (top-level list). Each row carries `teamId`,
`abbr`, `fullName`, `nick`, `conference`/`division`, `cityState`,
`stadiumName`, `smartId`, `logo` and site/ticket URLs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per team.

| col_name | type | description |
|---|---|---|
| `abbr` | character | Official team abbreviation used by the NFL Next Gen Stats system (e.g., 'KC', 'NE'). |
| `cityState` | character | City and state where the team is based (e.g., 'Kansas City, MO'). |
| `conferenceAbbr` | character | Abbreviation of the conference the team belongs to (e.g., 'AFC', 'NFC'). |
| `fullName` | character | Full name of the probable starting pitcher. |
| `logo` | character | Team or league logo URL. |
| `nick` | character | Team nickname or mascot name (e.g., 'Chiefs', 'Patriots'). |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `smartId` | character | NFL Next Gen Stats smart (UUID-style) identifier for the team. |
| `stadiumName` | character | Name of the team's home stadium (e.g., 'Arrowhead Stadium'). |
| `teamId` | character | NFL Next Gen Stats integer identifier for the team. |
| `teamSiteTicketUrl` | character | URL for purchasing tickets via the team's official website. |
| `teamSiteUrl` | character | URL of the team's official website. |
| `teamType` | character | Classification of the team type within the NFL system (e.g., 'NFL' for active franchises). |
| `ticketPhoneNumber` | character | Phone number for ticket sales inquiries for this team. |
| `yearFound` | integer | Year the franchise was founded. |
| `conference_id` | character | Referencing conference id. |
| `conference_abbr` | character | Conference abbreviation. |
| `conference_fullName` | character | Full name of the conference the team belongs to (e.g., 'American Football Conference'). |
| `division_id` | character | Division MLBAM ID. |
| `division_abbr` | character | Division abbreviation. |
| `division_fullName` | character | Full name of the division the team belongs to (e.g., 'AFC West'). |
| `divisionAbbr` | character | Abbreviation of the division the team belongs to (e.g., 'AFC West'). |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_league_teams
teams = nfl_ngs_league_teams()
teams.select(["teamId", "abbr", "fullName", "conferenceAbbr"]).head()
```

### nfl_ngs_man_zone_rates {#nfl_ngs_man_zone_rates}

`nfl_ngs_man_zone_rates(seasons: 'Union[int, Sequence[int]]', *, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Descriptive man/zone coverage rates from NGS-charted labels — NOT a trained classifier.

This is a group-by of the `defense_man_zone_type` /
`defense_coverage_type` labels that ship in
`sportsdataverse.nfl.load_nfl_pbp_participation` for charted
seasons (2016-2023). A *trained* coverage classifier is data-blocked —
see the module docstring's "Blocked (needs snap tracking)" section.
Unlabelled plays are dropped before rates are computed; `2_MAN` and
`PREVENT` calls stay in the `plays` denominator but have no
dedicated rate column, so the `cover_*_rate` columns sum to slightly
under 1 while `man_rate + zone_rate == 1` exactly.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, Sequence[int]]` |  | Season(s), charted 2016-2023. Seasons are loaded one at a time and concatenated `diagonal_relaxed` (the participation feed drifts schema across seasons). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |
| `_loader` | `Optional[Callable]` | `None` | Injectable loader for offline tests. |

**Returns**

One row per `(season, defteam)` with `plays` (labelled plays only), `man_rate`, `zone_rate` and `cover_0_rate` ... `cover_6_rate`. Un-charted seasons (all labels null, e.g. 2024+) return a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (YYYY), derived from the nflverse game id. |
| `defteam` | character | Defensive team abbreviation (the non-possession team of the game). |
| `plays` | integer | Number of charted (labelled) defensive plays in the denominator; unlabelled plays are dropped. |
| `man_rate` | double | Share of labelled plays charted as man coverage. man_rate + zone_rate == 1 exactly. |
| `zone_rate` | double | Share of labelled plays charted as zone coverage. |
| `cover_0_rate` | double | Share of labelled plays charted as COVER_0. 2_MAN and PREVENT calls stay in the denominator without a dedicated column, so the cover_*_rate columns sum to slightly under 1. |
| `cover_1_rate` | double | Share of labelled plays charted as COVER_1. |
| `cover_2_rate` | double | Share of labelled plays charted as COVER_2. |
| `cover_3_rate` | double | Share of labelled plays charted as COVER_3. |
| `cover_4_rate` | double | Share of labelled plays charted as COVER_4. |
| `cover_5_rate` | double | Share of labelled plays charted as COVER_5. |
| `cover_6_rate` | double | Share of labelled plays charted as COVER_6. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_man_zone_rates
df = nfl_ngs_man_zone_rates([2022])
print(df.sort("man_rate", descending=True).head())
```

### nfl_ngs_microsite_chart {#nfl_ngs_microsite_chart}

`nfl_ngs_microsite_chart(season: 'int' = 2024, season_type: 'str' = 'REG', week=None, chart_type=None, team_id=None, limit: 'int' = 100, offset: 'int' = 0, return_as_pandas: 'bool' = False)`

NGS microsite chart catalogue -- one row per rendered player chart image.

Wraps `/api/content/microsite/chart`; records live under `charts` and each
carries the chart `imageName`/`type` (`qb-grid`, `pass`, `route`,
`carry`) plus the player and headline stats (`passerRating`,
`completions`, etc.) and image-size URLs. Supports server-side paging.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` |  | `None` | optional week filter (the API accepts `"all"` by default). |
| `chart_type` |  | `None` | optional chart-type filter (e.g. `"qb-grid"`, `"pass"`). |
| `team_id` |  | `None` | optional team-id filter. |
| `limit` | `int` | `100` | page size (passed as `limit`). |
| `offset` | `int` | `0` | page offset. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per chart in the page.

| col_name | type | description |
|---|---|---|
| `imageName` | character | Filename or label for the player image asset used on the NGS microsite chart. |
| `esbId` | character | Elias Sports Bureau identifier for the player featured on the NGS microsite chart. |
| `firstName` | character | Scorer first name (localized list). |
| `gameId` | integer | Unique identifier for the NFL game associated with the NGS microsite chart entry. |
| `headshot` | character | NFL headshot url for player |
| `lastName` | character | Scorer last name (localized list). |
| `playerName` | character | Full display name of the player featured on the NGS microsite chart. |
| `position` | character | Primary position as reported by NFL.com |
| `receivingYards` | integer | Total receiving yards for the player (receiver) featured on the NGS microsite chart. |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Season type classification for the chart entry (e.g., 'REG' for regular season, 'POST' for playoffs). |
| `teamId` | character | Unique numeric identifier for the player's team in the NFL NGS/Shield data system. |
| `timestamp` | integer | Response timestamp (ISO 8601). |
| `touchdowns` | integer | Total number of touchdowns scored or thrown by the player featured on the NGS microsite chart. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `week` | integer | Season week. |
| `extraLargeImg` | character | URL to the extra-large player image asset used on the NFL NGS microsite chart display. |
| `playerNameSlug` | character | URL-safe slug version of the player's name used in NGS microsite routing (e.g., 'patrick-mahomes'). |
| `smallImg` | character | URL to the small player image asset used on the NFL NGS microsite chart display. |
| `mediumImg` | character | URL to the medium-sized player image asset used on the NFL NGS microsite chart display. |
| `largeImg` | character | URL to the large player image asset used on the NFL NGS microsite chart display. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushingYards` | integer | Total rushing yards for the player featured on the NGS microsite chart. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `completionPercentage` | double | Completion percentage for the quarterback featured on the NGS microsite chart. |
| `completions` | integer | The number of completed passes. |
| `interceptions` | integer | The number of interceptions thrown. |
| `passerRating` | double | NFL passer rating for the quarterback featured on the NGS microsite chart. |
| `passingYards` | integer | Total passing yards for the player (quarterback) featured on the NGS microsite chart. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_microsite_chart
charts = nfl_ngs_microsite_chart(season=2024, season_type="REG", limit=25)
charts.select(["playerName", "type", "imageName"]).head()
```

### nfl_ngs_microsite_chart_players {#nfl_ngs_microsite_chart_players}

`nfl_ngs_microsite_chart_players(season: 'int' = 2024, season_type: 'str' = 'REG', return_as_pandas: 'bool' = False)`

NGS microsite chart player index -- one row per player with a chart.

Wraps `/api/content/microsite/chart/players`; records live under `players`
and carry `esbId`, `firstName`, `lastName` and `playerName`. Useful as
the lookup list of who has charts available for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per player.

| col_name | type | description |
|---|---|---|
| `esbId` | character | Elias Sports Bureau identifier for the player used in NFL Next Gen Stats microsite chart data. |
| `firstName` | character | Scorer first name (localized list). |
| `lastName` | character | Scorer last name (localized list). |
| `playerName` | character | Full display name of the player as shown in NFL Next Gen Stats microsite charts. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_microsite_chart_players
who = nfl_ngs_microsite_chart_players(season=2024, season_type="REG")
who.select(["playerName", "esbId"]).head()
```
