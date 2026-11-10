---
title: "NBA — additional Python functions — Models and calculators: player_game–win_prob"
sidebar_label: "Models and calculators: player_game–win_prob"
sidebar_position: 13
description: "NBA — additional Python functions — Models and calculators: player_game–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: player_game–win_prob

### player_game_rates {#player_game_rates}

`player_game_rates(player_box: 'pl.DataFrame', team_box: 'pl.DataFrame', *, periods: 'Optional[int]' = None, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None) -> 'pl.DataFrame'`

True-shooting and usage for every boxed player of one game.

TS% = PTS / (2 (FGA + 0.44 FTA)); USG% = 100 ((FGA + 0.44 FTA + TOV) (team minutes / 5)) /
(MIN (team FGA + 0.44 team FTA + team TOV)) with team TOV as in `team_game_rates`
(`total_turnovers`, repaired where ESPN doubled or omitted it) and team
minutes = 5 x game minutes (regulation 48 NBA / 40 WNBA, MBB and WBB, 5 per overtime). For MBB and
WBB team TOV follows the college rule of `team_game_rates` (null in WBB before 2009 and in a
WBB game whose two teams together show fewer than 5, so usage is null there). A player with no shots, no minutes,
or `did_not_play` gets `null`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_box` | `DataFrame` |  | One game in the `load_nba_player_boxscore` schema. |
| `team_box` | `DataFrame` |  | The game's two `load_nba_team_boxscore` rows. |
| `periods` | `Optional[int]` | `None` | Highest period played (default: the regulation periods). |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods (2 or 4); default `game_regulation_periods` for the league and the team box's `season`. |

**Returns**

One row per `(game_id, athlete_id)` with `team_id, ts_pct, usg_pct`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id, the join key to the play-by-play and box-score frames. |
| `athlete_id` | Int64 | ESPN athlete id, the join key to the player box score. |
| `team_id` | Int64 | ESPN team id, the join key to the team box score. |
| `ts_pct` | Float64 | True-shooting percentage, PTS / (2 x (FGA + 0.44 x FTA)); null for a player with no shots, no minutes or a DNP. |
| `usg_pct` | Float64 | Usage rate, 100 x (FGA + 0.44 x FTA + TOV) x (team minutes / 5) / (MIN x team (FGA + 0.44 x FTA + total turnovers)); null for no minutes, a DNP, or a team whose turnovers were not recorded (WBB before 2009, or a WBB game whose two teams together show fewer than 5 turnovers). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `team_box` does not hold exactly two different teams of one `game_id` (a duplicated team row would double the player rows), or the league is unknown, or `league` contradicts `league_id`. |
| `polars.exceptions.ColumnNotFoundError` | If a required box-score column is missing. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import player_game_rates
from sportsdataverse.nba.nba_loaders import load_nba_player_boxscore, load_nba_team_boxscore
team_box = load_nba_team_boxscore([2025]).filter(pl.col("game_id") == 401766458)
player_box = load_nba_player_boxscore([2025]).filter(pl.col("game_id") == 401766458)
player_game_rates(player_box, team_box).sort("usg_pct", descending=True).head()
```

### player_game_splits {#player_game_splits}

`player_game_splits(pbp: 'pl.DataFrame', player_box: 'Optional[pl.DataFrame]' = None, *, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None) -> 'pl.DataFrame'`

Per-player shot, assist and turnover splits of one game, counted from its play-by-play (works mid-game).

Counted from the plays the player is the first athlete on (`athlete_id_1`): field goals (rim,
three and their assisted makes, as in `team_game_splits`), turnovers (`live_tov` with a
steal credited), rebounds and points (made shots and free throws); `assists` are the made
shots crediting the player as the passer (`athlete_id_2` on a make whose text names an assist).
`est_possessions` is an estimate, not an on-court count (that needs the lineup on each play):
the team's counted possessions times the player's share of the team's minutes (the box's
`minutes` over the team's minutes / 5), so a mid-game box gives the share so far. The per-100
rates (`pts`, `ast`, `reb`, `tov`) divide by it; they are null without minutes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game's plays, as `team_game_splits` takes them (plus `athlete_id_1` / `athlete_id_2`). |
| `player_box` | `Optional[DataFrame]` | `None` | The game's player box (`athlete_id`, `team_id`, `minutes`); every boxed player gets one row (a play crediting a player to the other team is left out); without a box, every player on the plays gets one, with null minutes. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods; see `team_game_splits`. |

**Returns**

One row per player (`game_id`, `athlete_id`, `team_id`, `minutes`, `est_possessions`, the shot, assist, rebound and turnover counts and shares, and the four `<m>_per100` rates); an empty frame with those columns when there are no plays and no box.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id, the join key to the play-by-play and box-score frames. |
| `athlete_id` | Int64 | ESPN athlete id, the join key to the player box score. |
| `team_id` | Int64 | ESPN team id, the join key to the team box score. |
| `minutes` | Float64 | Minutes from the player box (null without a box). |
| `est_possessions` | Float64 | Estimated possessions on the floor: the team's counted possessions times the player's share of the team's minutes (minutes / (team minutes / 5)); not an on-court count. |
| `points` | Int64 | Points from the player's made shots and free throws on the plays. |
| `fga` | Int64 | Field-goal attempts. |
| `fgm` | Int64 | Field goals made. |
| `assisted_fgm` | Int64 | Made field goals whose play text credits an assist. |
| `assisted_share` | Float64 | Share of the player's makes that were assisted. |
| `rim_fga` | Int64 | Attempts at the rim (twos under 4 feet). |
| `rim_fgm` | Int64 | Makes at the rim. |
| `rim_assisted_fgm` | Int64 | Assisted makes at the rim. |
| `rim_assisted_share` | Float64 | Share of makes at the rim that were assisted; null with none. |
| `fg3a` | Int64 | Three-point attempts. |
| `fg3m` | Int64 | Threes made. |
| `fg3_assisted_fgm` | Int64 | Assisted threes. |
| `fg3_assisted_share` | Float64 | Share of made threes that were assisted; null with none. |
| `assists` | Int64 | Made field goals crediting the player as the passer. |
| `rebounds` | Int64 | Rebounds credited to the player on the plays (team rebounds excluded). |
| `tov` | Int64 | Turnovers credited to the player. |
| `live_tov` | Int64 | The player's turnovers with a steal credited. |
| `pts_per100` | Float64 | Points per 100 estimated possessions; null without minutes. |
| `ast_per100` | Float64 | Assists per 100 estimated possessions; null without minutes. |
| `reb_per100` | Float64 | Rebounds per 100 estimated possessions; null without minutes. |
| `tov_per100` | Float64 | Turnovers per 100 estimated possessions; null without minutes. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the league is unknown or `pbp` holds more than one game. |
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import game_page_frames
from sportsdataverse.wnba.wnba_pbp import espn_wnba_pbp
frames = game_page_frames(espn_wnba_pbp(401726992, return_parsed=False), league="wnba")
frames["player_game_splits"].sort("est_possessions", descending=True).head()

# Who scored at the rim off passes

frames["player_game_splits"].select("athlete_id", "rim_fgm", "rim_assisted_share")
```

### predict_margin {#predict_margin}

`predict_margin(home_net: 'float', away_net: 'float', *, home_pace: 'float', away_pace: 'float', neutral: 'bool' = False, league_id: 'str' = '00') -> 'float'`

Expected home-minus-away margin from two adjusted net ratings.

The AdjNet difference (points/100 possessions) is scaled by the
matchup's `expected_possessions` before the home-court advantage
is added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_net` | `float` |  | Home team's adjusted net rating (`adj_net_rtg`). |
| `away_net` | `float` |  | Away team's adjusted net rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `neutral` | `bool` | `False` | True for a neutral-site game (no home-court advantage). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted HFA. |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_margin
predict_margin(10.0, -2.0, home_pace=100.0, away_pace=98.0, neutral=False)
```

### predict_total {#predict_total}

`predict_total(home_off: 'float', home_def: 'float', away_off: 'float', away_def: 'float', home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'`

Expected total points from adjusted ratings and paces.

Expected possessions come from `expected_possessions`; each side's
expected points per 100 possessions blend its offense with the
opponent's defense (`0.5 * (off + opp_def)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  | Home adjusted offensive rating (points/100 poss). |
| `home_def` | `float` |  | Home adjusted defensive rating. |
| `away_off` | `float` |  | Away adjusted offensive rating. |
| `away_def` | `float` |  | Away adjusted defensive rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the pace anchor. |

**Returns**

Expected combined points scored by both teams.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_total
predict_total(118.0, 108.0, 110.0, 112.0, 100.0, 98.0)
```

### raw_game_efficiency {#raw_game_efficiency}

`raw_game_efficiency(schedule: 'pl.DataFrame', team_box: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-team, per-game possessions + raw offensive/defensive rating.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | Frame with `game_id, season, date, home_team_id, away_team_id, neutral_site` (ids cast to `Utf8` here). |
| `team_box` | `DataFrame` |  | Per-team box score with `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. |

**Returns**

One row per (game_id, team_id): `game_id, season, date, team_id, opp_team_id, is_home, neutral_site, poss, off_rtg, def_rtg`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int64 | Season year. |
| `date` | String | Date in YYYY-MM-DD format. |
| `team_id` | String | Unique team identifier. |
| `opp_team_id` | String |  |
| `is_home` | Boolean | Whether the team was home. |
| `neutral_site` | Boolean | Neutral site. |
| `poss` | Float64 | Poss. |
| `off_rtg` | Float64 |  |
| `def_rtg` | Float64 |  |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_schedule, load_nba_team_boxscore
from sportsdataverse.nba.nba_team_ratings import raw_game_efficiency
eff = raw_game_efficiency(load_nba_schedule([2024]), load_nba_team_boxscore([2024]))
```

### render_report {#render_report}

`render_report(report: 'ValidationReport') -> 'str'`

Render a `ValidationReport` as a human-readable markdown validation card.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `report` | `ValidationReport` |  | A populated `ValidationReport` from `validate_model`. |

**Returns**

A multi-section markdown string with one `##` heading per oracle. Sections whose oracle result is `None` (either skipped or not applicable for a point-estimate model) are rendered as `- n/a`.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model, render_report,
)

rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
md = render_report(rep)
print(md)

# Capture the markdown string for downstream use

with open("validation_card.md", "w") as f:
    f.write(render_report(rep))
```

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

### team_game_splits {#team_game_splits}

`team_game_splits(pbp: 'pl.DataFrame', player_box: 'Optional[pl.DataFrame]' = None, *, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None) -> 'pl.DataFrame'`

Situational splits for both teams of one game, counted from its play-by-play (works mid-game).

Possessions are counted, not estimated: a field-goal attempt, a free throw, a turnover or a
rebound names the offense (a defensive rebound is the new offense's first event), and a
possession runs until the next such play is the other team's or the period ends. On the
fixtures the count runs 0.95-1.01 of the box estimate (FGA - ORB + TOV + 0.44 FTA) in the pros
and 0.99-1.05 in college. A technical free throw neither starts nor ends a possession, and its
points land in `non_possession_points` when the shooting team was on defense. `points` is every point the
team scored (the play-by-play's running score), `ppp` is `points / possessions`, and each
split pair partitions the possessions and their points: `transition` + `half_court` and
`second_chance` + `first_chance` both add up to `possessions` and to `points -
non_possession_points`. A **transition** possession started live (after a made shot, a defensive
rebound or a steal; not a period's first) and took its first field-goal attempt within
`TRANSITION_WINDOW_SECONDS` of the change of possession, with no timeout in between. A
**second-chance** possession had an offensive rebound, team rebounds included (not the dead-ball
one between two free throws of a trip, one after a technical free throw, or one with no time
left). A turnover is **live** when a steal is credited on it, otherwise **dead**;
`pts_off_live_tov` / `pts_off_dead_tov` are the points the team scored on the possessions
that started from the opponent's live / dead turnovers. Shot zones read the play coordinates of
`shot_coordinates`: `rim` is a two under 4 feet, `mid` any other located two,
`corner3` a three at or below 8.75 feet above the rim's line (`y_legacy` 87.5), `above_break3`
any other located three; plays without a location count in `fg2_unlocated` / `fg3_unlocated`,
so `rim + mid + fg2_unlocated + fg3 = fga` and `corner3 + above_break3 + fg3_unlocated = fg3`.
A three is `points_attempted` 3; a frame without that column (older captures) reads the
text ("three point", or 24 feet or more) and the location (22 feet or more to the side, or
23.75 out). `layup_dunk` is any shot whose play type names a layup or a dunk. `assisted_fgm` is the makes
whose text credits an assist. `largest_lead` is the team's biggest lead and `max_run` its
biggest unanswered run (`sportsdataverse._common.game_flow`'s rule). `bench_points` needs
the player box's `starter` flag (null without it). ESPN's college feed has no technical-foul
marker on its free throws, so a college technical is counted as a possession of the shooting team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game's plays in the basketball pbp schema, after `shot_coordinates` (needs `type_text`, `text`, `shooting_play`, `scoring_play`, `points_attempted` if present, `team_id`, `home_team_id`, `away_team_id`, `home_score`, `away_score`, `period_number`, a period clock and `x_legacy` / `y_legacy`), in play order. |
| `player_box` | `Optional[DataFrame]` | `None` | The game's player box, for `bench_points` (`team_id`, `points`, `starter`). |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods (2 or 4), for the first possession's clock; default `game_regulation_periods` from the frame's `season`. |

**Returns**

One row per team (`game_id`, `team_id`, the totals, the four split families as `<family>_poss` / `_pts` / `_ppp`, the turnover, zone, assist and free-throw columns, `bench_points`, `largest_lead`, `max_run`); an empty frame with those columns before tip-off.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id, the join key to the play-by-play and box-score frames. |
| `team_id` | Int64 | ESPN team id, the join key to the team box score. |
| `points` | Int64 | Every point the team scored, from the play-by-play's running score (the score so far in a live game). |
| `possessions` | Int64 | Counted possessions: runs of one offense in the play-by-play (a field-goal attempt, a non-technical free throw, a turnover or a rebound names the offense); about 3% below the box estimate in the pros. |
| `ppp` | Float64 | Points per possession, points / possessions (ORtg / 100); null with no possessions. |
| `non_possession_points` | Int64 | Points scored while the opponent had the ball (technical free throws), so the split families add up to points minus this. |
| `avg_possession_seconds` | Float64 | Mean game clock from the change of possession to the possession's last event. |
| `transition_poss` | Int64 | Count of transition possessions: a live start (after a made shot, a defensive rebound or a steal; not a period's first) with the first field-goal attempt within 8 seconds (pros) or 10 (college) of the change of possession and no timeout between. |
| `transition_pts` | Int64 | Points scored on the transition possessions. |
| `transition_ppp` | Float64 | Points per transition possession; null with none. |
| `half_court_poss` | Int64 | Count of half-court possessions: every possession that is not transition. |
| `half_court_pts` | Int64 | Points scored on the half-court possessions. |
| `half_court_ppp` | Float64 | Points per half-court possession; null with none. |
| `second_chance_poss` | Int64 | Count of second-chance possessions: those with an offensive rebound (not the dead-ball one between two free throws of a trip, after a technical free throw, or with no time left). |
| `second_chance_pts` | Int64 | Points scored on the second-chance possessions. |
| `second_chance_ppp` | Float64 | Points per second-chance possession; null with none. |
| `first_chance_poss` | Int64 | Count of first-chance possessions: those with no offensive rebound. |
| `first_chance_pts` | Int64 | Points scored on the first-chance possessions. |
| `first_chance_ppp` | Float64 | Points per first-chance possession; null with none. |
| `tov` | Int64 | Turnovers counted from the plays (team turnovers included). |
| `live_tov` | Int64 | Live-ball turnovers: those with a steal credited (the steal in the turnover text, or college's own Steal play next to it). |
| `dead_tov` | Int64 | Dead-ball turnovers: tov - live_tov. |
| `tov_pct` | Float64 | Turnovers per counted possession; null with no possessions. |
| `pts_off_live_tov` | Int64 | Points the team scored on possessions that started from the opponent's live-ball turnovers. |
| `pts_off_dead_tov` | Int64 | Points the team scored on possessions that started from the opponent's dead-ball turnovers. |
| `fga` | Int64 | Field-goal attempts on the plays. |
| `fgm` | Int64 | Field goals made on the plays. |
| `rim_fga` | Int64 | Attempts: twos under 4 feet from the rim (play coordinates). |
| `rim_fgm` | Int64 | Makes: twos under 4 feet from the rim (play coordinates). |
| `rim_fg_pct` | Float64 | Field-goal percentage: twos under 4 feet from the rim (play coordinates); null with no attempts. |
| `mid_fga` | Int64 | Attempts: located twos 4 feet or more from the rim. |
| `mid_fgm` | Int64 | Makes: located twos 4 feet or more from the rim. |
| `mid_fg_pct` | Float64 | Field-goal percentage: located twos 4 feet or more from the rim; null with no attempts. |
| `fg2_unlocated_fga` | Int64 | Attempts: twos without a play location. |
| `fg2_unlocated_fgm` | Int64 | Makes: twos without a play location. |
| `fg2_unlocated_fg_pct` | Float64 | Field-goal percentage: twos without a play location; null with no attempts. |
| `fg3_fga` | Int64 | Attempts: threes. |
| `fg3_fgm` | Int64 | Makes: threes. |
| `fg3_fg_pct` | Float64 | Field-goal percentage: threes; null with no attempts. |
| `corner3_fga` | Int64 | Attempts: located threes at or below 8.75 feet above the rim line (y_legacy 87.5). |
| `corner3_fgm` | Int64 | Makes: located threes at or below 8.75 feet above the rim line (y_legacy 87.5). |
| `corner3_fg_pct` | Float64 | Field-goal percentage: located threes at or below 8.75 feet above the rim line (y_legacy 87.5); null with no attempts. |
| `above_break3_fga` | Int64 | Attempts: located threes above the break. |
| `above_break3_fgm` | Int64 | Makes: located threes above the break. |
| `above_break3_fg_pct` | Float64 | Field-goal percentage: located threes above the break; null with no attempts. |
| `fg3_unlocated_fga` | Int64 | Attempts: threes without a play location. |
| `fg3_unlocated_fgm` | Int64 | Makes: threes without a play location. |
| `fg3_unlocated_fg_pct` | Float64 | Field-goal percentage: threes without a play location; null with no attempts. |
| `layup_dunk_fga` | Int64 | Attempts: shots whose play type names a layup or a dunk. |
| `layup_dunk_fgm` | Int64 | Makes: shots whose play type names a layup or a dunk. |
| `layup_dunk_fg_pct` | Float64 | Field-goal percentage: shots whose play type names a layup or a dunk; null with no attempts. |
| `rim_rate` | Float64 | Share of field-goal attempts at the rim, rim_fga / fga. |
| `fg3a_rate` | Float64 | Share of field-goal attempts from three, fg3_fga / fga. |
| `avg_fg2_distance` | Float64 | Mean distance in feet of the located two-point attempts. |
| `assisted_fgm` | Int64 | Made field goals whose play text credits an assist. |
| `assisted_share` | Float64 | Share of made field goals that were assisted, assisted_fgm / fgm. |
| `fta` | Int64 | Free-throw attempts on the plays (technicals included). |
| `ftm` | Int64 | Free throws made on the plays. |
| `ft_rate` | Float64 | Free-throw rate, fta / fga. |
| `bench_points` | Int64 | Points by players the box does not mark as starters; null without a starter flag. |
| `largest_lead` | Int64 | The team's biggest lead in points (0 if never ahead). |
| `max_run` | Int64 | The team's biggest unanswered scoring run in points. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the league is unknown or `pbp` holds more than one game. |
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import game_page_frames
from sportsdataverse.nba.nba_pbp import espn_nba_pbp
frames = game_page_frames(espn_nba_pbp(401766458, return_parsed=False))
frames["team_game_splits"].select("team_id", "possessions", "ppp", "transition_ppp")

# From warehouse frames

import polars as pl
from sportsdataverse.nba.nba_game_page import shot_coordinates, team_game_splits
from sportsdataverse.wbb.wbb_loaders import load_wbb_pbp, load_wbb_player_boxscore
pbp = load_wbb_pbp([2024]).filter(pl.col("game_id") == 401638645)
box = load_wbb_player_boxscore([2024]).filter(pl.col("game_id") == 401638645)
team_game_splits(shot_coordinates(pbp), box, league="wbb")
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
