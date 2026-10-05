---
title: "WNBA — additional Python functions — Wnba"
sidebar_label: "Wnba"
sidebar_position: 3
description: "WNBA — additional Python functions — Wnba — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Wnba

### wnba_aging_curve {#wnba_aging_curve}

`wnba_aging_curve(*, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

WNBA aging curve -- the NBA core bound to `league="wnba"`.

See `sportsdataverse.nba.nba_aging_curve.nba_aging_curve` for the
full contract; this is a by-reference re-export, same algorithm, women's
bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

Frame `age:Int64, rel_value:Float64, peak_age:Float64`.

| col_name | type | description |
|---|---|---|
| `age` | integer | Player age (in years). |
| `rel_value` | double | Value multiplier for this age relative to the peak age: a delta-method curve chaining minutes-weighted within-player consecutive-age changes in per-100-possession box-score value, quadratic-smoothed and min-max scaled to [0.4, 1.0], so the peak age is exactly 1.0 and the lowest-valued age 0.4. |
| `peak_age` | double | Age at which rel_value reaches its maximum of 1.0, repeated on every row for filtering and joining (29.0 in the bundled curve). |

**Example**

```python
from sportsdataverse.wnba import wnba_aging_curve
curve = wnba_aging_curve()
```

### wnba_availability {#wnba_availability}

`wnba_availability(seasons: "'int | list[int]'", *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

WNBA availability -- the NBA core bound to `league="wnba"`.

See `sportsdataverse.nba.nba_availability.nba_availability` for the
full contract; `avail_pct` is availability, not skill.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (start year) or list of seasons. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, season:Int64, avail_pct:Float64`.

**Example**

```python
from sportsdataverse.wnba import wnba_availability
proj = wnba_availability(2023)
```

### wnba_career_trajectory {#wnba_career_trajectory}

`wnba_career_trajectory(player_values: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

WNBA career trajectory -- the NBA core bound to `league="wnba"`.

See `sportsdataverse.nba.nba_aging_curve.nba_career_trajectory` for
the full contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_values` | `DataFrame` |  | Frame `player_id, age:Int64, value:Float64`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`player_values` plus `age_adjusted_value` and `proj_next_value`.

**Example**

```python
import polars as pl
from sportsdataverse.wnba import wnba_career_trajectory
player_values = pl.DataFrame({"player_id": ["1"], "age": [26], "value": [10.0]})
wnba_career_trajectory(player_values)
```

### wnba_draft_model {#wnba_draft_model}

`wnba_draft_model(draft_year: "'int | list[int]'", *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Project WNBA prospect career value + draft probability from draft slot.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year (e.g. `2023`) or list of years. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_career_value:Float64, draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8`. Empty input returns the zero-row schema, never raises.

**Example**

```python
from sportsdataverse.wnba import wnba_draft_model
board = wnba_draft_model(2023)
```

### wnba_enhanced_pbp {#wnba_enhanced_pbp}

`wnba_enhanced_pbp(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return a normalised enhanced play-by-play frame for a WNBA game.

Fetches the raw `playbyplayv3` payload from `stats.wnba.com` via
`~sportsdataverse.wnba.wnba_stats.wnba_stats_playbyplayv3` then
delegates all transformation to the league-agnostic
`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`
core with `league_id="10"`.  Never raises on malformed or empty
payloads — returns a zero-row frame instead.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema `sportsdataverse.nba.nba_enhanced_pbp.ENHANCED_PBP_SCHEMA`. Key columns include `game_id` (Utf8), `action_number` (Int64), `period` (Int64), `seconds_remaining` (Float64), `team_id` (Int64), `person_id` (Int64), `is_substitution` (Boolean), and one Boolean flag per event type.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_enhanced_pbp
df = wnba_enhanced_pbp("1022400001")
print(df.shape)

# Pandas output

df_pd = wnba_enhanced_pbp("1022400001", return_as_pandas=True)
print(type(df_pd))

# Filter substitution events

subs = df.filter(df["is_substitution"] == True)  # noqa: E712
print(subs.select(["period", "seconds_remaining", "person_id"]))
```

### wnba_expected_turnovers {#wnba_expected_turnovers}

`wnba_expected_turnovers(season: 'str', *, base: "'Optional[pl.DataFrame]'" = None, player_mix: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA expected turnovers / ball-security skill (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_expected_turnovers.nba_expected_turnovers`
to the women's league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base`) frame. |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_expected_turnovers.nba_expected_turnovers`.

**Example**

```python
from sportsdataverse.wnba import wnba_expected_turnovers
t = wnba_expected_turnovers("2024")
print(t.sort("ball_security_skill", descending=True).head())
```

### wnba_foul_drawing {#wnba_foul_drawing}

`wnba_foul_drawing(season: 'str', *, base: "'Optional[pl.DataFrame]'" = None, advanced: "'Optional[pl.DataFrame]'" = None, player_mix: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA foul-drawing / FT-generation (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_foul_drawing.nba_foul_drawing` to the
women's league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base`) frame. |
| `advanced` | `Optional[DataFrame]` | `None` | Injected `Advanced`-measure frame. |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_foul_drawing.nba_foul_drawing`.

**Example**

```python
from sportsdataverse.wnba import wnba_foul_drawing
f = wnba_foul_drawing("2024")
print(f.sort("foul_draw_skill", descending=True).head())
```

### wnba_in_game_win_prob {#wnba_in_game_win_prob}

`wnba_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

WNBA in-game win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_in_game_win_prob.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  |  |
| `pregame_home_prob` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### wnba_live_boxscore {#wnba_live_boxscore}

`wnba_live_boxscore(game_id: 'str | int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict[str, str] | None' = None) -> 'Any'`

Fetch and parse WNBA cdn.wnba.com liveData boxscore for a game.

Retrieves `https://cdn.wnba.com/static/json/liveData/boxscore/boxscore_{game_id}.json`
and parses it via `sportsdataverse.nba.nba_live.parse_nba_live_boxscore`
into six tables (game, officials, home/away players, home/away team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str \| int` |  | WNBA game ID (int or str). Zero-padded to 10 digits. |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict[str, str] \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict. Otherwise, a dict of six DataFrames (`game`, `officials`, `home_players`, `away_players`, `home_team`, `away_team`) parsed by `sportsdataverse.nba.nba_live.parse_nba_live_boxscore`. Their core columns are guaranteed on every frame, even a zero-row one, at their declared dtypes: `game_id` on all six, plus `game_status`, `game_time_utc`, `home_team_id`, `away_team_id`, and `attendance` on `game`; `person_id`, `name`, `jersey_num`, and `assignment` on `officials`; `team_id`, `person_id`, `name`, `jersey_num`, `position`, `starter`, and `played` on the player frames; and `team_id`, `team_tricode`, and `score` on the team frames. Every other liveData field is passed through, snake-cased, with each nested `statistics` object flattened into `statistics_*` columns; a field only some players carry, such as `not_playing_reason`, is present only when a player on that side has it.

| col_name | type | description |
|---|---|---|
| `game.game_id` | character | 10-digit NBA/WNBA game id (zero-padded), from the payload's game.gameId. |
| `game.game_status` | integer | Numeric game status code from the feed: 1 scheduled, 2 in progress, 3 final. |
| `game.game_time_utc` | character | Scheduled tip-off time in UTC (ISO-8601 timestamp). |
| `game.home_team_id` | integer | NBA/WNBA team id of the home team, taken from the payload's homeTeam object. |
| `game.away_team_id` | integer | NBA/WNBA team id of the away team, taken from the payload's awayTeam object. |
| `game.attendance` | integer | Reported attendance figure for the game, when published by the feed. |
| `game.game_time_local` | character | Scheduled tip-off in the arena's local time, ISO-8601 with UTC offset, e.g. 2026-06-13T18:00:00-04:00. |
| `game.game_time_home` | character | Scheduled tip-off in the home team's local time, ISO-8601 with UTC offset; the same as game_time_local on the capture, whose two teams share the arena's time zone. |
| `game.game_time_away` | character | Scheduled tip-off in the away team's local time, ISO-8601 with UTC offset; the same as game_time_local on the capture, whose two teams share the arena's time zone. |
| `game.game_et` | character | Scheduled tip-off in US Eastern time, ISO-8601 with UTC offset, e.g. 2026-06-13T18:00:00-04:00. |
| `game.duration` | integer | Wall-clock length of the game in whole minutes, opening tip to final buzzer, e.g. 127 on the capture, whose first and last play-by-play actions are 2h07m apart. |
| `game.game_code` | character | Game code written as the game date (YYYYMMDD), a slash, then the away and home team tricodes, e.g. 20260613/INDCON. |
| `game.game_status_text` | character | Game status as display text, e.g. Final. |
| `game.regulation_periods` | integer | Number of regulation periods in the game, 4. |
| `game.period` | integer | Current period, or the last period played on a final game; 5 and up are overtime periods (4 on the capture, which had no overtime). |
| `game.game_clock` | character | Time left in the current period as an ISO-8601 duration, e.g. PT00M00.00S on a final game. |
| `game.sellout` | character | Sellout flag as a string, "1" when the game sold out; "1" on the capture. |
| `officials.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) this officiating crew worked. |
| `officials.person_id` | integer | NBA/WNBA person id of the on-court official. |
| `officials.name` | character | Official's display name. |
| `officials.jersey_num` | character | Official's jersey number as a string. |
| `officials.assignment` | character | Crew role label from the feed (OFFICIAL1, OFFICIAL2 or OFFICIAL3, listed in no fixed order). |
| `officials.name_i` | character | Official's first initial and last name, e.g. K. Fahy. |
| `officials.first_name` | character | Official's first name as the feed writes it. |
| `officials.family_name` | character | Official's last name as the feed writes it. |
| `home_players.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) for this side's player entry. |
| `home_players.team_id` | integer | NBA/WNBA team id of the side (home or away) the player belongs to. |
| `home_players.person_id` | integer | NBA/WNBA player id. |
| `home_players.name` | character | Player's display name. |
| `home_players.jersey_num` | character | Player's jersey number as a string. |
| `home_players.position` | character | Starting-lineup slot, one each of SF, PF, C, SG, and PG across the five starters (order 1-5) rather than the player's roster position; null for non-starters. |
| `home_players.starter` | character | Feed's starter flag as a string ("1"/"0") for whether the player started the game. |
| `home_players.played` | character | Feed's flag as a string for whether the player recorded any playing time in the game ("1"/"0"). |
| `home_players.status` | character | Roster status; ACTIVE on every player in the capture, including those who did not play (see played and not_playing_reason). |
| `home_players.order` | integer | Player's place in the feed's roster listing for the team, from 1; the five starters are 1-5 (SF, PF, C, SG, PG), then the rest of the roster. |
| `home_players.oncourt` | character | Flag as a string ("1"/"0") for whether the player is on the floor as of the capture; on a final game, the five on the floor at the final buzzer. |
| `home_players.name_i` | character | Player's first initial and last name, e.g. D. Miller or H. Van Lith. |
| `home_players.first_name` | character | Player's first name as the feed writes it. |
| `home_players.family_name` | character | Player's last name as the feed writes it, e.g. Nelson-Ododa or Van Lith. |
| `home_players.statistics_assists` | integer | Assists credited to the player. |
| `home_players.statistics_blocks` | integer | Opponent shots the player blocked. |
| `home_players.statistics_blocks_received` | integer | Player's shot attempts that an opponent blocked. |
| `home_players.statistics_field_goals_attempted` | integer | Field-goal attempts, statistics_two_pointers_attempted plus statistics_three_pointers_attempted. |
| `home_players.statistics_field_goals_made` | integer | Field goals made, statistics_two_pointers_made plus statistics_three_pointers_made. |
| `home_players.statistics_field_goals_percentage` | double | Field-goal percentage as a 0-1 fraction, statistics_field_goals_made / statistics_field_goals_attempted; 0 when the player took no shot. |
| `home_players.statistics_fouls_offensive` | integer | Offensive fouls committed, also counted in statistics_fouls_personal. |
| `home_players.statistics_fouls_drawn` | integer | Fouls opponents committed on the player. |
| `home_players.statistics_fouls_personal` | integer | Personal fouls committed, offensive fouls included; technical fouls excluded. |
| `home_players.statistics_fouls_technical` | integer | Technical fouls charged to the player; a defensive three-seconds technical is charged to the team (the team row's statistics_fouls_team_technical), even when the play-by-play names the player. |
| `home_players.statistics_free_throws_attempted` | integer | Free-throw attempts by the player. |
| `home_players.statistics_free_throws_made` | integer | Free throws the player made. |
| `home_players.statistics_free_throws_percentage` | double | Free-throw percentage as a 0-1 fraction, statistics_free_throws_made / statistics_free_throws_attempted; 0 when the player attempted none. |
| `home_players.statistics_minus` | double | Points the opponent scored while the player was on the floor, as a float (e.g. 46.0); 0.0 for a player who did not play. |
| `home_players.statistics_minutes` | character | Playing time as an ISO-8601 duration to the hundredth of a second, e.g. PT32M09.10S; PT00M00.00S for a player who did not play. |
| `home_players.statistics_minutes_calculated` | character | Playing time in whole minutes as an ISO-8601 duration, e.g. PT17M; usually statistics_minutes rounded to the nearest minute, but adjusted so the team's players add up to the team's statistics_minutes_calculated (an 8:36 stint shows PT08M). |
| `home_players.statistics_plus` | double | Points the player's team scored while the player was on the floor, as a float (e.g. 29.0); 0.0 for a player who did not play. |
| `home_players.statistics_plus_minus_points` | double | Plus-minus, statistics_plus minus statistics_minus, as a float (e.g. -17.0). |
| `home_players.statistics_points` | integer | Points the player scored. |
| `home_players.statistics_points_fast_break` | integer | Fast-break points, free throws included. |
| `home_players.statistics_points_in_the_paint` | integer | Points on field goals made in the paint. |
| `home_players.statistics_points_second_chance` | integer | Second-chance points, free throws included. |
| `home_players.statistics_rebounds_defensive` | integer | Defensive rebounds the player grabbed. |
| `home_players.statistics_rebounds_offensive` | integer | Offensive rebounds the player grabbed. |
| `home_players.statistics_rebounds_total` | integer | Total rebounds, statistics_rebounds_offensive plus statistics_rebounds_defensive. |
| `home_players.statistics_steals` | integer | Steals credited to the player. |
| `home_players.statistics_three_pointers_attempted` | integer | Three-point field-goal attempts. |
| `home_players.statistics_three_pointers_made` | integer | Three-point field goals made. |
| `home_players.statistics_three_pointers_percentage` | double | Three-point percentage as a 0-1 fraction, statistics_three_pointers_made / statistics_three_pointers_attempted; 0 when the player attempted none. |
| `home_players.statistics_turnovers` | integer | Turnovers charged to the player. |
| `home_players.statistics_two_pointers_attempted` | integer | Two-point field-goal attempts. |
| `home_players.statistics_two_pointers_made` | integer | Two-point field goals made. |
| `home_players.statistics_two_pointers_percentage` | double | Two-point percentage as a 0-1 fraction, statistics_two_pointers_made / statistics_two_pointers_attempted; 0 when the player attempted none. |
| `home_players.not_playing_reason` | character | Code for why the player did not play, e.g. DND_COACH (the only code on the capture); null for everyone else, including some players who sat out with no stated reason. The column is present only when a player on that side has one. |
| `home_players.not_playing_description` | character | Free-text detail for not_playing_reason; "DP" on every DND_COACH row of the capture and null elsewhere. The column is present only when a player on that side has one. |
| `away_players.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) for this side's player entry. |
| `away_players.team_id` | integer | NBA/WNBA team id of the side (home or away) the player belongs to. |
| `away_players.person_id` | integer | NBA/WNBA player id. |
| `away_players.name` | character | Player's display name. |
| `away_players.jersey_num` | character | Player's jersey number as a string. |
| `away_players.position` | character | Starting-lineup slot, one each of SF, PF, C, SG, and PG across the five starters (order 1-5) rather than the player's roster position; null for non-starters. |
| `away_players.starter` | character | Feed's starter flag as a string ("1"/"0") for whether the player started the game. |
| `away_players.played` | character | Feed's flag as a string for whether the player recorded any playing time in the game ("1"/"0"). |
| `away_players.status` | character | Roster status; ACTIVE on every player in the capture, including those who did not play (see played and not_playing_reason). |
| `away_players.order` | integer | Player's place in the feed's roster listing for the team, from 1; the five starters are 1-5 (SF, PF, C, SG, PG), then the rest of the roster. |
| `away_players.oncourt` | character | Flag as a string ("1"/"0") for whether the player is on the floor as of the capture; on a final game, the five on the floor at the final buzzer. |
| `away_players.name_i` | character | Player's first initial and last name, e.g. D. Miller or H. Van Lith. |
| `away_players.first_name` | character | Player's first name as the feed writes it. |
| `away_players.family_name` | character | Player's last name as the feed writes it, e.g. Nelson-Ododa or Van Lith. |
| `away_players.statistics_assists` | integer | Assists credited to the player. |
| `away_players.statistics_blocks` | integer | Opponent shots the player blocked. |
| `away_players.statistics_blocks_received` | integer | Player's shot attempts that an opponent blocked. |
| `away_players.statistics_field_goals_attempted` | integer | Field-goal attempts, statistics_two_pointers_attempted plus statistics_three_pointers_attempted. |
| `away_players.statistics_field_goals_made` | integer | Field goals made, statistics_two_pointers_made plus statistics_three_pointers_made. |
| `away_players.statistics_field_goals_percentage` | double | Field-goal percentage as a 0-1 fraction, statistics_field_goals_made / statistics_field_goals_attempted; 0 when the player took no shot. |
| `away_players.statistics_fouls_offensive` | integer | Offensive fouls committed, also counted in statistics_fouls_personal. |
| `away_players.statistics_fouls_drawn` | integer | Fouls opponents committed on the player. |
| `away_players.statistics_fouls_personal` | integer | Personal fouls committed, offensive fouls included; technical fouls excluded. |
| `away_players.statistics_fouls_technical` | integer | Technical fouls charged to the player; a defensive three-seconds technical is charged to the team (the team row's statistics_fouls_team_technical), even when the play-by-play names the player. |
| `away_players.statistics_free_throws_attempted` | integer | Free-throw attempts by the player. |
| `away_players.statistics_free_throws_made` | integer | Free throws the player made. |
| `away_players.statistics_free_throws_percentage` | double | Free-throw percentage as a 0-1 fraction, statistics_free_throws_made / statistics_free_throws_attempted; 0 when the player attempted none. |
| `away_players.statistics_minus` | double | Points the opponent scored while the player was on the floor, as a float (e.g. 46.0); 0.0 for a player who did not play. |
| `away_players.statistics_minutes` | character | Playing time as an ISO-8601 duration to the hundredth of a second, e.g. PT32M09.10S; PT00M00.00S for a player who did not play. |
| `away_players.statistics_minutes_calculated` | character | Playing time in whole minutes as an ISO-8601 duration, e.g. PT17M; usually statistics_minutes rounded to the nearest minute, but adjusted so the team's players add up to the team's statistics_minutes_calculated (an 8:36 stint shows PT08M). |
| `away_players.statistics_plus` | double | Points the player's team scored while the player was on the floor, as a float (e.g. 29.0); 0.0 for a player who did not play. |
| `away_players.statistics_plus_minus_points` | double | Plus-minus, statistics_plus minus statistics_minus, as a float (e.g. -17.0). |
| `away_players.statistics_points` | integer | Points the player scored. |
| `away_players.statistics_points_fast_break` | integer | Fast-break points, free throws included. |
| `away_players.statistics_points_in_the_paint` | integer | Points on field goals made in the paint. |
| `away_players.statistics_points_second_chance` | integer | Second-chance points, free throws included. |
| `away_players.statistics_rebounds_defensive` | integer | Defensive rebounds the player grabbed. |
| `away_players.statistics_rebounds_offensive` | integer | Offensive rebounds the player grabbed. |
| `away_players.statistics_rebounds_total` | integer | Total rebounds, statistics_rebounds_offensive plus statistics_rebounds_defensive. |
| `away_players.statistics_steals` | integer | Steals credited to the player. |
| `away_players.statistics_three_pointers_attempted` | integer | Three-point field-goal attempts. |
| `away_players.statistics_three_pointers_made` | integer | Three-point field goals made. |
| `away_players.statistics_three_pointers_percentage` | double | Three-point percentage as a 0-1 fraction, statistics_three_pointers_made / statistics_three_pointers_attempted; 0 when the player attempted none. |
| `away_players.statistics_turnovers` | integer | Turnovers charged to the player. |
| `away_players.statistics_two_pointers_attempted` | integer | Two-point field-goal attempts. |
| `away_players.statistics_two_pointers_made` | integer | Two-point field goals made. |
| `away_players.statistics_two_pointers_percentage` | double | Two-point percentage as a 0-1 fraction, statistics_two_pointers_made / statistics_two_pointers_attempted; 0 when the player attempted none. |
| `away_players.not_playing_reason` | character | Code for why the player did not play, e.g. DND_COACH (the only code on the capture); null for everyone else, including some players who sat out with no stated reason. The column is present only when a player on that side has one. |
| `away_players.not_playing_description` | character | Free-text detail for not_playing_reason; "DP" on every DND_COACH row of the capture and null elsewhere. The column is present only when a player on that side has one. |
| `home_team.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) for this side's team entry. |
| `home_team.team_id` | integer | NBA/WNBA team id of the side (home or away). |
| `home_team.team_tricode` | character | Three-letter team code, e.g. LAS or NYL. |
| `home_team.score` | integer | Team's current or final score. |
| `home_team.team_name` | character | Team nickname, e.g. Sun or Fever. |
| `home_team.team_city` | character | Team's location label, a city or a state, e.g. Connecticut or Indiana. |
| `home_team.in_bonus` | character | Flag as a string ("1"/"0") for whether the team is in the bonus as of the capture, i.e. the opponent has reached the team-foul penalty in the current period (the last period on a final game). |
| `home_team.timeouts_remaining` | integer | Timeouts the team has left as of the capture (at the end of the game on a final). |
| `home_team.statistics_assists` | integer | Team assists, the players' assists summed. |
| `home_team.statistics_assists_turnover_ratio` | double | Assists divided by statistics_turnovers_total (team turnovers included), e.g. 24 / 9 = 2.6667. |
| `home_team.statistics_bench_points` | integer | Points scored by the players who did not start. |
| `home_team.statistics_biggest_lead` | integer | Team's largest lead in points during the game. |
| `home_team.statistics_biggest_lead_score` | character | Score when the team first reached its biggest lead, written away-home, e.g. "47-36" for the away team's 11-point lead. |
| `home_team.statistics_biggest_scoring_run` | integer | Team's longest run of unanswered points in the game; on the capture the home team's value (11, at 45-36) matches no home-team run in the play-by-play, whose longest is 7, and 45-36 falls inside the away team's 13-0 run. |
| `home_team.statistics_biggest_scoring_run_score` | character | Score when the team's longest run ended, written away-home, e.g. "47-36" (see statistics_biggest_scoring_run for the home team's value on the capture). |
| `home_team.statistics_blocks` | integer | Opponent shots the team blocked. |
| `home_team.statistics_blocks_received` | integer | Team's shot attempts that were blocked, equal to the opponent's blocks. |
| `home_team.statistics_fast_break_points_attempted` | integer | Fast-break field-goal attempts; free throws are not counted. |
| `home_team.statistics_fast_break_points_made` | integer | Fast-break field goals made; free throws are not counted. |
| `home_team.statistics_fast_break_points_percentage` | double | statistics_fast_break_points_made / statistics_fast_break_points_attempted as a 0-1 fraction. |
| `home_team.statistics_field_goals_attempted` | integer | Field-goal attempts by the team's players, the sum of the player rows. |
| `home_team.statistics_field_goals_effective_adjusted` | double | Effective field-goal percentage as a 0-1 fraction, (statistics_field_goals_made + 0.5 * statistics_three_pointers_made) / statistics_field_goals_attempted. |
| `home_team.statistics_field_goals_made` | integer | Field goals made by the team's players. |
| `home_team.statistics_field_goals_percentage` | double | Field-goal percentage as a 0-1 fraction, statistics_field_goals_made / statistics_field_goals_attempted. |
| `home_team.statistics_fouls_offensive` | integer | Offensive fouls committed, also counted in statistics_fouls_personal. |
| `home_team.statistics_fouls_drawn` | integer | Fouls drawn, equal to the opponent's statistics_fouls_personal on the capture. |
| `home_team.statistics_fouls_personal` | integer | Personal fouls committed by the team's players, offensive fouls included and technical fouls excluded. |
| `home_team.statistics_fouls_team` | integer | Team fouls, statistics_fouls_personal minus statistics_fouls_offensive on the capture. |
| `home_team.statistics_fouls_technical` | integer | Technical fouls charged to the team's players; a defensive three-seconds technical counts in statistics_fouls_team_technical instead, even when the play-by-play names the player. |
| `home_team.statistics_fouls_team_technical` | integer | Technical fouls charged to the team rather than a player, e.g. a delay-of-game or defensive three-seconds technical. |
| `home_team.statistics_free_throws_attempted` | integer | Free-throw attempts by the team's players. |
| `home_team.statistics_free_throws_made` | integer | Free throws made by the team's players. |
| `home_team.statistics_free_throws_percentage` | double | Free-throw percentage as a 0-1 fraction, statistics_free_throws_made / statistics_free_throws_attempted. |
| `home_team.statistics_lead_changes` | integer | Number of lead changes in the game, the same on both teams' rows. |
| `home_team.statistics_minutes` | character | Total playing time of the team's players as an ISO-8601 duration, five times the game length, e.g. PT200M00.00S for a 40-minute game. |
| `home_team.statistics_minutes_calculated` | character | Total playing time in whole minutes as an ISO-8601 duration, e.g. PT200M; the players' statistics_minutes_calculated add up to it. |
| `home_team.statistics_points` | integer | Team points, equal to score on the capture. |
| `home_team.statistics_points_against` | integer | Points the opponent scored. |
| `home_team.statistics_points_fast_break` | integer | Fast-break points, free throws included. |
| `home_team.statistics_points_from_turnovers` | integer | Points scored off the opponent's turnovers, free throws included. |
| `home_team.statistics_points_in_the_paint` | integer | Points on field goals made in the paint. |
| `home_team.statistics_points_in_the_paint_attempted` | integer | Field-goal attempts in the paint. |
| `home_team.statistics_points_in_the_paint_made` | integer | Field goals made in the paint. |
| `home_team.statistics_points_in_the_paint_percentage` | double | statistics_points_in_the_paint_made / statistics_points_in_the_paint_attempted as a 0-1 fraction. |
| `home_team.statistics_points_second_chance` | integer | Second-chance points, free throws included. |
| `home_team.statistics_rebounds_defensive` | integer | Defensive rebounds by the team's players; team rebounds are in statistics_rebounds_team_defensive. |
| `home_team.statistics_rebounds_offensive` | integer | Offensive rebounds by the team's players; team rebounds are in statistics_rebounds_team_offensive. |
| `home_team.statistics_rebounds_personal` | integer | Rebounds by the team's players, statistics_rebounds_defensive plus statistics_rebounds_offensive. |
| `home_team.statistics_rebounds_team` | integer | Team rebounds credited to the team rather than a player, statistics_rebounds_team_defensive plus statistics_rebounds_team_offensive. |
| `home_team.statistics_rebounds_team_defensive` | integer | Defensive rebounds credited to the team rather than a player. |
| `home_team.statistics_rebounds_team_offensive` | integer | Offensive rebounds credited to the team rather than a player. |
| `home_team.statistics_rebounds_total` | integer | All rebounds, statistics_rebounds_personal plus statistics_rebounds_team. |
| `home_team.statistics_second_chance_points_attempted` | integer | Second-chance field-goal attempts; free throws are not counted. |
| `home_team.statistics_second_chance_points_made` | integer | Second-chance field goals made; free throws are not counted. |
| `home_team.statistics_second_chance_points_percentage` | double | statistics_second_chance_points_made / statistics_second_chance_points_attempted as a 0-1 fraction. |
| `home_team.statistics_steals` | integer | Steals by the team's players. |
| `home_team.statistics_team_field_goal_attempts` | integer | Field-goal attempts credited to the team rather than a player and left out of statistics_field_goals_attempted; 0 on both teams in the capture. |
| `home_team.statistics_three_pointers_attempted` | integer | Three-point field-goal attempts by the team's players. |
| `home_team.statistics_three_pointers_made` | integer | Three-point field goals made by the team's players. |
| `home_team.statistics_three_pointers_percentage` | double | Three-point percentage as a 0-1 fraction, statistics_three_pointers_made / statistics_three_pointers_attempted. |
| `home_team.statistics_time_leading` | character | Game-clock time the team held the lead, as an ISO-8601 duration, e.g. PT31M23.00S. |
| `home_team.statistics_times_tied` | integer | Number of times the score was tied after 0-0, the same on both teams' rows. |
| `home_team.statistics_true_shooting_attempts` | double | True-shooting attempts, statistics_field_goals_attempted + 0.44 * statistics_free_throws_attempted. |
| `home_team.statistics_true_shooting_percentage` | double | True-shooting percentage as a 0-1 fraction, statistics_points / (2 * statistics_true_shooting_attempts). |
| `home_team.statistics_turnovers` | integer | Turnovers by the team's players; team turnovers are in statistics_turnovers_team. |
| `home_team.statistics_turnovers_team` | integer | Turnovers charged to the team rather than a player, e.g. a shot-clock or 8-second violation. |
| `home_team.statistics_turnovers_total` | integer | All turnovers, statistics_turnovers plus statistics_turnovers_team. |
| `home_team.statistics_two_pointers_attempted` | integer | Two-point field-goal attempts by the team's players. |
| `home_team.statistics_two_pointers_made` | integer | Two-point field goals made by the team's players. |
| `home_team.statistics_two_pointers_percentage` | double | Two-point percentage as a 0-1 fraction, statistics_two_pointers_made / statistics_two_pointers_attempted. |
| `away_team.game_id` | character | 10-digit NBA/WNBA game id (zero-padded) for this side's team entry. |
| `away_team.team_id` | integer | NBA/WNBA team id of the side (home or away). |
| `away_team.team_tricode` | character | Three-letter team code, e.g. LAS or NYL. |
| `away_team.score` | integer | Team's current or final score. |
| `away_team.team_name` | character | Team nickname, e.g. Sun or Fever. |
| `away_team.team_city` | character | Team's location label, a city or a state, e.g. Connecticut or Indiana. |
| `away_team.in_bonus` | character | Flag as a string ("1"/"0") for whether the team is in the bonus as of the capture, i.e. the opponent has reached the team-foul penalty in the current period (the last period on a final game). |
| `away_team.timeouts_remaining` | integer | Timeouts the team has left as of the capture (at the end of the game on a final). |
| `away_team.statistics_assists` | integer | Team assists, the players' assists summed. |
| `away_team.statistics_assists_turnover_ratio` | double | Assists divided by statistics_turnovers_total (team turnovers included), e.g. 24 / 9 = 2.6667. |
| `away_team.statistics_bench_points` | integer | Points scored by the players who did not start. |
| `away_team.statistics_biggest_lead` | integer | Team's largest lead in points during the game. |
| `away_team.statistics_biggest_lead_score` | character | Score when the team first reached its biggest lead, written away-home, e.g. "47-36" for the away team's 11-point lead. |
| `away_team.statistics_biggest_scoring_run` | integer | Team's longest run of unanswered points in the game; on the capture the home team's value (11, at 45-36) matches no home-team run in the play-by-play, whose longest is 7, and 45-36 falls inside the away team's 13-0 run. |
| `away_team.statistics_biggest_scoring_run_score` | character | Score when the team's longest run ended, written away-home, e.g. "47-36" (see statistics_biggest_scoring_run for the home team's value on the capture). |
| `away_team.statistics_blocks` | integer | Opponent shots the team blocked. |
| `away_team.statistics_blocks_received` | integer | Team's shot attempts that were blocked, equal to the opponent's blocks. |
| `away_team.statistics_fast_break_points_attempted` | integer | Fast-break field-goal attempts; free throws are not counted. |
| `away_team.statistics_fast_break_points_made` | integer | Fast-break field goals made; free throws are not counted. |
| `away_team.statistics_fast_break_points_percentage` | double | statistics_fast_break_points_made / statistics_fast_break_points_attempted as a 0-1 fraction. |
| `away_team.statistics_field_goals_attempted` | integer | Field-goal attempts by the team's players, the sum of the player rows. |
| `away_team.statistics_field_goals_effective_adjusted` | double | Effective field-goal percentage as a 0-1 fraction, (statistics_field_goals_made + 0.5 * statistics_three_pointers_made) / statistics_field_goals_attempted. |
| `away_team.statistics_field_goals_made` | integer | Field goals made by the team's players. |
| `away_team.statistics_field_goals_percentage` | double | Field-goal percentage as a 0-1 fraction, statistics_field_goals_made / statistics_field_goals_attempted. |
| `away_team.statistics_fouls_offensive` | integer | Offensive fouls committed, also counted in statistics_fouls_personal. |
| `away_team.statistics_fouls_drawn` | integer | Fouls drawn, equal to the opponent's statistics_fouls_personal on the capture. |
| `away_team.statistics_fouls_personal` | integer | Personal fouls committed by the team's players, offensive fouls included and technical fouls excluded. |
| `away_team.statistics_fouls_team` | integer | Team fouls, statistics_fouls_personal minus statistics_fouls_offensive on the capture. |
| `away_team.statistics_fouls_technical` | integer | Technical fouls charged to the team's players; a defensive three-seconds technical counts in statistics_fouls_team_technical instead, even when the play-by-play names the player. |
| `away_team.statistics_fouls_team_technical` | integer | Technical fouls charged to the team rather than a player, e.g. a delay-of-game or defensive three-seconds technical. |
| `away_team.statistics_free_throws_attempted` | integer | Free-throw attempts by the team's players. |
| `away_team.statistics_free_throws_made` | integer | Free throws made by the team's players. |
| `away_team.statistics_free_throws_percentage` | double | Free-throw percentage as a 0-1 fraction, statistics_free_throws_made / statistics_free_throws_attempted. |
| `away_team.statistics_lead_changes` | integer | Number of lead changes in the game, the same on both teams' rows. |
| `away_team.statistics_minutes` | character | Total playing time of the team's players as an ISO-8601 duration, five times the game length, e.g. PT200M00.00S for a 40-minute game. |
| `away_team.statistics_minutes_calculated` | character | Total playing time in whole minutes as an ISO-8601 duration, e.g. PT200M; the players' statistics_minutes_calculated add up to it. |
| `away_team.statistics_points` | integer | Team points, equal to score on the capture. |
| `away_team.statistics_points_against` | integer | Points the opponent scored. |
| `away_team.statistics_points_fast_break` | integer | Fast-break points, free throws included. |
| `away_team.statistics_points_from_turnovers` | integer | Points scored off the opponent's turnovers, free throws included. |
| `away_team.statistics_points_in_the_paint` | integer | Points on field goals made in the paint. |
| `away_team.statistics_points_in_the_paint_attempted` | integer | Field-goal attempts in the paint. |
| `away_team.statistics_points_in_the_paint_made` | integer | Field goals made in the paint. |
| `away_team.statistics_points_in_the_paint_percentage` | double | statistics_points_in_the_paint_made / statistics_points_in_the_paint_attempted as a 0-1 fraction. |
| `away_team.statistics_points_second_chance` | integer | Second-chance points, free throws included. |
| `away_team.statistics_rebounds_defensive` | integer | Defensive rebounds by the team's players; team rebounds are in statistics_rebounds_team_defensive. |
| `away_team.statistics_rebounds_offensive` | integer | Offensive rebounds by the team's players; team rebounds are in statistics_rebounds_team_offensive. |
| `away_team.statistics_rebounds_personal` | integer | Rebounds by the team's players, statistics_rebounds_defensive plus statistics_rebounds_offensive. |
| `away_team.statistics_rebounds_team` | integer | Team rebounds credited to the team rather than a player, statistics_rebounds_team_defensive plus statistics_rebounds_team_offensive. |
| `away_team.statistics_rebounds_team_defensive` | integer | Defensive rebounds credited to the team rather than a player. |
| `away_team.statistics_rebounds_team_offensive` | integer | Offensive rebounds credited to the team rather than a player. |
| `away_team.statistics_rebounds_total` | integer | All rebounds, statistics_rebounds_personal plus statistics_rebounds_team. |
| `away_team.statistics_second_chance_points_attempted` | integer | Second-chance field-goal attempts; free throws are not counted. |
| `away_team.statistics_second_chance_points_made` | integer | Second-chance field goals made; free throws are not counted. |
| `away_team.statistics_second_chance_points_percentage` | double | statistics_second_chance_points_made / statistics_second_chance_points_attempted as a 0-1 fraction. |
| `away_team.statistics_steals` | integer | Steals by the team's players. |
| `away_team.statistics_team_field_goal_attempts` | integer | Field-goal attempts credited to the team rather than a player and left out of statistics_field_goals_attempted; 0 on both teams in the capture. |
| `away_team.statistics_three_pointers_attempted` | integer | Three-point field-goal attempts by the team's players. |
| `away_team.statistics_three_pointers_made` | integer | Three-point field goals made by the team's players. |
| `away_team.statistics_three_pointers_percentage` | double | Three-point percentage as a 0-1 fraction, statistics_three_pointers_made / statistics_three_pointers_attempted. |
| `away_team.statistics_time_leading` | character | Game-clock time the team held the lead, as an ISO-8601 duration, e.g. PT31M23.00S. |
| `away_team.statistics_times_tied` | integer | Number of times the score was tied after 0-0, the same on both teams' rows. |
| `away_team.statistics_true_shooting_attempts` | double | True-shooting attempts, statistics_field_goals_attempted + 0.44 * statistics_free_throws_attempted. |
| `away_team.statistics_true_shooting_percentage` | double | True-shooting percentage as a 0-1 fraction, statistics_points / (2 * statistics_true_shooting_attempts). |
| `away_team.statistics_turnovers` | integer | Turnovers by the team's players; team turnovers are in statistics_turnovers_team. |
| `away_team.statistics_turnovers_team` | integer | Turnovers charged to the team rather than a player, e.g. a shot-clock or 8-second violation. |
| `away_team.statistics_turnovers_total` | integer | All turnovers, statistics_turnovers plus statistics_turnovers_team. |
| `away_team.statistics_two_pointers_attempted` | integer | Two-point field-goal attempts by the team's players. |
| `away_team.statistics_two_pointers_made` | integer | Two-point field goals made by the team's players. |
| `away_team.statistics_two_pointers_percentage` | double | Two-point percentage as a 0-1 fraction, statistics_two_pointers_made / statistics_two_pointers_attempted. |

**Example**

```python
from sportsdataverse.wnba.wnba_live import wnba_live_boxscore
result = wnba_live_boxscore("1022600097")
officials = result["officials"]
print(officials.select("person_id", "name", "assignment"))
```

### wnba_live_pbp {#wnba_live_pbp}

`wnba_live_pbp(game_id: 'str | int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict[str, str] | None' = None) -> 'Any'`

Fetch and parse WNBA cdn.wnba.com liveData play-by-play for a game.

Retrieves `https://cdn.wnba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json`
and parses it via `sportsdataverse.nba.nba_live.parse_nba_live_pbp`. Carries
the same per-whistle referee ids (`official_id`) and wall-clock timestamps
(`time_actual`) as the NBA feed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str \| int` |  | WNBA game ID (int or str). Zero-padded to 10 digits. |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) instead of a DataFrame. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame instead of polars. |
| `proxy` | `dict[str, str] \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict. Otherwise, a DataFrame with one row per action, parsed by `sportsdataverse.nba.nba_live.parse_nba_live_pbp`. Its 13 core columns (`game_id`, `action_number`, `period`, `clock`, `time_actual`, `action_type`, `sub_type`, `team_id`, `person_id`, `official_id`, `x_legacy`, `y_legacy`, `description`) are guaranteed on every frame, even a zero-row one, at their declared dtypes. Every other liveData action field is passed through, snake-cased, when the payload carries it, so an event-specific column such as `block_person_id` or `foul_drawn_person_id` is present only when the game had that event.

| col_name | type | description |
|---|---|---|
| `game_id` | character | 10-digit NBA/WNBA game id (zero-padded), stamped onto every action from the payload's game.gameId. |
| `action_number` | integer | Action id from the liveData feed's actionNumber field, assigned in the order actions are logged rather than strictly in game order (an action logged late gets a higher number than plays that followed it); sort by order_number for game order. |
| `period` | integer | Period of the game: 1-4 for quarters, 5+ for overtime periods. |
| `clock` | character | ISO-8601 duration game clock at the action, e.g. "PT11M25.00S", not yet converted to MM:SS. |
| `time_actual` | character | Wall-clock UTC timestamp when the action occurred, letting plays be matched to real elapsed time. |
| `action_type` | character | Action category from the feed, lower-cased, e.g. 2pt, 3pt, foul, substitution, or turnover. |
| `sub_type` | character | Action sub-type as the feed writes it, e.g. personal or offensive for a foul, Jump Shot or Layup for a 2pt/3pt shot, 1 of 2 for a free throw. |
| `team_id` | integer | NBA/WNBA team id of the team associated with the action, when applicable. |
| `person_id` | integer | NBA/WNBA player id of the primary person involved in the action, when applicable. |
| `official_id` | integer | Referee's person id for the whistle on this action; populated on every foul since the 2019-20 season, and also on whistled turnovers and violations such as traveling, out-of-bounds, or kicked ball (null on live-ball turnovers like a bad pass). |
| `x_legacy` | double | Shot x-coordinate in the legacy stats.nba.com coordinate system; populated only on 2pt/3pt shots (fouls and blocks carry a court zone in area/area_detail instead). |
| `y_legacy` | double | Shot y-coordinate in the legacy stats.nba.com coordinate system; populated only on 2pt/3pt shots (fouls and blocks carry a court zone in area/area_detail instead). |
| `description` | character | Long-form human-readable description of the action, as shown on NBA.com's live scoreboard. |
| `period_type` | character | Period kind from the feed, REGULAR for the four quarters and OVERTIME for an overtime period. |
| `possession` | integer | Team id of the team with the ball as of the action (on a rebound or steal, the team that gained it); 0 on game and period start/end rows. |
| `score_home` | character | Home team's running score after the action, stored as a string. |
| `score_away` | character | Away team's running score after the action, stored as a string. |
| `order_number` | integer | The feed's sort key for the action (actions ship in this order); sorting by it gives game order even where action_number does not, since an action logged late is slotted in at its place in the game. |
| `edited` | character | UTC timestamp (ISO-8601, whole seconds) of the feed's last edit to the action, never earlier than time_actual. |
| `qualifiers` | list | List of context tags on the action, e.g. pointsinthepaint, fastbreak, 2ndchance, or fromturnover on a shot, 2freethrow or inpenalty on a foul, and team on a team rebound or turnover; empty when none apply. |
| `descriptor` | character | Extra detail on sub_type, e.g. driving or step back on a shot, shooting or loose ball on a foul, bad pass on a turnover, heldball or startperiod on a jump ball; null when absent. |
| `is_target_score_last_period` | logical | Feed flag for a last period played to a target score instead of the game clock (the Elam-ending format); False on every action of a regular game. |
| `team_tricode` | character | Three-letter code of the team in team_id, e.g. CON or IND. |
| `player_name` | character | Last name of the action's primary player (person_id) as the feed writes it, e.g. Boston; null on team and administrative actions. |
| `player_name_i` | character | First initial and last name of the action's primary player (person_id), e.g. A. Boston; null on team and administrative actions. |
| `person_ids_filter` | list | List of every player id on the action, i.e. person_id plus any assist, block, steal, foul-drawn, or jump-ball participant; empty when no player is involved. |
| `is_field_goal` | integer | Field-goal attempt flag, 1 on a 2pt/3pt shot and 0 on every other action. |
| `shot_result` | character | Made or Missed; set on 2pt/3pt shots and free throws. |
| `shot_distance` | double | Shot distance from the basket in feet; set on 2pt/3pt shots. |
| `x` | double | Shot location along the court's length on a 0-100 scale from the left baseline; populated only on 2pt/3pt shots, like x_legacy. |
| `y` | double | Shot location across the court's width on a 0-100 scale, 50 at the baskets' centerline; populated only on 2pt/3pt shots, like y_legacy. |
| `side` | character | Court half of the shot, left or right (left where x is below 50); populated only on 2pt/3pt shots. |
| `area` | character | Court zone of the action, e.g. Restricted Area, In The Paint (Non-RA), Mid-Range, Left Corner 3, Right Corner 3, or Above the Break 3; set on 2pt/3pt shots and also on fouls, blocks, steals, turnovers, and most rebounds, which carry no x/y coordinates. |
| `area_detail` | character | Second zone label for the same actions as area; the WNBA feed writes zone names like those in area (e.g. Mid-Range, Left Corner 3), not always the same zone as area on a given action. |
| `points_total` | integer | Scorer's running point total in the game, including this basket; set on made shots and made free throws. |
| `assist_person_id` | integer | Player id credited with the assist on a made 2pt/3pt shot; null on unassisted and missed shots. |
| `assist_player_name_initial` | character | First initial and last name of the player credited with the assist on a made 2pt/3pt shot, e.g. A. Edwards. |
| `assist_total` | integer | Assisting player's running assist total in the game, including this assist. |
| `shot_action_number` | integer | On a rebound, the action_number of the missed shot or free throw being rebounded. |
| `rebound_total` | integer | Rebounder's running rebound total in the game, including this rebound (rebound_offensive_total plus rebound_defensive_total); null on a team rebound. |
| `rebound_offensive_total` | integer | Rebounder's running offensive-rebound total in the game as of this rebound; null on a team rebound. |
| `rebound_defensive_total` | integer | Rebounder's running defensive-rebound total in the game as of this rebound; null on a team rebound. |
| `foul_drawn_person_id` | integer | Player id of the opponent who drew the foul; null when no player drew it (e.g. a technical foul). |
| `foul_drawn_player_name` | character | Last name of the opponent who drew the foul; null when no player drew it (e.g. a technical foul). |
| `foul_personal_total` | integer | Fouling player's running personal-foul count in the game as of this foul (a technical foul leaves it unchanged); null on a team foul. |
| `foul_technical_total` | integer | Fouling player's running technical-foul count in the game as of this foul; null on a team foul. |
| `turnover_total` | integer | Player's running turnover count in the game, including this turnover; null on a team turnover. |
| `steal_person_id` | integer | Player id credited with the steal, on the turnover row it forced; the steal is also logged as its own steal action. |
| `steal_player_name` | character | Last name of the player credited with the steal, on the turnover row it forced. |
| `block_person_id` | integer | Player id credited with the block, on the blocked (missed) shot's row; the block is also logged as its own block action. |
| `block_player_name` | character | Last name of the player credited with the block, on the blocked (missed) shot's row. |
| `jump_ball_won_person_id` | integer | Player id of the jumper who won the tip on a jump ball. |
| `jump_ball_won_player_name` | character | Last name of the jumper who won the tip on a jump ball. |
| `jump_ball_lost_person_id` | integer | Player id of the jumper who lost the tip on a jump ball. |
| `jump_ball_lost_player_name` | character | Last name of the jumper who lost the tip on a jump ball. |
| `jump_ball_recoverd_person_id` | integer | Player id of the player who recovered the tip on a jump ball, null when a team recovered it; the column keeps the feed's own spelling (jumpBallRecoverdPersonId). |
| `jump_ball_recovered_name` | character | First initial and last name of the player who recovered the tip on a jump ball, e.g. L. Lacan, or a team label when a team recovered it. |

**Example**

```python
from sportsdataverse.wnba.wnba_live import wnba_live_pbp
pbp = wnba_live_pbp("1022600097")
print(pbp.filter(pbp["action_type"] == "foul").height)
```

### wnba_matchup_drapm {#wnba_matchup_drapm}

`wnba_matchup_drapm(season: 'str', *, matchups: "'Optional[pl.DataFrame]'" = None, config: "'Optional[PlaytypeConfig]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA matchup defensive RAPM (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm` to the
women's league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `matchups` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leagueseasonmatchups`-shaped frame. |
| `config` | `Optional[PlaytypeConfig]` | `None` | `~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig` override. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm`.

**Example**

```python
from sportsdataverse.wnba import wnba_matchup_drapm
d = wnba_matchup_drapm("2024")
print(d.sort("matchup_drapm", descending=True).head())
```

### wnba_on_court {#wnba_on_court}

`wnba_on_court(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return the rotation-keyed on-court player frame for a WNBA game.

Makes three network calls (play-by-play v3, game rotation,
box-score traditional v3), infers on-court rosters from the rotation
stints via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
and returns one row per PBP action with ten Int64 player-ID columns
(`home_player_1..5` / `away_player_1..5`).  All transformation is
performed by the shared `nba/` core with `league_id="10"` forwarded
to the rotation endpoint.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per PBP action and columns `home_player_1` … `home_player_5`, `away_player_1` … `away_player_5` (all Int64), plus the `action_number` join key.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_on_court
oc = wnba_on_court("1022400001")
print(oc.select(["action_number", "home_player_1"]).head())

# Pandas output

oc_pd = wnba_on_court("1022400001", return_as_pandas=True)
print(type(oc_pd))

# Join on enhanced PBP

from sportsdataverse.wnba.wnba_engine import wnba_enhanced_pbp
enh = wnba_enhanced_pbp("1022400001")
joined = enh.join(oc, on="action_number", how="left")
```

### wnba_pbp_disk {#wnba_pbp_disk}

`wnba_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

### wnba_play_context {#wnba_play_context}

`wnba_play_context(game_id: 'str', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return a WNBA game's possessions with the full CTG play-context surface.

WNBA sibling of
`~sportsdataverse.nba.nba_play_context.nba_play_context` — the
Cleaning the Glass recreation (possession start-type taxonomy + the
halfcourt / transition / putback contexts + CTG's garbage-time and heave
filters). One network call (`playbyplayv3` on `stats.wnba.com`); every
transformation is done by the league-agnostic
`~sportsdataverse.nba.nba_play_context.add_play_context` core, so
there is **no WNBA-specific classification logic** to drift.

Two caveats worth stating plainly:

* **CTG is NBA-only.** There is no published WNBA play-context table to
  calibrate against, so `transition_seconds` inherits the NBA's fitted
  6.0 s default. The WNBA fixtures land inside the NBA's transition-frequency
  gate at that value (`tests/wnba/test_wnba_play_context_shim.py`), which
  is a sanity check on the shared engine — not evidence that 6.0 s is the
  *right* WNBA cutoff. Re-fit it if a WNBA oracle ever appears.
* Shot-zone boundaries are league-agnostic (feet from the rim), and the
  corner-three test uses the same legacy coordinates, which the WNBA feed
  also ships.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier (e.g. `"1022400001"`). |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff, in seconds. |
| `transition_variant` | `str` | `'hoop_math'` | See `~sportsdataverse.nba.nba_play_context.add_transition`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The possession frame (`POSSESSIONS_SCHEMA`) plus `~sportsdataverse.nba.nba_play_context.PLAY_CONTEXT_POSSESSIONS_SCHEMA`. Empty or malformed payloads return a zero-row frame — never raises on payload content.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_play_context
poss = wnba_play_context("1022400001")
print(poss["possession_start_type_ctg"].value_counts())

# Transition rate (CTG's default filtered view)

import polars as pl
clean = poss.filter(
    (pl.col("is_garbage_time") == False)  # noqa: E712
    & (pl.col("is_heave_possession") == False)  # noqa: E712
)
print(clean["is_transition"].mean())
```

### wnba_player_crosswalk {#wnba_player_crosswalk}

`wnba_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source player crosswalk (ESPN / WNBA Stats / Fox).

One row per ESPN athlete per team. `match_method` / `match_confidence`
describe the **Stats API** match (normalized exact name, then
Jaro-Winkler with jersey and DOB tiebreaks); Fox contributes
`fox_athlete_id` only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WNBA season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 21 columns.

**Example**

```python
from sportsdataverse.wnba import wnba_player_crosswalk
df = wnba_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = wnba_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### wnba_player_props {#wnba_player_props}

`wnba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

WNBA player props (league_id='10'). See sportsdataverse.nba.nba_player_props.nba_player_props.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  |  |
| `game_id` | `str` |  |  |
| `home_team_id` | `str` |  |  |
| `away_team_id` | `str` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### wnba_playtype_ratings {#wnba_playtype_ratings}

`wnba_playtype_ratings(season: 'str', *, off_team: "'Optional[pl.DataFrame]'" = None, def_team: "'Optional[pl.DataFrame]'" = None, schedule: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA Synergy play-type-adjusted offense/defense (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_playtype.nba_playtype_ratings` to the
women's league. Synergy coverage is sparse for the WNBA; an empty upstream
fetch degrades to a zero-row frame (never raises).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_playtype.nba_playtype_ratings`.

**Example**

```python
from sportsdataverse.wnba import wnba_playtype_ratings
r = wnba_playtype_ratings("2024")
print(r.sort("adj_off", descending=True).head())
```

### wnba_possessions {#wnba_possessions}

`wnba_possessions(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return the possession-level lineup stint matrix for a WNBA game.

Builds possessions from the enhanced PBP via
`~sportsdataverse.nba.nba_possessions.build_possessions`, resolves
on-court rosters via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
then attaches the 5v5 lineups via
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.
All transformation is performed by the shared `nba/` cores — no WNBA-
specific logic.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | WNBA game identifier string (e.g. `"1022400001"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema combining `POSSESSIONS_SCHEMA` and ten lineup columns: `off_player_1` … `off_player_5`, `def_player_1` … `def_player_5` (all Int64). One row per possession. Empty or malformed inputs return a zero-row frame.

**Example**

```python
from sportsdataverse.wnba.wnba_engine import wnba_possessions
poss = wnba_possessions("1022400001")
print(poss.shape)

# Pandas output

poss_pd = wnba_possessions("1022400001", return_as_pandas=True)
print(type(poss_pd))

# Total points check

total = int(poss["points"].sum())
print(f"Total points scored: {total}")
```

### wnba_predict_games {#wnba_predict_games}

`wnba_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

WNBA vectorized pregame predictions (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_predict_games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  |  |
| `ratings` | `DataFrame` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### wnba_predict_margin {#wnba_predict_margin}

`wnba_predict_margin(home_net: 'float', away_net: 'float', *, home_pace: 'float', away_pace: 'float', neutral: 'bool' = False, league_id: 'str' = '00') -> 'float'`

WNBA expected margin (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_margin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_net` | `float` |  |  |
| `away_net` | `float` |  |  |
| `home_pace` | `float` |  |  |
| `away_pace` | `float` |  |  |
| `neutral` | `bool` | `False` |  |
| `league_id` | `str` | `'00'` |  |

### wnba_predict_total {#wnba_predict_total}

`wnba_predict_total(home_off: 'float', home_def: 'float', away_off: 'float', away_def: 'float', home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'`

WNBA expected total (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_total.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  |  |
| `home_def` | `float` |  |  |
| `away_off` | `float` |  |  |
| `away_def` | `float` |  |  |
| `home_pace` | `float` |  |  |
| `away_pace` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |
