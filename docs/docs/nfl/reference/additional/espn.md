---
title: "NFL — additional Python functions — ESPN"
sidebar_label: "ESPN"
sidebar_position: 1
description: "NFL — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — ESPN

### espn_nfl_calendar {#espn_nfl_calendar}

`espn_nfl_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_calendar - look up the NFL calendar for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `ondays` | `boolean` | `None` | Used to return dates for calendar ondays |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

One row per calendar week of the requested season (`week`, `week_label`, `week_start_date`, `season_type`, ...); `season` is the `season` argument as passed (null when omitted). With `ondays=True` it is a different frame: one row per date with games (`dates`, `dateURL`, `url`).

| col_name | type | description |
|---|---|---|
| `week_label` | String | Week label as ESPN's calendar names it, e.g. Week 1. |
| `week_alternate_label` | String | Alternate week label ESPN ships next to week_label; usually the same text. |
| `week_detail` | String | Date span of the week as display text. |
| `week` | String | Season week. |
| `week_start_date` | String | Start of the week window, an ISO 8601 UTC timestamp string. |
| `week_end_date` | String | End of the week window, an ISO 8601 UTC timestamp string (the last minute before the next week). |
| `season_type_label` | String | Name of the season segment the week belongs to, e.g. Regular Season. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `season_type_start_date` | String | Start of that season segment, an ISO 8601 UTC timestamp string. |
| `season_type_end_date` | String | End of that season segment, an ISO 8601 UTC timestamp string. |
| `season` | Int64 | The season argument as passed (null when the call omits it); ondays=True returns a different frame (dates, dateURL, url). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season` is less than 2002. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_calendar
cal = espn_nfl_calendar(season=2024)

# Fetch the ondays-style flattened schedule URLs

on_days = espn_nfl_calendar(season=2024, ondays=True)

# Pandas round-trip

cal_pd = espn_nfl_calendar(season=2024, return_as_pandas=True)
```

### espn_nfl_game_rosters {#espn_nfl_game_rosters}

`espn_nfl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame | pd.DataFrame | dict'`

espn_nfl_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nfl_schedule(). |
| `raw` | `bool` | `False` | If True, returns the Core v2 competitors payload (dict) as ESPN sent it; its roster, team and athlete `$ref` links are not followed. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id' plus the athlete's `position_id`, `position_name`, `position_display_name`, `position_abbreviation` and `position_leaf` (a leaf position also `position_parent_href`). With `raw=True`, the competitors payload (dict).

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 |  |
| `athlete_uid` | String |  |
| `athlete_guid` | String |  |
| `athlete_type` | String |  |
| `first_name` | String | First name of player |
| `last_name` | String | Last name of player |
| `full_name` | String | Full name as per NFL.com |
| `athlete_display_name` | String |  |
| `short_name` | String | Player short name (i.e. "F.Last") |
| `weight` | Float64 | Official weight, in pounds |
| `display_weight` | String |  |
| `height` | Float64 | Official height, in inches |
| `display_height` | String |  |
| `age` | Int64 | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `date_of_birth` | String |  |
| `debut_year` | Int64 |  |
| `slug` | String |  |
| `jersey` | String |  |
| `linked` | Boolean |  |
| `active` | Boolean |  |
| `alternate_ids_sdr` | String |  |
| `birth_place_city` | String |  |
| `birth_place_state` | String |  |
| `birth_place_country` | String |  |
| `headshot_href` | String | Link to ESPN Headshot of Player |
| `headshot_alt` | String |  |
| `projections_href` | String | ESPN API hyperlink reference URL for the player's statistical projection resource. |
| `contracts_href` | String | ESPN API hyperlink reference URL for the full list of the player's historical contract records. |
| `experience_years` | Int64 |  |
| `college_athlete_href` | String | ESPN API hyperlink reference URL for the athlete's college profile resource. |
| `contract_href` | String | ESPN API hyperlink reference URL for the player's full contract resource. |
| `contract_option_type` | Int64 |  |
| `contract_salary` | Int64 |  |
| `contract_bonus` | Int64 | Signing or roster bonus amount (in dollars) associated with the player's current contract. |
| `contract_years_remaining` | Int64 |  |
| `contract_signed_through` | Int64 | Final year of the player's current contract, expressed as a four-digit season year. |
| `contract_season_href` | String | ESPN API hyperlink reference URL for the specific season-level contract detail resource. |
| `contract_team_href` | String | ESPN API hyperlink reference URL for the team associated with the player's contract. |
| `contract_active` | Boolean |  |
| `status_id` | Int64 |  |
| `status_name` | String |  |
| `status_type` | String |  |
| `status_abbreviation` | String |  |
| `position_id` | Int64 | ESPN's numeric identifier of the athlete's listed position (the Core v2 position resource id). |
| `position_name` | String | Name of the athlete's listed position from ESPN's position resource, e.g. Guard or Offensive Lineman. |
| `position_display_name` | String | Display form of the athlete's listed position as ESPN renders it; usually equal to position_name. |
| `position_abbreviation` | String | Short code of the athlete's listed position (e.g. G, F, RW, OL), from ESPN's position resource. |
| `position_leaf` | Boolean | Whether the position is a leaf of ESPN's position tree; False marks a grouping, not a specific role. |
| `position_parent_href` | String | ESPN API URL of the broader position group above a leaf position; null for a grouping position. |
| `contract_salary_remaining` | Int64 |  |
| `draft_display_text` | String |  |
| `draft_round` | Int64 | Round that player was drafted in |
| `draft_year` | Int64 | Year that player was drafted |
| `draft_selection` | Int64 |  |
| `draft_team_href` | String | ESPN API hyperlink reference URL for the team that originally drafted this player. |
| `draft_pick_href` | String | ESPN API hyperlink reference URL for the draft-pick record associated with this player. |
| `hand_type` | String |  |
| `hand_abbreviation` | String |  |
| `hand_display_value` | String |  |
| `starter` | Boolean |  |
| `jersey_right` | String | Secondary or alternate jersey number display string used by ESPN (e.g., for special-game uniforms). |
| `valid` | Boolean |  |
| `did_not_play` | Boolean |  |
| `display_name` | String | Full name of player |
| `athlete_href` | String | ESPN API hyperlink reference URL for the athlete resource. |
| `position_href` | String | ESPN API hyperlink reference URL for the player's positional classification resource. |
| `statistics_href` | String | ESPN API hyperlink reference URL for the player's career statistics resource. |
| `team_id` | Int64 |  |
| `order` | Int64 |  |
| `home_away` | String |  |
| `winner` | Boolean |  |
| `team_guid` | String |  |
| `team_uid` | String |  |
| `team_slug` | String |  |
| `team_location` | String |  |
| `team_name` | String |  |
| `team_nickname` | String |  |
| `team_abbreviation` | String |  |
| `team_display_name` | String |  |
| `team_short_display_name` | String |  |
| `team_color` | String |  |
| `team_alternate_color` | String |  |
| `is_active` | Boolean | Active contract |
| `is_all_star` | Boolean |  |
| `team_alternate_ids_sdr` | String | SportsDataverse SDR alternate identifier for the team, used for cross-source joins. |
| `logo_href` | String |  |
| `logo_dark_href` | String |  |
| `game_id` | Int64 | Ten digit identifier for NFL game. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN has no competitors, roster or athlete resource for the game (404), or every rostered athlete's `$ref` answered 404. A single 404 athlete is left out with a `UserWarning` and the rest of the roster is returned. |
| `AssetFetchError` | A fetch failed (403, 429, 5xx, a dropped connection); never treated as a missing athlete. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_game_rosters
rosters = espn_nfl_game_rosters(game_id=401220403)
rosters.shape

# Pandas round-trip with home/away split

rosters_pd = espn_nfl_game_rosters(game_id=401220403, return_as_pandas=True)
home = rosters_pd[rosters_pd["home_away"] == "home"]
away = rosters_pd[rosters_pd["home_away"] == "away"]
```

### espn_nfl_play_participants {#espn_nfl_play_participants}

`espn_nfl_play_participants(game_id: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, resolve_missing: 'bool' = True, resolve_missing_max: 'int' = 50, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull ESPN per-play participants for an NFL game.

One row per play keyed by `play_id` with `{type}_player_name` /
`{type}_player_id` scalars (first occurrence) and `{type}_player_names`
/ `{type}_player_ids` lists for every participant type ESPN ships
(`passer`, `rusher`, `receiver`, `tackler`, `sacked_by`,
`forced_by`, `pass_defender`, `kicker`, `punter`, `returner`,
`recoverer`, `scorer`, `pat_scorer`, `penalized`, `assisted_by`).
`NFLPlayProcess` runs it on the live path to overwrite the text-extracted
names and ids.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `raw` | `bool` | `False` | If True, returns the raw list of play-items dicts. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |
| `resolve_missing` | `bool` | `True` | Fetch athletes the sidecar omits from their `$ref`. |
| `resolve_missing_max` | `int` | `50` | Cap on those per-athlete requests (default 50). |

**Returns**

Polars (or pandas) DataFrame, one row per play with at least one participant (a play whose `participants[]` is empty has no row); the raw play dicts when `raw=True`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Ten digit identifier for NFL game. |
| `play_id` | Int64 | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `kicker_player_name` | String | String name for the kicker on FG or kickoff. |
| `returner_player_name` | String |  |
| `assisted_by_player_name` | String |  |
| `other_player_name` | String |  |
| `passer_player_name` | String | String name for the player that attempted the pass. |
| `receiver_player_name` | String | String name for the targeted receiver. |
| `rusher_player_name` | String | String name for the player that attempted the run. |
| `tackler_player_name` | String |  |
| `pass_defender_player_name` | String |  |
| `scorer_player_name` | String |  |
| `pat_scorer_player_name` | String |  |
| `penalized_player_name` | String |  |
| `punter_player_name` | String | String name for the punter. |
| `fumbler_player_name` | String |  |
| `forced_by_player_name` | String |  |
| `recoverer_player_name` | String |  |
| `sacked_by_player_name` | String |  |
| `snapper_player_name` | String |  |
| `holder_player_name` | String |  |
| `kicker_player_id` | String | Unique identifier for the kicker on FG or kickoff. |
| `returner_player_id` | String |  |
| `assisted_by_player_id` | String |  |
| `other_player_id` | String |  |
| `passer_player_id` | String | Unique identifier for the player that attempted the pass. |
| `receiver_player_id` | String | Unique identifier for the receiver that was targeted on the pass. |
| `rusher_player_id` | String | Unique identifier for the player that attempted the run. |
| `tackler_player_id` | String |  |
| `pass_defender_player_id` | String |  |
| `scorer_player_id` | String |  |
| `pat_scorer_player_id` | String |  |
| `penalized_player_id` | String |  |
| `punter_player_id` | String | Unique identifier for the punter. |
| `fumbler_player_id` | String |  |
| `forced_by_player_id` | String |  |
| `recoverer_player_id` | String |  |
| `sacked_by_player_id` | String |  |
| `snapper_player_id` | String |  |
| `holder_player_id` | String |  |
| `kicker_position_id` | String |  |
| `returner_position_id` | String |  |
| `assisted_by_position_id` | String |  |
| `other_position_id` | String |  |
| `passer_position_id` | String |  |
| `receiver_position_id` | String |  |
| `rusher_position_id` | String |  |
| `tackler_position_id` | String |  |
| `pass_defender_position_id` | String |  |
| `scorer_position_id` | String |  |
| `pat_scorer_position_id` | String |  |
| `penalized_position_id` | String |  |
| `punter_position_id` | String |  |
| `fumbler_position_id` | String |  |
| `forced_by_position_id` | String |  |
| `recoverer_position_id` | String |  |
| `sacked_by_position_id` | String |  |
| `snapper_position_id` | String |  |
| `holder_position_id` | String |  |
| `kicker_player_names` | List |  |
| `returner_player_names` | List |  |
| `assisted_by_player_names` | List |  |
| `other_player_names` | List |  |
| `passer_player_names` | List |  |
| `receiver_player_names` | List |  |
| `rusher_player_names` | List |  |
| `tackler_player_names` | List |  |
| `pass_defender_player_names` | List |  |
| `scorer_player_names` | List |  |
| `pat_scorer_player_names` | List |  |
| `penalized_player_names` | List |  |
| `punter_player_names` | List |  |
| `fumbler_player_names` | List |  |
| `forced_by_player_names` | List |  |
| `recoverer_player_names` | List |  |
| `sacked_by_player_names` | List |  |
| `snapper_player_names` | List |  |
| `holder_player_names` | List |  |
| `kicker_player_ids` | List |  |
| `returner_player_ids` | List |  |
| `assisted_by_player_ids` | List |  |
| `other_player_ids` | List |  |
| `passer_player_ids` | List |  |
| `receiver_player_ids` | List |  |
| `rusher_player_ids` | List |  |
| `tackler_player_ids` | List |  |
| `pass_defender_player_ids` | List |  |
| `scorer_player_ids` | List |  |
| `pat_scorer_player_ids` | List |  |
| `penalized_player_ids` | List |  |
| `punter_player_ids` | List |  |
| `fumbler_player_ids` | List |  |
| `forced_by_player_ids` | List |  |
| `recoverer_player_ids` | List |  |
| `sacked_by_player_ids` | List |  |
| `snapper_player_ids` | List |  |
| `holder_player_ids` | List |  |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_play_participants
participants = espn_nfl_play_participants(game_id=401872922)
print(participants.select("play_id", "passer_player_name", "passer_player_id").head())
```

### espn_nfl_player_stats {#espn_nfl_player_stats}

`espn_nfl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NFL athlete's ESPN **season** stat line as one wide row.

See `sportsdataverse.wbb.espn_wbb_player_stats` for full
documentation of the wide return shape, the `{category}_{stat}` stat
columns (for football: `passing_*`, `rushing_*`, `receiving_*`,
`scoring_*`, ...), the athlete / team metadata blocks, and the
`season_type` / `total` parameters. For the richer multi-category
web-v3 payload use `sportsdataverse.nfl.espn_nfl_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NFL athlete identifier (e.g. `3139477` for Patrick Mahomes). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | `"regular"` (type 2) or `"postseason"` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When `raw=True` returns the raw statistics JSON `dict`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `total` | Boolean | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `athlete_id` | Int64 |  |
| `athlete_uid` | String |  |
| `athlete_guid` | String |  |
| `athlete_type` | String |  |
| `first_name` | String | First name of player |
| `last_name` | String | Last name of player |
| `full_name` | String | Full name as per NFL.com |
| `display_name` | String | Full name of player |
| `short_name` | String | Player short name (i.e. "F.Last") |
| `weight` | Float64 | Official weight, in pounds |
| `display_weight` | String |  |
| `height` | Float64 | Official height, in inches |
| `display_height` | String |  |
| `age` | Int64 | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `date_of_birth` | String |  |
| `jersey` | String |  |
| `slug` | String |  |
| `active` | Boolean |  |
| `position_id` | Int64 |  |
| `position_name` | String |  |
| `position_display_name` | String |  |
| `position_abbreviation` | String |  |
| `college_name` | String | Official college (usually the last one attended) |
| `status_id` | Int64 |  |
| `status_name` | String |  |
| `general_fumbles` | Float64 | Total number of times the player fumbled the ball regardless of who recovered it. |
| `general_fumbles_lost` | Float64 | Number of fumbles by the player that were subsequently recovered by the opposing team. |
| `general_fumbles_forced` | Float64 | Number of fumbles the player forced from opposing ball carriers. |
| `general_fumbles_forced_primary` | Float64 | Number of fumbles forced where the player was credited as the primary forcer rather than an assist. |
| `general_fumbles_recovered` | Float64 | Number of fumbles (own or opponent's) recovered by the player. |
| `general_fumbles_recovered_yards` | Float64 | Total yards gained on fumble recoveries returned by the player. |
| `general_fumbles_touchdowns` | Float64 | Number of touchdowns scored by the player on fumble recoveries. |
| `general_games_played` | Float64 |  |
| `general_offensive_two_pt_returns` | Float64 | Number of two-point conversion attempts the player's offense returned defensively for a score. |
| `general_offensive_fumbles_touchdowns` | Float64 | Number of touchdowns scored by recovering an offensive team fumble (e.g., a fumble recovered in the end zone). |
| `general_defensive_fumbles_touchdowns` | Float64 | Number of touchdowns scored by recovering or returning an opponent's fumble on the defensive side. |
| `passing_avg_gain` | Float64 | Average yards gained per passing attempt including sacks. |
| `passing_completion_pct` | Float64 | Percentage of pass attempts that resulted in a completed reception (completions divided by attempts). |
| `passing_completions` | Float64 |  |
| `passing_espnqb_rating` | Float64 | ESPN's proprietary composite quarterback rating blending efficiency, usage, and situational performance into a single metric. |
| `passing_interception_pct` | Float64 | Percentage of pass attempts that resulted in an interception (interceptions divided by attempts). |
| `passing_interceptions` | Float64 | Total number of passes thrown that were intercepted by the opposing defense. |
| `passing_long_passing` | Float64 | Longest completed pass in yards recorded by the player during the period. |
| `passing_net_passing_yards` | Float64 | Passing yards minus yards lost on sacks, giving a net aerial production figure. |
| `passing_net_passing_yards_per_game` | Float64 | Net passing yards (after sack yardage deduction) per game played. |
| `passing_net_total_yards` | Float64 | Combined net rushing and net passing yards accumulated by the player. |
| `passing_net_yards_per_game` | Float64 | Net total offensive yards (rushing plus net passing) per game. |
| `passing_passing_attempts` | Float64 | Total number of pass attempts thrown by the player. |
| `passing_passing_big_plays` | Float64 | Number of passing plays that gained 20 or more yards. |
| `passing_passing_first_downs` | Float64 | Number of pass completions that resulted in a first down. |
| `passing_passing_fumbles` | Float64 | Number of times the player fumbled while in the act of passing or being sacked. |
| `passing_passing_fumbles_lost` | Float64 | Number of passing-related fumbles that were recovered by the opposing team. |
| `passing_passing_touchdown_pct` | Float64 | Percentage of pass attempts that resulted in a touchdown (touchdowns divided by attempts). |
| `passing_passing_touchdowns` | Float64 | Total number of touchdown passes thrown by the player. |
| `passing_passing_yards` | Float64 | Total aerial yards gained on completions thrown by the player. |
| `passing_passing_yards_after_catch` | Float64 | Total yards gained by receivers after catching the ball on passes thrown by the player. |
| `passing_passing_yards_at_catch` | Float64 | Total yards gained through the air (prior to the catch) on completions thrown by the player. |
| `passing_passing_yards_per_game` | Float64 | Gross passing yards per game played by the quarterback. |
| `passing_qb_rating` | Float64 | Traditional NFL passer rating computed from completion percentage, yards per attempt, touchdown percentage, and interception percentage on a roughly 0–158.3 scale. |
| `passing_sacks` | Float64 | Total number of times the player was sacked behind the line of scrimmage while attempting to pass. |
| `passing_sack_yards_lost` | Float64 | Total yards lost by the player as a result of being sacked behind the line of scrimmage. |
| `passing_net_passing_attempts` | Float64 | Total pass attempts minus sacks taken, representing meaningful dropbacks. |
| `passing_team_games_played` | Float64 | Number of games played by the player's team during which the player accumulated passing statistics. |
| `passing_total_offensive_plays` | Float64 | Total number of offensive plays (pass attempts, rushes, and sacks) run with the player as the primary ball handler. |
| `passing_total_points_per_game` | Float64 | Average total points scored by the player's team per game. |
| `passing_total_touchdowns` | Float64 | Total touchdowns (passing, rushing, and receiving) scored by or credited to the player. |
| `passing_total_yards` | Float64 | Combined total of passing, rushing, and receiving yards accumulated by the player. |
| `passing_total_yards_from_scrimmage` | Float64 | Total yards from scrimmage (rushing plus receiving) credited to the player in addition to passing yards. |
| `passing_two_point_pass_convs` | Float64 | Number of successful two-point conversion attempts thrown by the player. |
| `passing_two_pt_pass` | Float64 | Number of two-point conversion passes the player successfully completed. |
| `passing_two_pt_pass_attempts` | Float64 | Number of two-point conversion pass attempts thrown by the player regardless of outcome. |
| `passing_yards_from_scrimmage_per_game` | Float64 | Yards from scrimmage (rushing plus receiving) per game for a player who also has passing statistics recorded. |
| `passing_yards_per_completion` | Float64 | Average yards gained per completed pass attempt. |
| `passing_yards_per_game` | Float64 | Gross passing yards per game (equivalent to passing_passing_yards_per_game; alternate label). |
| `passing_yards_per_pass_attempt` | Float64 | Gross passing yards divided by total pass attempts, not penalizing for sacks. |
| `passing_net_yards_per_pass_attempt` | Float64 | Net passing yards divided by total pass attempts including sacks, penalizing passers for yardage lost in the pocket. |
| `passing_qbr` | Float64 |  |
| `passing_adj_qbr` | Float64 | ESPN's Adjusted Total Quarterback Rating, which adjusts raw QBR for opponent strength and clutch situations on a 0–100 scale. |
| `passing_quarterback_rating` | Float64 | Alternate or supplemental quarterback rating value; may represent a different calculation context from passing_qb_rating (e.g., situational or opponent-adjusted). |
| `rushing_avg_gain` | Float64 | Average yards gained per rushing attempt by the player. |
| `rushing_espnrb_rating` | Float64 | ESPN's proprietary composite running back rating blending efficiency and usage metrics. |
| `rushing_long_rushing` | Float64 | Longest single rushing play in yards recorded by the player during the period. |
| `rushing_net_total_yards` | Float64 | Combined net rushing and receiving yards accumulated by the player. |
| `rushing_net_yards_per_game` | Float64 | Net total offensive yards per game for the player. |
| `rushing_rushing_attempts` | Float64 | Total number of rushing attempts carried by the player. |
| `rushing_rushing_big_plays` | Float64 | Number of rushing plays that gained 10 or more yards. |
| `rushing_rushing_first_downs` | Float64 | Number of rushing attempts that resulted in a first down. |
| `rushing_rushing_fumbles` | Float64 | Number of times the player fumbled while carrying the ball on a rushing play. |
| `rushing_rushing_fumbles_lost` | Float64 | Number of rushing fumbles by the player that were recovered by the opposing team. |
| `rushing_rushing_touchdowns` | Float64 | Total number of rushing touchdowns scored by the player. |
| `rushing_rushing_yards` | Float64 | Total yards gained by the player on all rushing attempts. |
| `rushing_rushing_yards_per_game` | Float64 | Rushing yards per game played by the player. |
| `rushing_stuffs` | Float64 | Number of rushing attempts where the player was tackled at or behind the line of scrimmage. |
| `rushing_stuff_yards_lost` | Float64 | Total yards lost on rushing plays where the player was tackled behind the line of scrimmage (stuffed). |
| `rushing_team_games_played` | Float64 | Number of games played by the player's team during which the player accumulated rushing statistics. |
| `rushing_total_offensive_plays` | Float64 | Total number of offensive plays run with the player active in a rushing role. |
| `rushing_total_points_per_game` | Float64 | Average total points scored by the player's team per game. |
| `rushing_total_touchdowns` | Float64 | Total touchdowns (rushing and receiving) scored by the player. |
| `rushing_total_yards` | Float64 | Combined total rushing and receiving yards accumulated by the player. |
| `rushing_total_yards_from_scrimmage` | Float64 | Total yards from scrimmage (rushing plus receiving) credited to the player. |
| `rushing_two_point_rush_convs` | Float64 | Number of successful two-point conversion rushes scored by the player. |
| `rushing_two_pt_rush` | Float64 | Number of two-point conversion rushes the player successfully converted. |
| `rushing_two_pt_rush_attempts` | Float64 | Number of two-point conversion rush attempts by the player regardless of outcome. |
| `rushing_yards_from_scrimmage_per_game` | Float64 | Total yards from scrimmage per game for the player. |
| `rushing_yards_per_game` | Float64 | Rushing yards per game (equivalent label to rushing_rushing_yards_per_game). |
| `rushing_yards_per_rush_attempt` | Float64 | Average yards gained per rushing attempt by the player. |
| `receiving_avg_gain` | Float64 | Average yards gained per reception by the player. |
| `receiving_espnwr_rating` | Float64 | ESPN's proprietary composite wide receiver / pass-catcher rating blending efficiency and usage metrics. |
| `receiving_long_reception` | Float64 | Longest single reception in yards recorded by the player during the period. |
| `receiving_net_total_yards` | Float64 | Combined net rushing and receiving yards accumulated by the player. |
| `receiving_net_yards_per_game` | Float64 | Net total offensive yards (rushing plus receiving) per game for the player. |
| `receiving_receiving_big_plays` | Float64 | Number of receptions that gained 20 or more yards. |
| `receiving_receiving_first_downs` | Float64 | Number of receptions that resulted in a first down. |
| `receiving_receiving_fumbles` | Float64 | Number of times the player fumbled after making a reception. |
| `receiving_receiving_fumbles_lost` | Float64 | Number of post-reception fumbles by the player that were recovered by the opposing team. |
| `receiving_receiving_targets` | Float64 | Total number of times the player was the intended target of a pass attempt. |
| `receiving_receiving_touchdowns` | Float64 | Total number of touchdown receptions credited to the player. |
| `receiving_receiving_yards` | Float64 | Total yards gained by the player on all receptions. |
| `receiving_receiving_yards_after_catch` | Float64 | Total yards gained by the player after making contact with the ball (yards after catch). |
| `receiving_receiving_yards_at_catch` | Float64 | Total air yards at the point of the catch (depth of target) on receptions by the player. |
| `receiving_receiving_yards_per_game` | Float64 | Receiving yards per game played by the player. |
| `receiving_receptions` | Float64 | Total number of passes successfully caught by the player. |
| `receiving_team_games_played` | Float64 | Number of games played by the player's team during which the player accumulated receiving statistics. |
| `receiving_total_offensive_plays` | Float64 | Total number of offensive plays run during which the player was active on the field. |
| `receiving_total_points_per_game` | Float64 | Average total points scored by the player's team per game (context for the receiver's role). |
| `receiving_total_touchdowns` | Float64 | Total touchdowns (rushing and receiving) scored by the player. |
| `receiving_total_yards` | Float64 | Combined total rushing and receiving yards accumulated by the player. |
| `receiving_total_yards_from_scrimmage` | Float64 | Total yards from scrimmage (rushing plus receiving) credited to the player. |
| `receiving_two_point_rec_convs` | Float64 | Number of successful two-point conversion receptions caught by the player. |
| `receiving_two_pt_reception` | Float64 | Number of two-point conversion passes the player successfully caught. |
| `receiving_two_pt_reception_attempts` | Float64 | Number of two-point conversion targets thrown to the player regardless of outcome. |
| `receiving_yards_from_scrimmage_per_game` | Float64 | Total yards from scrimmage per game for the player. |
| `receiving_yards_per_game` | Float64 | Receiving yards per game (equivalent label to receiving_receiving_yards_per_game). |
| `receiving_yards_per_reception` | Float64 | Average yards gained per reception, also known as yards per catch. |
| `defensive_assist_tackles` | Float64 | Number of assisted tackles credited to the player (helped bring down the ball carrier but was not the primary tackler). |
| `defensive_avg_interception_yards` | Float64 | Average yards gained per interception returned by the player. |
| `defensive_avg_sack_yards` | Float64 | Average yards lost per sack the player recorded against the opposing quarterback. |
| `defensive_avg_stuff_yards` | Float64 | Average yards lost per run stuff (tackle behind the line of scrimmage) recorded by the player. |
| `defensive_blocked_field_goal_touchdowns` | Float64 | Number of touchdowns scored by the player after blocking an opponent's field goal attempt and returning it. |
| `defensive_blocked_punt_touchdowns` | Float64 | Number of touchdowns scored by the player after blocking an opponent's punt and returning it. |
| `defensive_hurries` | Float64 | Number of times the player pressured the quarterback into an early or errant throw without recording a sack. |
| `defensive_kicks_blocked` | Float64 | Total number of kicks (field goals or extra points) the player blocked. |
| `defensive_long_interception` | Float64 | Longest single interception return in yards recorded by the player. |
| `defensive_misc_touchdowns` | Float64 | Number of defensive touchdowns scored via miscellaneous means not captured by other specific categories. |
| `defensive_passes_batted_down` | Float64 | Number of passes the player knocked down at the line of scrimmage without recording an interception. |
| `defensive_passes_defended` | Float64 | Total number of passes the player broke up or deflected, including both pass deflections and interceptions. |
| `defensive_qb_hits` | Float64 | Number of times the player legally hit the quarterback during or just after a pass attempt. |
| `defensive_two_pt_returns` | Float64 | Number of two-point conversion attempts the player's defense returned for a defensive conversion score. |
| `defensive_sacks` | Float64 |  |
| `defensive_sack_yards` | Float64 | Total yards lost by the opposing offense as a result of the player's sacks. |
| `defensive_safeties` | Float64 | Number of safeties recorded by the player (tackling the ball carrier in their own end zone). |
| `defensive_solo_tackles` | Float64 | Number of unassisted tackles credited solely to the player. |
| `defensive_stuffs` | Float64 | Number of times the player tackled a ball carrier for a loss on a rushing play. |
| `defensive_stuff_yards` | Float64 | Total yards lost by the offense on run stuffs recorded by the player. |
| `defensive_tackles_for_loss` | Float64 | Total number of tackles resulting in a loss of yards for the opposing offense. |
| `defensive_tackles_yards_lost` | Float64 | Total yards lost by the opposing offense on the player's tackles for loss. |
| `defensive_team_games_played` | Float64 | Number of games played by the player's team in which the player appeared on the defensive side. |
| `defensive_total_tackles` | Float64 | Combined total of solo tackles and assisted tackles recorded by the player. |
| `defensive_yards_allowed` | Float64 | Total yards allowed by the player's defense during games the player appeared in. |
| `defensive_points_allowed` | Float64 | Total points allowed by the player's team during games the player appeared in. |
| `defensive_one_pt_safeties_made` | Float64 | Number of one-point safeties recorded by the player's defense (scored when the opposing team is downed in their own end zone during a try). |
| `defensive_missed_field_goal_return_td` | Float64 | Number of touchdowns scored by returning a missed field goal attempt. |
| `defensive_blocked_punt_ez_rec_td` | Float64 | Number of touchdowns scored by recovering a blocked punt in the end zone. |
| `defensive_interceptions_interceptions` | Float64 | Total number of passes intercepted by the player. |
| `defensive_interceptions_interception_touchdowns` | Float64 | Number of touchdowns scored by the player on interception returns. |
| `defensive_interceptions_interception_yards` | Float64 | Total yards gained by the player on interception returns. |
| `scoring_defensive_points` | Float64 | Total points scored by the player's defense via safeties, defensive touchdowns, and blocked kick returns. |
| `scoring_field_goals` | Float64 | Total number of field goals successfully made by the player during the period. |
| `scoring_kick_extra_points` | Float64 | Total number of extra point attempts (PAT kicks) by the player. |
| `scoring_kick_extra_points_made` | Float64 | Total number of extra points successfully kicked through the uprights by the player. |
| `scoring_misc_points` | Float64 | Points scored via miscellaneous methods not captured by other scoring categories. |
| `scoring_passing_touchdowns` | Float64 | Total passing touchdowns thrown by the player, contributing to their total scoring line. |
| `scoring_receiving_touchdowns` | Float64 | Total receiving touchdowns scored by the player. |
| `scoring_return_touchdowns` | Float64 | Total touchdowns scored by the player on kick or punt returns. |
| `scoring_rushing_touchdowns` | Float64 | Total rushing touchdowns scored by the player. |
| `scoring_total_points` | Float64 | Total points contributed by the player across all scoring methods (touchdowns, PATs, field goals, etc.). |
| `scoring_total_points_per_game` | Float64 | Average total points contributed by the player per game. |
| `scoring_total_touchdowns` | Float64 | Total touchdowns scored or thrown by the player across all methods. |
| `scoring_total_two_point_convs` | Float64 | Total number of successful two-point conversions scored or thrown by the player. |
| `scoring_two_point_pass_convs` | Float64 | Number of successful two-point conversion passes thrown by the player. |
| `scoring_two_point_rec_convs` | Float64 | Number of successful two-point conversion receptions caught by the player. |
| `scoring_two_point_rush_convs` | Float64 | Number of successful two-point conversion rushes scored by the player. |
| `scoring_one_pt_safeties_made` | Float64 | Number of one-point safeties recorded, scored when the defense stops an offense that is attempting a two-point conversion in their own end zone. |
| `team_id` | Int64 |  |
| `team_uid` | String |  |
| `team_guid` | String |  |
| `team_slug` | String |  |
| `team_location` | String |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_display_name` | String |  |
| `team_short_display_name` | String |  |
| `team_color` | String |  |
| `team_alternate_color` | String |  |
| `team_is_active` | Boolean |  |
| `team_logo_href` | String |  |
| `general_defensive_fumbles_forced` | Float64 | Fumbles the player forced on defense, excluding miscellaneous and special-teams plays (ESPN defensiveFumblesForced, as a float); 0.0 in the single sampled row. |
| `general_misc_fumbles_forced` | Float64 | Fumbles the player forced when not on defense or special teams (ESPN miscFumblesForced, as a float); 0.0 in the single sampled row. |
| `general_special_teams_fumbles_forced` | Float64 | Fumbles the player forced on special-teams plays (ESPN specialTeamsFumblesForced, as a float); 0.0 in the single sampled row. |
| `passing_offensive_snap_pct` | Float64 | ESPN's offensiveSnapPct stat (described upstream as '% of plays the player was on the field'); 0.0 for every athlete checked, including a 175-target receiver (Ja'Marr Chase, 2024), so ESPN does not appear to populate it. |
| `passing_target_share_pct` | Float64 | ESPN's targetSharePct stat (described upstream as '% of total team targets'); 0.0 for every athlete checked, including a 175-target receiver (Ja'Marr Chase, 2024), so ESPN does not appear to populate it. |
| `passing_yards_per_route_run` | Float64 | ESPN's yardsPerRouteRun stat (yards per route run, YPRR) under the passing category; 0.0 for every athlete checked, including a 175-target receiver (Ja'Marr Chase, 2024), so ESPN does not appear to populate it. |
| `passing_avg_depth_of_target` | Float64 | ESPN's avgDepthOfTarget stat (average depth of target, aDOT) under the passing category; 0.0 for every athlete checked, including a 175-target receiver (Ja'Marr Chase, 2024), so ESPN does not appear to populate it. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season_type` is not `"regular"`/`"postseason"`. |
| `sportsdataverse.errors.NoDataError` | ESPN returned 404. |
| `sportsdataverse.errors.AssetFetchError` | The statistics, athlete or team fetch failed (403, 429, 5xx, a dead connection); only a 404 on the athlete or team node leaves those columns null. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_player_stats
df = espn_nfl_player_stats(athlete_id=3139477, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### espn_nfl_schedule {#espn_nfl_schedule}

`espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_schedule - look up the NFL schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `week` | `int` | `None` | Week of the schedule. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `groups` |  | `None` |  |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. A day with no games returns a zero-row frame carrying the documented columns.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ID of the player in the 'name' column. |
| `uid` | String |  |
| `date` | String |  |
| `attendance` | Int64 |  |
| `time_valid` | Boolean |  |
| `neutral_site` | Boolean |  |
| `conference_competition` | Boolean |  |
| `play_by_play_available` | Boolean |  |
| `recent` | Boolean |  |
| `start_date` | String |  |
| `broadcast` | String |  |
| `highlights` | String |  |
| `notes_type` | String |  |
| `notes_headline` | String |  |
| `broadcast_market` | String |  |
| `broadcast_name` | String |  |
| `type_id` | Int64 |  |
| `type_abbreviation` | String |  |
| `venue_id` | Int64 |  |
| `venue_full_name` | String |  |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_address_country` | String | Two-letter ISO country code or country name for the country where the venue is located. |
| `venue_indoor` | Boolean |  |
| `status_clock` | Float64 |  |
| `status_display_clock` | String |  |
| `status_period` | Int64 |  |
| `status_type_id` | Int64 |  |
| `status_type_name` | String |  |
| `status_type_state` | String |  |
| `status_type_completed` | Boolean |  |
| `status_type_description` | String |  |
| `status_type_detail` | String |  |
| `status_type_short_detail` | String |  |
| `status_is_tbd_flex` | Boolean | Boolean flag indicating whether the game's broadcast slot is designated as a flex/TBD window. |
| `format_regulation_periods` | Int64 |  |
| `home_id` | Int64 |  |
| `home_uid` | String |  |
| `home_location` | String |  |
| `home_name` | String |  |
| `home_abbreviation` | String |  |
| `home_display_name` | String |  |
| `home_short_display_name` | String |  |
| `home_color` | String |  |
| `home_alternate_color` | String |  |
| `home_is_active` | Boolean |  |
| `home_venue_id` | Int64 |  |
| `home_logo` | String |  |
| `home_score` | String | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `home_current_rank` | Int64 | Always null. The NFL has no poll, so ESPN ships no curatedRank; kept so the column set matches the college schedules. |
| `home_linescores` | List | Comma-separated or serialized quarter-by-quarter score totals for the home team. |
| `home_records` | String | Serialized win-loss-tie record(s) for the home team (e.g., overall, home, away, conference). |
| `away_id` | Int64 |  |
| `away_uid` | String |  |
| `away_location` | String |  |
| `away_name` | String |  |
| `away_abbreviation` | String |  |
| `away_display_name` | String |  |
| `away_short_display_name` | String |  |
| `away_color` | String |  |
| `away_alternate_color` | String |  |
| `away_is_active` | Boolean |  |
| `away_venue_id` | Int64 |  |
| `away_logo` | String |  |
| `away_score` | String | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `away_current_rank` | Int64 | Always null. The NFL has no poll, so ESPN ships no curatedRank; kept so the column set matches the college schedules. |
| `away_linescores` | List | Comma-separated or serialized quarter-by-quarter score totals for the away team. |
| `away_records` | String | Serialized win-loss-tie record(s) for the away team (e.g., overall, home, away, conference). |
| `game_id` | Int64 | Ten digit identifier for NFL game. |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | Int64 | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | Int64 | Season week. |
| `format_overtime_periods` | Int64 |  |
| `home_logo_dark` | String |  |
| `away_logo_dark` | String |  |
| `home_winner` | Boolean | True when the home team won, from ESPN's winner flag; null before the game ends. |
| `away_winner` | Boolean | True when the away team won, from ESPN's winner flag; null before the game ends. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_schedule
sched = espn_nfl_schedule(dates=20240908)

# Specific week of regular season (``season_type=2``)

wk1 = espn_nfl_schedule(dates=2024, week=1, season_type=2)

# Pandas round-trip

sched_pd = espn_nfl_schedule(dates=20240908, return_as_pandas=True)
```

### espn_nfl_teams {#espn_nfl_teams}

`espn_nfl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_teams - look up NFL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of the NFL teams, one row per team, with the columns in the returns table. A payload without teams returns a zero-row frame with those columns. The result is not cached: every call fetches (through the package's opt-in TTL response cache, `sportsdataverse.cache`, when it is on).

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String |  |
| `team_alternate_color` | String |  |
| `team_color` | String |  |
| `team_display_name` | String |  |
| `team_id` | Int64 |  |
| `team_is_active` | Boolean |  |
| `team_is_all_star` | Boolean |  |
| `team_location` | String |  |
| `team_logos` | Int64 |  |
| `team_name` | String |  |
| `team_nickname` | String |  |
| `team_short_display_name` | String |  |
| `team_slug` | String |  |
| `team_uid` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | ESPN answered 404 for the teams resource. |
| `AssetFetchError` | The fetch failed (403, 429, 5xx, a dropped connection); never returned as an empty frame. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_teams
teams = espn_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = espn_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbreviation", "team_display_name"]].head()

# Pipeline next step (build a team_id to abbreviation map)

abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Normalize one ESPN scoreboard `event` into a flatter shape.

Splits the competitors list into `home` / `away` siblings, hoists
notes / broadcast metadata onto the competition root, and drops the
fields the schedule helper does not need (`odds`, `leaders`,
`geoBroadcasts`, etc.). Used internally by
`espn_nfl_schedule`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `Dict` |  | A single `events[i]` dict from the ESPN scoreboard endpoint. |

**Returns**

The mutated event dict with normalized `home` / `away` / broadcast keys.

**Example**

```python
from sportsdataverse.nfl import espn_nfl_scoreboard
from sportsdataverse.nfl.nfl_schedule import scoreboard_event_parsing
payload = espn_nfl_scoreboard(dates=20240115, return_parsed=False)
for ev in payload.get("events", []):
    scoreboard_event_parsing(ev)
    ev["competitions"][0]["home"]["abbreviation"]
```
