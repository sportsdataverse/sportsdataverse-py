---
title: "NFL — additional Python functions — Play-by-play, schedule & rosters"
sidebar_label: "Play-by-play, schedule & rosters"
sidebar_position: 11
description: "NFL — additional Python functions — Play-by-play, schedule & rosters — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Play-by-play, schedule & rosters

### espn_nfl_game_rosters {#espn_nfl_game_rosters}

`espn_nfl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nfl_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

| col_name | type | description |
|---|---|---|
| `athlete_id` | integer | ESPN athlete id. |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `full_name` | character | Full name as per NFL.com |
| `athlete_display_name` | character | Player display name. |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `weight` | double | Official weight, in pounds |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `height` | double | Official height, in inches |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `date_of_birth` | character | Player date of birth (if published). |
| `debut_year` | integer | Year of professional debut. |
| `slug` | character | URL slug for the team. |
| `jersey` | character | Jersey number. |
| `linked` | logical | TRUE if the record is linked to a related entity. |
| `active` | logical | `TRUE` if the player was active for the game. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `headshot_href` | character | Link to ESPN Headshot of Player |
| `headshot_alt` | character | Alternative-text label for the headshot. |
| `projections_href` | character | ESPN API hyperlink reference URL for the player's statistical projection resource. |
| `contracts_href` | character | ESPN API hyperlink reference URL for the full list of the player's historical contract records. |
| `experience_years` | integer | Years of experience. |
| `college_athlete_href` | character | ESPN API hyperlink reference URL for the athlete's college profile resource. |
| `contract_href` | character | ESPN API hyperlink reference URL for the player's full contract resource. |
| `contract_option_type` | integer | Contract option type. |
| `contract_salary` | integer | Contract salary. |
| `contract_bonus` | integer | Signing or roster bonus amount (in dollars) associated with the player's current contract. |
| `contract_years_remaining` | integer | Contract years remaining. |
| `contract_signed_through` | integer | Final year of the player's current contract, expressed as a four-digit season year. |
| `contract_season_href` | character | ESPN API hyperlink reference URL for the specific season-level contract detail resource. |
| `contract_team_href` | character | ESPN API hyperlink reference URL for the team associated with the player's contract. |
| `contract_active` | logical | Contract active. |
| `status_id` | character | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `contract_salary_remaining` | integer | Contract salary remaining. |
| `draft_display_text` | character | Draft display text. |
| `draft_round` | integer | Round that player was drafted in |
| `draft_year` | integer | Year that player was drafted |
| `draft_selection` | integer | Draft selection. |
| `draft_team_href` | character | ESPN API hyperlink reference URL for the team that originally drafted this player. |
| `draft_pick_href` | character | ESPN API hyperlink reference URL for the draft-pick record associated with this player. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |
| `starter` | logical | `TRUE` if the athlete started the game. |
| `jersey_right` | character | Secondary or alternate jersey number display string used by ESPN (e.g., for special-game uniforms). |
| `valid` | logical | `TRUE` if the roster entry is flagged valid by ESPN. |
| `did_not_play` | logical | `TRUE` if the athlete did not play. |
| `display_name` | character | Full name of player |
| `athlete_href` | character | ESPN API hyperlink reference URL for the athlete resource. |
| `position_href` | character | ESPN API hyperlink reference URL for the player's positional classification resource. |
| `statistics_href` | character | ESPN API hyperlink reference URL for the player's career statistics resource. |
| `team_id` | integer | ESPN team id. |
| `order` | integer | Team order within the competition (0 = first). |
| `home_away` | character | `home` or `away`. |
| `winner` | logical | `TRUE` if this team won the game. |
| `team_guid` | character | ESPN team GUID. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | character | Team slug for the stat row. |
| `team_location` | character | Team location / school name. |
| `team_name` | character | Team nickname. |
| `team_nickname` | character | Team nickname label. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Full team display name. |
| `team_short_display_name` | character | Short team display name. |
| `team_color` | character | Primary team color. |
| `team_alternate_color` | character | Alternate team color. |
| `is_active` | logical | Active contract |
| `is_all_star` | logical | Whether the team is an all-star team. |
| `team_alternate_ids_sdr` | character | SportsDataverse SDR alternate identifier for the team, used for cross-source joins. |
| `logo_href` | character | URL of the default team logo. |
| `logo_dark_href` | character | URL of the dark-variant team logo. |
| `game_id` | integer | Ten digit identifier for NFL game. |

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

Polars (or pandas) DataFrame, one row per play; the raw play dicts when `raw=True`.

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
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `total` | logical | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `athlete_id` | integer | ESPN athlete id. |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `full_name` | character | Full name as per NFL.com |
| `display_name` | character | Full name of player |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `weight` | double | Official weight, in pounds |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `height` | double | Official height, in inches |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `date_of_birth` | character | Player date of birth (if published). |
| `jersey` | character | Jersey number. |
| `slug` | character | URL slug for the team. |
| `active` | logical | `TRUE` if the player was active for the game. |
| `position_id` | integer | ESPN position id. |
| `position_name` | character | Position name (e.g. `Quarterback`). |
| `position_display_name` | character | Human-readable position name. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`). |
| `college_name` | character | Official college (usually the last one attended) |
| `status_id` | integer | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `general_fumbles` | double | Total number of times the player fumbled the ball regardless of who recovered it. |
| `general_fumbles_lost` | double | Number of fumbles by the player that were subsequently recovered by the opposing team. |
| `general_fumbles_forced` | double | Number of fumbles the player forced from opposing ball carriers. |
| `general_fumbles_forced_primary` | double | Number of fumbles forced where the player was credited as the primary forcer rather than an assist. |
| `general_fumbles_recovered` | double | Number of fumbles (own or opponent's) recovered by the player. |
| `general_fumbles_recovered_yards` | double | Total yards gained on fumble recoveries returned by the player. |
| `general_fumbles_touchdowns` | double | Number of touchdowns scored by the player on fumble recoveries. |
| `general_games_played` | double | Games Played. |
| `general_offensive_two_pt_returns` | double | Number of two-point conversion attempts the player's offense returned defensively for a score. |
| `general_offensive_fumbles_touchdowns` | double | Number of touchdowns scored by recovering an offensive team fumble (e.g., a fumble recovered in the end zone). |
| `general_defensive_fumbles_touchdowns` | double | Number of touchdowns scored by recovering or returning an opponent's fumble on the defensive side. |
| `passing_avg_gain` | double | Average yards gained per passing attempt including sacks. |
| `passing_completion_pct` | double | Percentage of pass attempts that resulted in a completed reception (completions divided by attempts). |
| `passing_completions` | double | Pass completions (split from CFBD's `C/ATT` field). |
| `passing_espnqb_rating` | double | ESPN's proprietary composite quarterback rating blending efficiency, usage, and situational performance into a single metric. |
| `passing_interception_pct` | double | Percentage of pass attempts that resulted in an interception (interceptions divided by attempts). |
| `passing_interceptions` | double | Total number of passes thrown that were intercepted by the opposing defense. |
| `passing_long_passing` | double | Longest completed pass in yards recorded by the player during the period. |
| `passing_net_passing_yards` | double | Passing yards minus yards lost on sacks, giving a net aerial production figure. |
| `passing_net_passing_yards_per_game` | double | Net passing yards (after sack yardage deduction) per game played. |
| `passing_net_total_yards` | double | Combined net rushing and net passing yards accumulated by the player. |
| `passing_net_yards_per_game` | double | Net total offensive yards (rushing plus net passing) per game. |
| `passing_passing_attempts` | double | Total number of pass attempts thrown by the player. |
| `passing_passing_big_plays` | double | Number of passing plays that gained 20 or more yards. |
| `passing_passing_first_downs` | double | Number of pass completions that resulted in a first down. |
| `passing_passing_fumbles` | double | Number of times the player fumbled while in the act of passing or being sacked. |
| `passing_passing_fumbles_lost` | double | Number of passing-related fumbles that were recovered by the opposing team. |
| `passing_passing_touchdown_pct` | double | Percentage of pass attempts that resulted in a touchdown (touchdowns divided by attempts). |
| `passing_passing_touchdowns` | double | Total number of touchdown passes thrown by the player. |
| `passing_passing_yards` | double | Total aerial yards gained on completions thrown by the player. |
| `passing_passing_yards_after_catch` | double | Total yards gained by receivers after catching the ball on passes thrown by the player. |
| `passing_passing_yards_at_catch` | double | Total yards gained through the air (prior to the catch) on completions thrown by the player. |
| `passing_passing_yards_per_game` | double | Gross passing yards per game played by the quarterback. |
| `passing_qb_rating` | double | Traditional NFL passer rating computed from completion percentage, yards per attempt, touchdown percentage, and interception percentage on a roughly 0–158.3 scale. |
| `passing_sacks` | double | Total number of times the player was sacked behind the line of scrimmage while attempting to pass. |
| `passing_sack_yards_lost` | double | Total yards lost by the player as a result of being sacked behind the line of scrimmage. |
| `passing_net_passing_attempts` | double | Total pass attempts minus sacks taken, representing meaningful dropbacks. |
| `passing_team_games_played` | double | Number of games played by the player's team during which the player accumulated passing statistics. |
| `passing_total_offensive_plays` | double | Total number of offensive plays (pass attempts, rushes, and sacks) run with the player as the primary ball handler. |
| `passing_total_points_per_game` | double | Average total points scored by the player's team per game. |
| `passing_total_touchdowns` | double | Total touchdowns (passing, rushing, and receiving) scored by or credited to the player. |
| `passing_total_yards` | double | Combined total of passing, rushing, and receiving yards accumulated by the player. |
| `passing_total_yards_from_scrimmage` | double | Total yards from scrimmage (rushing plus receiving) credited to the player in addition to passing yards. |
| `passing_two_point_pass_convs` | double | Number of successful two-point conversion attempts thrown by the player. |
| `passing_two_pt_pass` | double | Number of two-point conversion passes the player successfully completed. |
| `passing_two_pt_pass_attempts` | double | Number of two-point conversion pass attempts thrown by the player regardless of outcome. |
| `passing_yards_from_scrimmage_per_game` | double | Yards from scrimmage (rushing plus receiving) per game for a player who also has passing statistics recorded. |
| `passing_yards_per_completion` | double | Average yards gained per completed pass attempt. |
| `passing_yards_per_game` | double | Gross passing yards per game (equivalent to passing_passing_yards_per_game; alternate label). |
| `passing_yards_per_pass_attempt` | double | Gross passing yards divided by total pass attempts, not penalizing for sacks. |
| `passing_net_yards_per_pass_attempt` | double | Net passing yards divided by total pass attempts including sacks, penalizing passers for yardage lost in the pocket. |
| `passing_qbr` | double | ESPN Quarterback Rating (QBR) for the player in this game. |
| `passing_adj_qbr` | double | ESPN's Adjusted Total Quarterback Rating, which adjusts raw QBR for opponent strength and clutch situations on a 0–100 scale. |
| `passing_quarterback_rating` | double | Alternate or supplemental quarterback rating value; may represent a different calculation context from passing_qb_rating (e.g., situational or opponent-adjusted). |
| `rushing_avg_gain` | double | Average yards gained per rushing attempt by the player. |
| `rushing_espnrb_rating` | double | ESPN's proprietary composite running back rating blending efficiency and usage metrics. |
| `rushing_long_rushing` | double | Longest single rushing play in yards recorded by the player during the period. |
| `rushing_net_total_yards` | double | Combined net rushing and receiving yards accumulated by the player. |
| `rushing_net_yards_per_game` | double | Net total offensive yards per game for the player. |
| `rushing_rushing_attempts` | double | Total number of rushing attempts carried by the player. |
| `rushing_rushing_big_plays` | double | Number of rushing plays that gained 10 or more yards. |
| `rushing_rushing_first_downs` | double | Number of rushing attempts that resulted in a first down. |
| `rushing_rushing_fumbles` | double | Number of times the player fumbled while carrying the ball on a rushing play. |
| `rushing_rushing_fumbles_lost` | double | Number of rushing fumbles by the player that were recovered by the opposing team. |
| `rushing_rushing_touchdowns` | double | Total number of rushing touchdowns scored by the player. |
| `rushing_rushing_yards` | double | Total yards gained by the player on all rushing attempts. |
| `rushing_rushing_yards_per_game` | double | Rushing yards per game played by the player. |
| `rushing_stuffs` | double | Number of rushing attempts where the player was tackled at or behind the line of scrimmage. |
| `rushing_stuff_yards_lost` | double | Total yards lost on rushing plays where the player was tackled behind the line of scrimmage (stuffed). |
| `rushing_team_games_played` | double | Number of games played by the player's team during which the player accumulated rushing statistics. |
| `rushing_total_offensive_plays` | double | Total number of offensive plays run with the player active in a rushing role. |
| `rushing_total_points_per_game` | double | Average total points scored by the player's team per game. |
| `rushing_total_touchdowns` | double | Total touchdowns (rushing and receiving) scored by the player. |
| `rushing_total_yards` | double | Combined total rushing and receiving yards accumulated by the player. |
| `rushing_total_yards_from_scrimmage` | double | Total yards from scrimmage (rushing plus receiving) credited to the player. |
| `rushing_two_point_rush_convs` | double | Number of successful two-point conversion rushes scored by the player. |
| `rushing_two_pt_rush` | double | Number of two-point conversion rushes the player successfully converted. |
| `rushing_two_pt_rush_attempts` | double | Number of two-point conversion rush attempts by the player regardless of outcome. |
| `rushing_yards_from_scrimmage_per_game` | double | Total yards from scrimmage per game for the player. |
| `rushing_yards_per_game` | double | Rushing yards per game (equivalent label to rushing_rushing_yards_per_game). |
| `rushing_yards_per_rush_attempt` | double | Average yards gained per rushing attempt by the player. |
| `receiving_avg_gain` | double | Average yards gained per reception by the player. |
| `receiving_espnwr_rating` | double | ESPN's proprietary composite wide receiver / pass-catcher rating blending efficiency and usage metrics. |
| `receiving_long_reception` | double | Longest single reception in yards recorded by the player during the period. |
| `receiving_net_total_yards` | double | Combined net rushing and receiving yards accumulated by the player. |
| `receiving_net_yards_per_game` | double | Net total offensive yards (rushing plus receiving) per game for the player. |
| `receiving_receiving_big_plays` | double | Number of receptions that gained 20 or more yards. |
| `receiving_receiving_first_downs` | double | Number of receptions that resulted in a first down. |
| `receiving_receiving_fumbles` | double | Number of times the player fumbled after making a reception. |
| `receiving_receiving_fumbles_lost` | double | Number of post-reception fumbles by the player that were recovered by the opposing team. |
| `receiving_receiving_targets` | double | Total number of times the player was the intended target of a pass attempt. |
| `receiving_receiving_touchdowns` | double | Total number of touchdown receptions credited to the player. |
| `receiving_receiving_yards` | double | Total yards gained by the player on all receptions. |
| `receiving_receiving_yards_after_catch` | double | Total yards gained by the player after making contact with the ball (yards after catch). |
| `receiving_receiving_yards_at_catch` | double | Total air yards at the point of the catch (depth of target) on receptions by the player. |
| `receiving_receiving_yards_per_game` | double | Receiving yards per game played by the player. |
| `receiving_receptions` | double | Total number of passes successfully caught by the player. |
| `receiving_team_games_played` | double | Number of games played by the player's team during which the player accumulated receiving statistics. |
| `receiving_total_offensive_plays` | double | Total number of offensive plays run during which the player was active on the field. |
| `receiving_total_points_per_game` | double | Average total points scored by the player's team per game (context for the receiver's role). |
| `receiving_total_touchdowns` | double | Total touchdowns (rushing and receiving) scored by the player. |
| `receiving_total_yards` | double | Combined total rushing and receiving yards accumulated by the player. |
| `receiving_total_yards_from_scrimmage` | double | Total yards from scrimmage (rushing plus receiving) credited to the player. |
| `receiving_two_point_rec_convs` | double | Number of successful two-point conversion receptions caught by the player. |
| `receiving_two_pt_reception` | double | Number of two-point conversion passes the player successfully caught. |
| `receiving_two_pt_reception_attempts` | double | Number of two-point conversion targets thrown to the player regardless of outcome. |
| `receiving_yards_from_scrimmage_per_game` | double | Total yards from scrimmage per game for the player. |
| `receiving_yards_per_game` | double | Receiving yards per game (equivalent label to receiving_receiving_yards_per_game). |
| `receiving_yards_per_reception` | double | Average yards gained per reception, also known as yards per catch. |
| `defensive_assist_tackles` | double | Number of assisted tackles credited to the player (helped bring down the ball carrier but was not the primary tackler). |
| `defensive_avg_interception_yards` | double | Average yards gained per interception returned by the player. |
| `defensive_avg_sack_yards` | double | Average yards lost per sack the player recorded against the opposing quarterback. |
| `defensive_avg_stuff_yards` | double | Average yards lost per run stuff (tackle behind the line of scrimmage) recorded by the player. |
| `defensive_blocked_field_goal_touchdowns` | double | Number of touchdowns scored by the player after blocking an opponent's field goal attempt and returning it. |
| `defensive_blocked_punt_touchdowns` | double | Number of touchdowns scored by the player after blocking an opponent's punt and returning it. |
| `defensive_hurries` | double | Number of times the player pressured the quarterback into an early or errant throw without recording a sack. |
| `defensive_kicks_blocked` | double | Total number of kicks (field goals or extra points) the player blocked. |
| `defensive_long_interception` | double | Longest single interception return in yards recorded by the player. |
| `defensive_misc_touchdowns` | double | Number of defensive touchdowns scored via miscellaneous means not captured by other specific categories. |
| `defensive_passes_batted_down` | double | Number of passes the player knocked down at the line of scrimmage without recording an interception. |
| `defensive_passes_defended` | double | Total number of passes the player broke up or deflected, including both pass deflections and interceptions. |
| `defensive_qb_hits` | double | Number of times the player legally hit the quarterback during or just after a pass attempt. |
| `defensive_two_pt_returns` | double | Number of two-point conversion attempts the player's defense returned for a defensive conversion score. |
| `defensive_sacks` | double | Sacks credited to the player. |
| `defensive_sack_yards` | double | Total yards lost by the opposing offense as a result of the player's sacks. |
| `defensive_safeties` | double | Number of safeties recorded by the player (tackling the ball carrier in their own end zone). |
| `defensive_solo_tackles` | double | Number of unassisted tackles credited solely to the player. |
| `defensive_stuffs` | double | Number of times the player tackled a ball carrier for a loss on a rushing play. |
| `defensive_stuff_yards` | double | Total yards lost by the offense on run stuffs recorded by the player. |
| `defensive_tackles_for_loss` | double | Total number of tackles resulting in a loss of yards for the opposing offense. |
| `defensive_tackles_yards_lost` | double | Total yards lost by the opposing offense on the player's tackles for loss. |
| `defensive_team_games_played` | double | Number of games played by the player's team in which the player appeared on the defensive side. |
| `defensive_total_tackles` | double | Combined total of solo tackles and assisted tackles recorded by the player. |
| `defensive_yards_allowed` | double | Total yards allowed by the player's defense during games the player appeared in. |
| `defensive_points_allowed` | double | Total points allowed by the player's team during games the player appeared in. |
| `defensive_one_pt_safeties_made` | double | Number of one-point safeties recorded by the player's defense (scored when the opposing team is downed in their own end zone during a try). |
| `defensive_missed_field_goal_return_td` | double | Number of touchdowns scored by returning a missed field goal attempt. |
| `defensive_blocked_punt_ez_rec_td` | double | Number of touchdowns scored by recovering a blocked punt in the end zone. |
| `defensive_interceptions_interceptions` | double | Total number of passes intercepted by the player. |
| `defensive_interceptions_interception_touchdowns` | double | Number of touchdowns scored by the player on interception returns. |
| `defensive_interceptions_interception_yards` | double | Total yards gained by the player on interception returns. |
| `scoring_defensive_points` | double | Total points scored by the player's defense via safeties, defensive touchdowns, and blocked kick returns. |
| `scoring_field_goals` | double | Total number of field goals successfully made by the player during the period. |
| `scoring_kick_extra_points` | double | Total number of extra point attempts (PAT kicks) by the player. |
| `scoring_kick_extra_points_made` | double | Total number of extra points successfully kicked through the uprights by the player. |
| `scoring_misc_points` | double | Points scored via miscellaneous methods not captured by other scoring categories. |
| `scoring_passing_touchdowns` | double | Total passing touchdowns thrown by the player, contributing to their total scoring line. |
| `scoring_receiving_touchdowns` | double | Total receiving touchdowns scored by the player. |
| `scoring_return_touchdowns` | double | Total touchdowns scored by the player on kick or punt returns. |
| `scoring_rushing_touchdowns` | double | Total rushing touchdowns scored by the player. |
| `scoring_total_points` | double | Total points contributed by the player across all scoring methods (touchdowns, PATs, field goals, etc.). |
| `scoring_total_points_per_game` | double | Average total points contributed by the player per game. |
| `scoring_total_touchdowns` | double | Total touchdowns scored or thrown by the player across all methods. |
| `scoring_total_two_point_convs` | double | Total number of successful two-point conversions scored or thrown by the player. |
| `scoring_two_point_pass_convs` | double | Number of successful two-point conversion passes thrown by the player. |
| `scoring_two_point_rec_convs` | double | Number of successful two-point conversion receptions caught by the player. |
| `scoring_two_point_rush_convs` | double | Number of successful two-point conversion rushes scored by the player. |
| `scoring_one_pt_safeties_made` | double | Number of one-point safeties recorded, scored when the defense stops an offense that is attempting a two-point conversion in their own end zone. |
| `team_id` | integer | ESPN team id. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_guid` | character | ESPN team GUID. |
| `team_slug` | character | Team slug for the stat row. |
| `team_location` | character | Team location / school name. |
| `team_name` | character | Team nickname. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Full team display name. |
| `team_short_display_name` | character | Short team display name. |
| `team_color` | character | Primary team color. |
| `team_alternate_color` | character | Alternate team color. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_logo_href` | character | Default team logo URL. |
| `general_defensive_fumbles_forced` | double | Fumbles the player forced on defense, excluding miscellaneous and special-teams plays (ESPN defensiveFumblesForced, as a float); 0.0 in the single sampled row. |
| `general_misc_fumbles_forced` | double | Fumbles the player forced when not on defense or special teams (ESPN miscFumblesForced, as a float); 0.0 in the single sampled row. |
| `general_special_teams_fumbles_forced` | double | Fumbles the player forced on special-teams plays (ESPN specialTeamsFumblesForced, as a float); 0.0 in the single sampled row. |
| `passing_offensive_snap_pct` | double | ESPN's offensiveSnapPct stat (described upstream as '% of plays the player was on the field'); 0.0 for every athlete checked, including a 175-target receiver (Ja'Marr Chase, 2024), so ESPN does not appear to populate it. |
| `passing_target_share_pct` | double | ESPN's targetSharePct stat (described upstream as '% of total team targets'); 0.0 for every athlete checked, including a 175-target receiver (Ja'Marr Chase, 2024), so ESPN does not appear to populate it. |
| `passing_yards_per_route_run` | double | ESPN's yardsPerRouteRun stat (yards per route run, YPRR) under the passing category; 0.0 for every athlete checked, including a 175-target receiver (Ja'Marr Chase, 2024), so ESPN does not appear to populate it. |
| `passing_avg_depth_of_target` | double | ESPN's avgDepthOfTarget stat (average depth of target, aDOT) under the passing category; 0.0 for every athlete checked, including a 175-target receiver (Ja'Marr Chase, 2024), so ESPN does not appear to populate it. |

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

Polars dataframe containing schedule dates for the requested season. Returns None if no games

| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `uid` | character | ESPN global unique identifier. |
| `date` | character | Date of the poll release. |
| `attendance` | integer | Reported attendance at the game. |
| `time_valid` | logical | Whether the start time is confirmed. |
| `neutral_site` | logical | TRUE/FALSE flag for if the game took place at a neutral site. |
| `conference_competition` | logical | Conference competition. |
| `play_by_play_available` | logical | Whether play-by-play data is available. |
| `recent` | logical | Whether the game is recent. |
| `start_date` | character | Season start timestamp (ISO 8601, UTC). |
| `broadcast` | character | Broadcast network short name. |
| `highlights` | character | Game highlight urls. |
| `notes_type` | character | Notes type. |
| `notes_headline` | character | Notes headline. |
| `broadcast_market` | character | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | character | Broadcast name. |
| `type_id` | character | Play-type id. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `venue_id` | character | Referencing venue id. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_address_country` | character | Two-letter ISO country code or country name for the country where the venue is located. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Current period. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `status_is_tbd_flex` | logical | Boolean flag indicating whether the game's broadcast slot is designated as a flex/TBD window. |
| `format_regulation_periods` | integer | Format regulation periods. |
| `home_id` | character | Home team referencing id. |
| `home_uid` | character | Home team's uid. |
| `home_location` | character | Home team's location. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_short_display_name` | character | Home short display name. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_is_active` | logical | Home team's is active. |
| `home_venue_id` | character | Unique identifier for home venue. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `home_current_rank` | integer | Current AP or ESPN power ranking of the home team at the time of the game. |
| `home_linescores` | list | Comma-separated or serialized quarter-by-quarter score totals for the home team. |
| `home_records` | character | Serialized win-loss-tie record(s) for the home team (e.g., overall, home, away, conference). |
| `away_id` | character | Away team referencing id. |
| `away_uid` | character | Away team's uid. |
| `away_location` | character | Away team's location. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_short_display_name` | character | Away short display name. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_is_active` | logical | Away team's is active. |
| `away_venue_id` | character | Unique identifier for away venue. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `away_current_rank` | integer | Current AP or ESPN power ranking of the away team at the time of the game. |
| `away_linescores` | list | Comma-separated or serialized quarter-by-quarter score totals for the away team. |
| `away_records` | character | Serialized win-loss-tie record(s) for the away team (e.g., overall, home, away, conference). |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | integer | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_schedule
sched = espn_nfl_schedule(dates=20240908)

# Specific week of regular season (``season_type=2``)

wk1 = espn_nfl_schedule(dates=2024, week=1, season_type=2)

# Pandas round-trip

sched_pd = espn_nfl_schedule(dates=20240908, return_as_pandas=True)
```
