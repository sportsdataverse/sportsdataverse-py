---
title: NFL — additional Python functions
sidebar_label: Additional functions
description: "NFL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# NFL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nfl`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nfl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nfl_game_rosters}

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
| `athlete_display_name` | character | Player display name; `athlete_detail = TRUE` only. |
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
| `team_location` | character | Team location / school name; `team_detail = TRUE` only. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_nickname` | character | Team nickname label; `team_detail = TRUE` only. |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `team_display_name` | character | Full team display name; `team_detail = TRUE` only. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_color` | character | Primary team color; `team_detail = TRUE` only. |
| `team_alternate_color` | character | Alternate team color; `team_detail = TRUE` only. |
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

### `espn_nfl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_nfl_player_stats}

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
| `position_name` | character | Position name (e.g. `Quarterback`); `position_detail = TRUE` only. |
| `position_display_name` | character | Human-readable position name; `position_detail = TRUE` only. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
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
| `team_location` | character | Team location / school name; `team_detail = TRUE` only. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `team_display_name` | character | Full team display name; `team_detail = TRUE` only. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_color` | character | Primary team color; `team_detail = TRUE` only. |
| `team_alternate_color` | character | Alternate team color; `team_detail = TRUE` only. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_logo_href` | character | Default team logo URL; `team_detail = TRUE` only. |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_player_stats
df = espn_nfl_player_stats(athlete_id=3139477, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### `espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nfl_schedule}

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

## Dataset loaders

### `load_combine(return_as_pandas=False) -> 'pl.DataFrame'` {#load_combine}

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `draft_year` | double | Year that player was drafted |
| `draft_team` | character | Team that drafted player |
| `draft_round` | double | Round that player was drafted in |
| `draft_ovr` | double | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfb_id` | character | Sports Reference (CFB) ID for player |
| `player_name` | character | Full name of player |
| `pos` | character | Position as tracked by FP |
| `school` | character | College of player |
| `ht` | character | Height of player (feet and inches) |
| `wt` | double | Weight of player (lbs) |
| `forty` | double | Player's 40 yard dash time at combine (seconds) |
| `bench` | double | Reps benched by player at combine |
| `vertical` | double | Player's vertical jump at combine (inches) |
| `broad_jump` | double | Player's broad jump at combine (inches) |
| `cone` | double | Player's 3 cone drill time at combine (seconds) |
| `shuttle` | double | Player's shuttle run time at combine (seconds) |

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

# Filter by draft year and position

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### `load_contracts(return_as_pandas=False) -> 'pl.DataFrame'` {#load_contracts}

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `is_active` | logical | Active contract |
| `year_signed` | integer | Year the contract was signed |
| `years` | integer | Contract length |
| `value` | double | Total contract value |
| `apy` | double | Average money per contract year |
| `guaranteed` | double | Total guaranteed money |
| `apy_cap_pct` | double | Average money per contract year as percentage of the team's salary cap at signing |
| `inflated_value` | double | Total contract value inflated to account for the rise of the salary cap |
| `inflated_apy` | double | Average money per contract year inflated to account for the rise of the salary cap |
| `inflated_guaranteed` | double | Total guaranteed money inflated to account for the rise of the salary cap |
| `player_page` | character | Player's OverTheCap url |
| `otc_id` | integer | Over the Cap ID for player |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `date_of_birth` | character | Player date of birth (if published). |
| `height` | character | Official height, in inches |
| `weight` | character | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_overall` | integer | Overall draft selection number. |
| `draft_team` | character | Team that drafted player |
| `cols` | double | Placeholder column retained in the contracts loader output schema; contains no meaningful data in this context. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

# Pandas round-trip with sort by APY

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### `load_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_depth_charts}

Load NFL Depth Chart data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2001 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing depth chart data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `club_code` | character | Three-letter team abbreviation identifying the NFL club on the depth chart row. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `depth_team` | character | Numeric depth rank indicating whether the player is listed as the starter (1), backup (2), or further reserve on the depth chart. |
| `last_name` | character | Last name of player |
| `first_name` | character | First name of player |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `formation` | character | Offensive or defensive formation context in which the depth chart position applies (e.g., 'Shotgun', 'Nickel'). |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Primary position as reported by NFL.com |
| `elias_id` | character | Elias Sports Bureau identifier for the player, used by the NFL for official statistical tracking. |
| `depth_position` | character | Positional grouping label used to place the player on the team's official depth chart (e.g., 'QB', 'WR1', 'ILB'). |
| `full_name` | character | Full name as per NFL.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])

# Multi-season range

depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

### `load_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'` {#load_draft_picks}

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `round` | integer | Draft round |
| `pick` | integer | Draft overall pick |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `cfb_player_id` | character | ID from College Football Reference |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `hof` | logical | Whether player has been selected to the Pro Football Hall of Fame |
| `position` | character | Primary position as reported by NFL.com |
| `category` | character | Broader category of player positions |
| `side` | character | O for offense, D for defense, S for special teams |
| `college` | character | Official college (usually the last one attended) |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `to` | integer | Final season played in NFL |
| `allpro` | integer | Number of AP First Team All-Pro selections as recorded by PFR |
| `probowls` | integer | Number of Pro Bowls |
| `seasons_started` | integer | Number of seasons recorded as primary starter for position |
| `w_av` | integer | Weighted Approximate Value |
| `car_av` | logical | Career Approximate Value |
| `dr_av` | integer | Draft Approximate Value |
| `games` | integer | Games played in career |
| `pass_completions` | integer | Number of successful completions for a given game |
| `pass_attempts` | integer | Career pass attempts |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_tds` | integer | Career pass touchdowns thrown |
| `pass_ints` | integer | Career pass interceptions thrown |
| `rush_atts` | integer | Career rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `rush_tds` | integer | Career rushing touchdowns |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `rec_yards` | integer | Career receiving yards |
| `rec_tds` | integer | Career receiving touchdowns |
| `def_solo_tackles` | integer | Career solo tackles |
| `def_ints` | integer | Career interceptions |
| `def_sacks` | double | Number of sacks form this player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

# Filter to a single year and round

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### `load_espn_qbr(seasons: 'List[int]', summary_type: 'str' = 'season', return_as_pandas: 'bool' = False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'` {#load_espn_qbr}

Load ESPN Total QBR (Quarterback Rating) data going back to 2006.

Mirrors nflreadpy / nflreadr `load_espn_qbr` -- the lone nflreadpy dataset
that previously had no sdv-py loader. ESPN publishes Total QBR only from 2006
onward, so 2006 is the earliest available season (unlike the 1999 floor on
play-by-play). nflverse republishes ESPN's QBR through the `espn_data`
release as two combined files (one per `summary_type`), each covering all
seasons; this loader reads the requested file once and post-filters by
`season` (the same access pattern as `load_nfl_schedule`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Seasons to return. 2006 is the earliest available season. |
| `summary_type` | `str` | `'season'` | Aggregation level. `"season"` (default) returns one row per quarterback-season; `"week"` returns one row per quarterback-game. Any other value raises `ValueError`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which QBR release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse `espn_data` release. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_espn_qbr` release (built by `nfl-data` from ESPN's QBR web endpoint -- the same source nflverse's espnscrapeR uses). Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing ESPN Total QBR for the requested seasons, summarized per `summary_type`.

**Example**

```python
from sportsdataverse.nfl import load_nfl_espn_qbr
qbr = load_nfl_espn_qbr(seasons=[2024])
qbr.shape

# Week-level QBR

qbr_week = load_nfl_espn_qbr(seasons=[2024], summary_type="week")

# Multi-season range

qbr = load_nfl_espn_qbr(seasons=range(2020, 2025))

# Pandas round-trip

qbr_pd = load_nfl_espn_qbr(seasons=[2024], return_as_pandas=True)
qbr_pd[["season", "team_abb", "qbr_total"]].head()
```

### `load_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'` {#load_ff_opportunity}

Load NFL fantasy football opportunity data from ffverse/ffopportunity

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2006 is the earliest available season. |
| `stat_type` | `str` | `'weekly'` | One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly". |
| `model_version` | `str` | `'latest'` | One of "latest", "v1.0.0". Defaults to "latest". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football opportunity data for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `posteam` | character | String abbreviation for the team with possession. |
| `week` | double | Season week. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `full_name` | character | Full name as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `rec_attempt` | double | Total number of targets for a given game |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_air_yards` | double | Total air yards thrown for a given game |
| `rec_air_yards` | double | Total air yards on receiving attempts for a given game |
| `pass_completions` | double | Number of successful completions for a given game |
| `receptions` | double | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `pass_completions_exp` | double | Expected number of pass_completions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `receptions_exp` | double | Expected number of receptions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_yards_gained` | double | Total passing yards gained for a given game |
| `rec_yards_gained` | double | Total receiving yards gained for a given game |
| `rush_yards_gained` | double | Total rushing yards gained for a given game |
| `pass_yards_gained_exp` | double | Expected number of pass_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_yards_gained_exp` | double | Expected number of rec_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_yards_gained_exp` | double | Expected number of rush_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rec_touchdown` | double | Total receiving touchdowns |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `pass_touchdown_exp` | double | Expected number of pass_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_touchdown_exp` | double | Expected number of rec_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_touchdown_exp` | double | Expected number of rush_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_two_point_conv` | double | Number of successful passing two point conversions |
| `rec_two_point_conv` | double | Number of successful receiving two point conversions |
| `rush_two_point_conv` | double | Number of successful rushing two point conversions |
| `pass_two_point_conv_exp` | double | Expected number of pass_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_two_point_conv_exp` | double | Expected number of rec_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_two_point_conv_exp` | double | Expected number of rush_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_first_down` | double | Number of passing first downs |
| `rec_first_down` | double | Number of receiving first downs |
| `rush_first_down` | double | Number of rushing first downs |
| `pass_first_down_exp` | double | Expected number of pass_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_first_down_exp` | double | Expected number of rec_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_first_down_exp` | double | Expected number of rush_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_interception` | double | Number of interceptions thrown |
| `rec_interception` | double | Number of interceptions on targets |
| `pass_interception_exp` | double | Expected number of pass_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_interception_exp` | double | Expected number of rec_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fumble_lost` | double | Number of fumbles on receiving attempts |
| `rush_fumble_lost` | double | Number of fumbles on rushing attempts |
| `pass_fantasy_points_exp` | double | Expected number of pass_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fantasy_points_exp` | double | Expected number of rec_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_fantasy_points_exp` | double | Expected number of rush_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_fantasy_points` | double | Total fantasy points from passing, assuming 0.04 points per pass yard, 4 points per pass TD, -2 points per interception |
| `rec_fantasy_points` | double | Total fantasy points from receiving, assuming PPR scoring |
| `rush_fantasy_points` | double | Total fantasy points from rushing, assuming PPR scoring |
| `total_yards_gained` | double | Total scrimmage yards (sum of pass, rush, and receiving yards) |
| `total_yards_gained_exp` | double | Expected number of total_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_touchdown` | double | Total touchdowns (sum of pass, rush, and receiving touchdowns) |
| `total_touchdown_exp` | double | Expected number of total_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_first_down` | double | Total first downs (sum of pass, rush, and receiving first downs) |
| `total_first_down_exp` | double | Expected number of total_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_fantasy_points` | double | Total fantasy points (sum of pass, rush, and receiving fantasy points) |
| `total_fantasy_points_exp` | double | Expected number of total_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_completions_diff` | double | Difference between actual and expected number of pass_completions - often interpreted as efficiency for a given play/game |
| `receptions_diff` | double | Difference between actual and expected number of receptions - often interpreted as efficiency for a given play/game |
| `pass_yards_gained_diff` | double | Difference between actual and expected number of pass_yards_gained - often interpreted as efficiency for a given play/game |
| `rec_yards_gained_diff` | double | Difference between actual and expected number of rec_yards_gained - often interpreted as efficiency for a given play/game |
| `rush_yards_gained_diff` | double | Difference between actual and expected number of rush_yards_gained - often interpreted as efficiency for a given play/game |
| `pass_touchdown_diff` | double | Difference between actual and expected number of pass_touchdown - often interpreted as efficiency for a given play/game |
| `rec_touchdown_diff` | double | Difference between actual and expected number of rec_touchdown - often interpreted as efficiency for a given play/game |
| `rush_touchdown_diff` | double | Difference between actual and expected number of rush_touchdown - often interpreted as efficiency for a given play/game |
| `pass_two_point_conv_diff` | double | Difference between actual and expected number of pass_two_point_conv - often interpreted as efficiency for a given play/game |
| `rec_two_point_conv_diff` | double | Difference between actual and expected number of rec_two_point_conv - often interpreted as efficiency for a given play/game |
| `rush_two_point_conv_diff` | double | Difference between actual and expected number of rush_two_point_conv - often interpreted as efficiency for a given play/game |
| `pass_first_down_diff` | double | Difference between actual and expected number of pass_first_down - often interpreted as efficiency for a given play/game |
| `rec_first_down_diff` | double | Difference between actual and expected number of rec_first_down - often interpreted as efficiency for a given play/game |
| `rush_first_down_diff` | double | Difference between actual and expected number of rush_first_down - often interpreted as efficiency for a given play/game |
| `pass_interception_diff` | double | Difference between actual and expected number of pass_interception - often interpreted as efficiency for a given play/game |
| `rec_interception_diff` | double | Difference between actual and expected number of rec_interception - often interpreted as efficiency for a given play/game |
| `pass_fantasy_points_diff` | double | Difference between actual and expected number of pass_fantasy_points - often interpreted as efficiency for a given play/game |
| `rec_fantasy_points_diff` | double | Difference between actual and expected number of rec_fantasy_points - often interpreted as efficiency for a given play/game |
| `rush_fantasy_points_diff` | double | Difference between actual and expected number of rush_fantasy_points - often interpreted as efficiency for a given play/game |
| `total_yards_gained_diff` | double | Difference between actual and expected number of total_yards_gained - often interpreted as efficiency for a given play/game |
| `total_touchdown_diff` | double | Difference between actual and expected number of total_touchdown - often interpreted as efficiency for a given play/game |
| `total_first_down_diff` | double | Difference between actual and expected number of total_first_down - often interpreted as efficiency for a given play/game |
| `total_fantasy_points_diff` | double | Difference between actual and expected number of total_fantasy_points - often interpreted as efficiency for a given play/game |
| `pass_attempt_team` | double | Team-level total pass_attempt for a game, summed across all plays/players for that team. |
| `rec_attempt_team` | double | Team-level total rec_attempt for a game, summed across all plays/players for that team. |
| `rush_attempt_team` | double | Team-level total rush_attempt for a game, summed across all plays/players for that team. |
| `pass_air_yards_team` | double | Team-level total pass_air_yards for a game, summed across all plays/players for that team. |
| `rec_air_yards_team` | double | Team-level total rec_air_yards for a game, summed across all plays/players for that team. |
| `pass_completions_team` | double | Team-level total pass_completions for a game, summed across all plays/players for that team. |
| `receptions_team` | double | Team-level total receptions for a game, summed across all plays/players for that team. |
| `pass_completions_exp_team` | double | Team-level total expected pass_completions_exp for a game, summed across all plays & players for that team. |
| `receptions_exp_team` | double | Team-level total expected receptions_exp for a game, summed across all plays & players for that team. |
| `pass_yards_gained_team` | double | Team-level total pass_yards_gained for a game, summed across all plays/players for that team. |
| `rec_yards_gained_team` | double | Team-level total rec_yards_gained for a game, summed across all plays/players for that team. |
| `rush_yards_gained_team` | double | Team-level total rush_yards_gained for a game, summed across all plays/players for that team. |
| `pass_yards_gained_exp_team` | double | Team-level total expected pass_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rec_yards_gained_exp_team` | double | Team-level total expected rec_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rush_yards_gained_exp_team` | double | Team-level total expected rush_yards_gained_exp for a game, summed across all plays & players for that team. |
| `pass_touchdown_team` | double | Team-level total pass_touchdown for a game, summed across all plays/players for that team. |
| `rec_touchdown_team` | double | Team-level total rec_touchdown for a game, summed across all plays/players for that team. |
| `rush_touchdown_team` | double | Team-level total rush_touchdown for a game, summed across all plays/players for that team. |
| `pass_touchdown_exp_team` | double | Team-level total expected pass_touchdown_exp for a game, summed across all plays & players for that team. |
| `rec_touchdown_exp_team` | double | Team-level total expected rec_touchdown_exp for a game, summed across all plays & players for that team. |
| `rush_touchdown_exp_team` | double | Team-level total expected rush_touchdown_exp for a game, summed across all plays & players for that team. |
| `pass_two_point_conv_team` | double | Team-level total pass_two_point_conv for a game, summed across all plays/players for that team. |
| `rec_two_point_conv_team` | double | Team-level total rec_two_point_conv for a game, summed across all plays/players for that team. |
| `rush_two_point_conv_team` | double | Team-level total rush_two_point_conv for a game, summed across all plays/players for that team. |
| `pass_two_point_conv_exp_team` | double | Team-level total expected pass_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rec_two_point_conv_exp_team` | double | Team-level total expected rec_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rush_two_point_conv_exp_team` | double | Team-level total expected rush_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `pass_first_down_team` | double | Team-level total pass_first_down for a game, summed across all plays/players for that team. |
| `rec_first_down_team` | double | Team-level total rec_first_down for a game, summed across all plays/players for that team. |
| `rush_first_down_team` | double | Team-level total rush_first_down for a game, summed across all plays/players for that team. |
| `pass_first_down_exp_team` | double | Team-level total expected pass_first_down_exp for a game, summed across all plays & players for that team. |
| `rec_first_down_exp_team` | double | Team-level total expected rec_first_down_exp for a game, summed across all plays & players for that team. |
| `rush_first_down_exp_team` | double | Team-level total expected rush_first_down_exp for a game, summed across all plays & players for that team. |
| `pass_interception_team` | double | Team-level total pass_interception for a game, summed across all plays/players for that team. |
| `rec_interception_team` | double | Team-level total rec_interception for a game, summed across all plays/players for that team. |
| `pass_interception_exp_team` | double | Team-level total expected pass_interception_exp for a game, summed across all plays & players for that team. |
| `rec_interception_exp_team` | double | Team-level total expected rec_interception_exp for a game, summed across all plays & players for that team. |
| `rec_fumble_lost_team` | double | Team-level total rec_fumble_lost for a game, summed across all plays/players for that team. |
| `rush_fumble_lost_team` | double | Team-level total rush_fumble_lost for a game, summed across all plays/players for that team. |
| `pass_fantasy_points_exp_team` | double | Team-level total expected pass_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rec_fantasy_points_exp_team` | double | Team-level total expected rec_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rush_fantasy_points_exp_team` | double | Team-level total expected rush_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `pass_fantasy_points_team` | double | Team-level total pass_fantasy_points for a game, summed across all plays/players for that team. |
| `rec_fantasy_points_team` | double | Team-level total rec_fantasy_points for a game, summed across all plays/players for that team. |
| `rush_fantasy_points_team` | double | Team-level total rush_fantasy_points for a game, summed across all plays/players for that team. |
| `pass_completions_diff_team` | double | Team-level difference between actual and expected number of pass_completions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `receptions_diff_team` | double | Team-level difference between actual and expected number of receptions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_yards_gained_diff_team` | double | Team-level difference between actual and expected number of pass_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rec_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rush_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_touchdown_diff_team` | double | Team-level difference between actual and expected number of pass_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_touchdown_diff_team` | double | Team-level difference between actual and expected number of rec_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_touchdown_diff_team` | double | Team-level difference between actual and expected number of rush_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of pass_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rec_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rush_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_first_down_diff_team` | double | Team-level difference between actual and expected number of pass_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_first_down_diff_team` | double | Team-level difference between actual and expected number of rec_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_first_down_diff_team` | double | Team-level difference between actual and expected number of rush_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_interception_diff_team` | double | Team-level difference between actual and expected number of pass_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_interception_diff_team` | double | Team-level difference between actual and expected number of rec_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of pass_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rec_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rush_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_yards_gained_team` | double | Team-level total total_yards_gained for a game, summed across all plays/players for that team. |
| `total_yards_gained_exp_team` | double | Team-level total expected total_yards_gained_exp for a game, summed across all plays & players for that team. |
| `total_yards_gained_diff_team` | double | Team-level difference between actual and expected number of total_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_touchdown_team` | double | Team-level total total_touchdown for a game, summed across all plays/players for that team. |
| `total_touchdown_exp_team` | double | Team-level total expected total_touchdown_exp for a game, summed across all plays & players for that team. |
| `total_touchdown_diff_team` | double | Team-level difference between actual and expected number of total_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_first_down_team` | double | Team-level total total_first_down for a game, summed across all plays/players for that team. |
| `total_first_down_exp_team` | double | Team-level total expected total_first_down_exp for a game, summed across all plays & players for that team. |
| `total_first_down_diff_team` | double | Team-level difference between actual and expected number of total_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_fantasy_points_team` | double | Team-level total total_fantasy_points for a game, summed across all plays/players for that team. |
| `total_fantasy_points_exp_team` | double | Team-level total expected total_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `total_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of total_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

# Pass play-by-play opportunity stats

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

# Rush play-by-play opportunity stats with pinned model version

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### `load_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'` {#load_ff_playerids}

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

| col_name | type | description |
|---|---|---|
| `mfl_id` | integer | MyFantasyLeague.com ID - this is the primary key for this table and is unique and complete. Usually an integer of 5 digits. |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `fantasypros_id` | character | FantasyPros.com ID - usually an integer of 5 digits. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `sleeper_id` | integer | Sleeper ID - usually an integer with ~4 digits. |
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `espn_id` | integer | ESPN ID - usual format is an integer with ~5 digits |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `fleaflicker_id` | character | Fleaflicker ID - usual format is an integer with ~4 digits. Fleaflicker API also has sportradar and that's generally preferred. |
| `cbs_id` | integer | CBS ID - usual format is an integer with ~ 7 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfbref_id` | character | College Football Reference ID - usual format is firstname-lastname-integer |
| `rotowire_id` | integer | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `rotoworld_id` | character | Rotoworld ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `ktc_id` | integer | KeepTradeCut ID - usual format is an integer with ~four digits. |
| `stats_id` | integer | Stats ID - usual format is five digit integer |
| `stats_global_id` | integer | Stats Global ID - usual format is a six digit integer |
| `fantasy_data_id` | integer | FantasyData ID - usual format five digit integer |
| `swish_id` | character | Player ID for Swish Analytics |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `merge_name` | character | Name but formatted for name joins via ffscrapr::dp_cleannames() - coerced to lowercase, stripped of punctuation and suffixes, and common substitutions performed. |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `birthdate` | character | Birthdate |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_pick` | integer | Draft pick within round, i.e. 32nd pick of second round. |
| `draft_ovr` | integer | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `twitter_username` | character | Official twitter handle, if known |
| `height` | integer | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `db_season` | integer | Year of database build. Previous years may also be available via dynastyprocess. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

# Filter to active QBs

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### `load_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'` {#load_ff_rankings}

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of `"draft"` (current draft rankings), `"week"` (weekly rankings), or `"all"` (full historical rankings). Defaults to `"draft"`. Kept for nflreadpy parity since its parameter is also called `type`; the forward-going preferred name is `kind`. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as `type`. If both are supplied, `kind` wins. If neither is supplied, defaults to `"draft"` via `type`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

| col_name | type | description |
|---|---|---|
| `fp_page` | character | The relative url that the data was scraped from (add the prefix https://www.fantasypros.com/ to visit the page) |
| `page_type` | character | Two word identifier separated by a dash identifying the type of fantasy ranking (best = bestball; dynasty; redraft) and what position it applies to |
| `ecr_type` | character | A two letter identifier combining the ranking type (b = bestball; d = dynasty; r = redraft) and position type (o = overall; p = positional; sf = superflex; rk = rookie) |
| `player` | character | Player name |
| `id` | integer | ID of the player in the 'name' column. |
| `pos` | character | Position as tracked by FP |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `ecr` | double | Average (mean) expert ranking for this player |
| `sd` | double | Standard deviation of expert rankings for this player |
| `best` | integer | The highest ranking given for this player by any one expert |
| `worst` | integer | The lowest ranking given for this player by any one expert |
| `sportsdata_id` | character | ID - also known as sportradar_id (they are equivalent!) |
| `player_filename` | character | base URL for this player on fantasypros.com |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `cbs_id` | character | CBS ID - usual format is an integer with ~ 7 digits. |
| `player_owned_avg` | double | The average percentage this player is rostered across ESPN and Yahoo |
| `player_owned_espn` | character | The percentage that this player is rostered in ESPN leagues |
| `player_owned_yahoo` | character | The percentage that this player is rostered in Yahoo leagues |
| `player_image_url` | character | An image of the player |
| `player_square_image_url` | character | An square image of the player |
| `rank_delta` | integer | Change in ranks over a recent period |
| `bye` | integer | NFL bye week |
| `mergename` | character | Player name after being cleaned by dp_cleannames - generally strips punctuation and suffixes as well as performing common name substitutions. |
| `scrape_date` | character | Date this dataframe was last updated |
| `tm` | character | Team ID as used on MyFantasyLeague.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

# Weekly rankings

weekly = load_nfl_ff_rankings(kind="week")

# Full historical rankings (parquet)

history = load_nfl_ff_rankings(kind="all")

# nflreadpy-parity ``type=`` parameter (still supported)

draft = load_nfl_ff_rankings(type="draft")
```

### `load_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_ftn_charting}

Load NFL FTN charting data going back to 2022

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2022 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing FTN charting data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `ftn_game_id` | integer | FTN game ID |
| `nflverse_game_id` | character | nflverse identifier for games. Format is season, week, away_team, home_team |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `ftn_play_id` | integer | FTN play ID |
| `nflverse_play_id` | integer | Play ID used by nflverse, corresponds to GSIS play ID |
| `starting_hash` | character | hash the ball was place(L = left, M = middle, R = right) |
| `qb_location` | character | pre-snap position of quarterback(U = under center, S = shotgun, P = pistol) |
| `n_offense_backfield` | integer | number of players in the backfield at the snap |
| `n_defense_box` | integer | Number of defenders positioned in the box at the snap, as charted by FTN Data. |
| `is_no_huddle` | logical | no huddle |
| `is_motion` | logical | motion occurred on the play before or at the time of the snap |
| `is_play_action` | logical | play-action pass |
| `is_screen_pass` | logical | screen pass |
| `is_rpo` | logical | play is considered run-pass option |
| `is_trick_play` | logical | trick play |
| `is_qb_out_of_pocket` | logical | quarterback moved out of pocket |
| `is_interception_worthy` | logical | interception worthy pass |
| `is_throw_away` | logical | quarterback thrown away |
| `read_thrown` | character | read the ball was thrown |
| `is_catchable_ball` | logical | catchable ball(defined by throws that are generally on target that are not defended away) |
| `is_contested_ball` | logical | contested ball(defined by whether or not the receiver is facing physical contact at the time of the catch) |
| `is_created_reception` | logical | created reception(defined by a reception that only occurs due to an exceptional play by the receiver) |
| `is_drop` | logical | receiver drop |
| `is_qb_sneak` | logical | quarterback sneak |
| `n_blitzers` | integer | number of blitzers |
| `n_pass_rushers` | integer | number of pass rushers |
| `is_qb_fault_sack` | logical | sack that is the fault of the quarterback |
| `date_pulled` | character | Date the data was retrieved from the FTN Data API by nflverse jobs |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])

# Multi-season range

charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

# Filter to plays with motion

import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

### `load_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_injuries}

Load NFL injuries data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2009 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing injuries data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `week` | integer | Season week. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `position` | character | Primary position as reported by NFL.com |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `report_primary_injury` | character | Primary injury listed on official injury report |
| `report_secondary_injury` | character | Secondary injury listed on official injury report |
| `report_status` | character | Player's status for game on official injury report |
| `practice_primary_injury` | character | Primary injury listed on practice injury report |
| `practice_secondary_injury` | character | Secondary injury listed on practice injury report |
| `practice_status` | character | Player's participation in practice |
| `date_modified` | character | Date and time that injury information was updated |

**Example**

```python
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])

# Multi-season range with team filter

import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

### `load_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nextgen_stats}

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats
accessors. Mirrors the API surface of nflreadpy's
`load_nextgen_stats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — `seasons` is applied as a post-filter on the `season` column. |
| `stat_type` | `str` | `'passing'` | One of `"passing"`, `"rushing"`, `"receiving"`. Defaults to `"passing"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested `stat_type` and `seasons`.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_time_to_throw` | double | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `avg_completed_air_yards` | double | Average air yards on completed passes |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `avg_air_yards_differential` | double | Air Yards Differential is calculated by subtracting the passer's average Intended Air Yards from his average Completed Air Yards. This stat indicates if he is on average attempting deep passes than he on average completes. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `max_completed_air_distance` | double | Air Distance is the amount of yards the ball has traveled on a pass, from the point of release to the point of reception (as the crow flies). Unlike Air Yards, Air Distance measures the actual distance the passer throws the ball. |
| `avg_air_yards_to_sticks` | double | Air Yards to the Sticks shows the amount of Air Yards ahead or behind the first down marker on all attempts for a passer. The metric indicates if the passer is attempting his passes past the 1st down marker, or if he is relying on his skill position players to make yards after catch. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_touchdowns` | integer | Number of touchdowns scored on pass plays |
| `interceptions` | integer | The number of interceptions thrown. |
| `passer_rating` | double | Overall NFL passer rating |
| `completions` | integer | The number of completed passes. |
| `completion_percentage` | double | Percentage of completed passes |
| `expected_completion_percentage` | double | Using a passer's Completion Probability on every play, determine what a passer's completion percentage is expected to be. |
| `completion_percentage_above_expectation` | double | A passer's actual completion percentage compared to their Expected Completion Percentage. |
| `avg_air_distance` | double | A receiver's average depth of target |
| `max_air_distance` | double | A receiver's maximum depth of target |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

# Rushing NextGen stats

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

# Receiving NextGen stats with a follow-up filter

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

# Pandas round-trip

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### `load_nfl_combine(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_combine}

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `draft_year` | double | Year that player was drafted |
| `draft_team` | character | Team that drafted player |
| `draft_round` | double | Round that player was drafted in |
| `draft_ovr` | double | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfb_id` | character | Sports Reference (CFB) ID for player |
| `player_name` | character | Full name of player |
| `pos` | character | Position as tracked by FP |
| `school` | character | College of player |
| `ht` | character | Height of player (feet and inches) |
| `wt` | double | Weight of player (lbs) |
| `forty` | double | Player's 40 yard dash time at combine (seconds) |
| `bench` | double | Reps benched by player at combine |
| `vertical` | double | Player's vertical jump at combine (inches) |
| `broad_jump` | double | Player's broad jump at combine (inches) |
| `cone` | double | Player's 3 cone drill time at combine (seconds) |
| `shuttle` | double | Player's shuttle run time at combine (seconds) |

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

# Filter by draft year and position

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### `load_nfl_contracts(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_contracts}

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `is_active` | logical | Active contract |
| `year_signed` | integer | Year the contract was signed |
| `years` | integer | Contract length |
| `value` | double | Total contract value |
| `apy` | double | Average money per contract year |
| `guaranteed` | double | Total guaranteed money |
| `apy_cap_pct` | double | Average money per contract year as percentage of the team's salary cap at signing |
| `inflated_value` | double | Total contract value inflated to account for the rise of the salary cap |
| `inflated_apy` | double | Average money per contract year inflated to account for the rise of the salary cap |
| `inflated_guaranteed` | double | Total guaranteed money inflated to account for the rise of the salary cap |
| `player_page` | character | Player's OverTheCap url |
| `otc_id` | integer | Over the Cap ID for player |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `date_of_birth` | character | Player date of birth (if published). |
| `height` | character | Official height, in inches |
| `weight` | character | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_overall` | integer | Overall draft selection number. |
| `draft_team` | character | Team that drafted player |
| `cols` | double | Number of contract columns returned in the contracts dataset (metadata artifact from the loader). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

# Pandas round-trip with sort by APY

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### `load_nfl_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_draft_picks}

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `round` | integer | Draft round |
| `pick` | integer | Draft overall pick |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `cfb_player_id` | character | ID from College Football Reference |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `hof` | logical | Whether player has been selected to the Pro Football Hall of Fame |
| `position` | character | Primary position as reported by NFL.com |
| `category` | character | Broader category of player positions |
| `side` | character | O for offense, D for defense, S for special teams |
| `college` | character | Official college (usually the last one attended) |
| `age` | integer | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `to` | integer | Final season played in NFL |
| `allpro` | integer | Number of AP First Team All-Pro selections as recorded by PFR |
| `probowls` | integer | Number of Pro Bowls |
| `seasons_started` | integer | Number of seasons recorded as primary starter for position |
| `w_av` | integer | Weighted Approximate Value |
| `car_av` | logical | Career Approximate Value |
| `dr_av` | integer | Draft Approximate Value |
| `games` | integer | Games played in career |
| `pass_completions` | integer | Number of successful completions for a given game |
| `pass_attempts` | integer | Career pass attempts |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_tds` | integer | Career pass touchdowns thrown |
| `pass_ints` | integer | Career pass interceptions thrown |
| `rush_atts` | integer | Career rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `rush_tds` | integer | Career rushing touchdowns |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `rec_yards` | integer | Career receiving yards |
| `rec_tds` | integer | Career receiving touchdowns |
| `def_solo_tackles` | integer | Career solo tackles |
| `def_ints` | integer | Career interceptions |
| `def_sacks` | double | Number of sacks form this player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

# Filter to a single year and round

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### `load_nfl_espn_qbr(seasons: 'List[int]', summary_type: 'str' = 'season', return_as_pandas: 'bool' = False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'` {#load_nfl_espn_qbr}

Load ESPN Total QBR (Quarterback Rating) data going back to 2006.

Mirrors nflreadpy / nflreadr `load_espn_qbr` -- the lone nflreadpy dataset
that previously had no sdv-py loader. ESPN publishes Total QBR only from 2006
onward, so 2006 is the earliest available season (unlike the 1999 floor on
play-by-play). nflverse republishes ESPN's QBR through the `espn_data`
release as two combined files (one per `summary_type`), each covering all
seasons; this loader reads the requested file once and post-filters by
`season` (the same access pattern as `load_nfl_schedule`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Seasons to return. 2006 is the earliest available season. |
| `summary_type` | `str` | `'season'` | Aggregation level. `"season"` (default) returns one row per quarterback-season; `"week"` returns one row per quarterback-game. Any other value raises `ValueError`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which QBR release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse `espn_data` release. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_espn_qbr` release (built by `nfl-data` from ESPN's QBR web endpoint -- the same source nflverse's espnscrapeR uses). Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing ESPN Total QBR for the requested seasons, summarized per `summary_type`.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (year) the Total QBR record covers. |
| `season_type` | character | Season segment for the record -- regular season or postseason. |
| `game_week` | character | Week scope of the QBR aggregation; for season-level rows this is the season-summary tag. |
| `team_abb` | character | Team abbreviation for the quarterback's team during the period. |
| `player_id` | character | ESPN athlete identifier for the quarterback. |
| `name_short` | character | Abbreviated display name of the quarterback (e.g. 'P. Mahomes'). |
| `rank` | double | Quarterback's rank by Total QBR among qualified passers for the period. |
| `qbr_total` | double | ESPN Total QBR on a 0-100 scale -- the headline opponent-adjusted quarterback rating. |
| `pts_added` | double | Points the quarterback added versus a league-average passer (ESPN QBR points-added component). |
| `qb_plays` | double | Count of qualifying quarterback action plays used to compute QBR. |
| `epa_total` | double | Total expected points added across the quarterback's plays (ESPN QBR EPA component). |
| `pass` | double | QBR points contribution from pass plays. |
| `run` | double | QBR points contribution from designed runs and scrambles. |
| `exp_sack` | double | QBR points contribution adjustment from expected sacks. |
| `penalty` | double | QBR points contribution from penalties attributed to the quarterback. |
| `qbr_raw` | double | Raw (non-opponent-adjusted) QBR for the period. |
| `sack` | double | QBR points contribution from sacks taken. |
| `name_first` | character | Quarterback's first name. |
| `name_last` | character | Quarterback's last name. |
| `name_display` | character | Quarterback's full display name. |
| `headshot_href` | character | URL of the quarterback's ESPN headshot image. |
| `team` | character | Full team name for the quarterback's team during the period. |
| `qualified` | logical | Whether the quarterback met ESPN's minimum action-play threshold to qualify for ranking. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_espn_qbr
qbr = load_nfl_espn_qbr(seasons=[2024])
qbr.shape

# Week-level QBR

qbr_week = load_nfl_espn_qbr(seasons=[2024], summary_type="week")

# Multi-season range

qbr = load_nfl_espn_qbr(seasons=range(2020, 2025))

# Pandas round-trip

qbr_pd = load_nfl_espn_qbr(seasons=[2024], return_as_pandas=True)
qbr_pd[["season", "team_abb", "qbr_total"]].head()
```

### `load_nfl_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_ff_opportunity}

Load NFL fantasy football opportunity data from ffverse/ffopportunity

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2006 is the earliest available season. |
| `stat_type` | `str` | `'weekly'` | One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly". |
| `model_version` | `str` | `'latest'` | One of "latest", "v1.0.0". Defaults to "latest". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football opportunity data for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | character | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `posteam` | character | String abbreviation for the team with possession. |
| `week` | double | Season week. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `full_name` | character | Full name as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `rec_attempt` | double | Total number of targets for a given game |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_air_yards` | double | Total air yards thrown for a given game |
| `rec_air_yards` | double | Total air yards on receiving attempts for a given game |
| `pass_completions` | double | Number of successful completions for a given game |
| `receptions` | double | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `pass_completions_exp` | double | Expected number of pass_completions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `receptions_exp` | double | Expected number of receptions in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_yards_gained` | double | Total passing yards gained for a given game |
| `rec_yards_gained` | double | Total receiving yards gained for a given game |
| `rush_yards_gained` | double | Total rushing yards gained for a given game |
| `pass_yards_gained_exp` | double | Expected number of pass_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_yards_gained_exp` | double | Expected number of rec_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_yards_gained_exp` | double | Expected number of rush_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rec_touchdown` | double | Total receiving touchdowns |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `pass_touchdown_exp` | double | Expected number of pass_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_touchdown_exp` | double | Expected number of rec_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_touchdown_exp` | double | Expected number of rush_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_two_point_conv` | double | Number of successful passing two point conversions |
| `rec_two_point_conv` | double | Number of successful receiving two point conversions |
| `rush_two_point_conv` | double | Number of successful rushing two point conversions |
| `pass_two_point_conv_exp` | double | Expected number of pass_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_two_point_conv_exp` | double | Expected number of rec_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_two_point_conv_exp` | double | Expected number of rush_two_point_conv in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_first_down` | double | Number of passing first downs |
| `rec_first_down` | double | Number of receiving first downs |
| `rush_first_down` | double | Number of rushing first downs |
| `pass_first_down_exp` | double | Expected number of pass_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_first_down_exp` | double | Expected number of rec_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_first_down_exp` | double | Expected number of rush_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_interception` | double | Number of interceptions thrown |
| `rec_interception` | double | Number of interceptions on targets |
| `pass_interception_exp` | double | Expected number of pass_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_interception_exp` | double | Expected number of rec_interception in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fumble_lost` | double | Number of fumbles on receiving attempts |
| `rush_fumble_lost` | double | Number of fumbles on rushing attempts |
| `pass_fantasy_points_exp` | double | Expected number of pass_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rec_fantasy_points_exp` | double | Expected number of rec_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `rush_fantasy_points_exp` | double | Expected number of rush_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_fantasy_points` | double | Total fantasy points from passing, assuming 0.04 points per pass yard, 4 points per pass TD, -2 points per interception |
| `rec_fantasy_points` | double | Total fantasy points from receiving, assuming PPR scoring |
| `rush_fantasy_points` | double | Total fantasy points from rushing, assuming PPR scoring |
| `total_yards_gained` | double | Total scrimmage yards (sum of pass, rush, and receiving yards) |
| `total_yards_gained_exp` | double | Expected number of total_yards_gained in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_touchdown` | double | Total touchdowns (sum of pass, rush, and receiving touchdowns) |
| `total_touchdown_exp` | double | Expected number of total_touchdown in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_first_down` | double | Total first downs (sum of pass, rush, and receiving first downs) |
| `total_first_down_exp` | double | Expected number of total_first_down in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `total_fantasy_points` | double | Total fantasy points (sum of pass, rush, and receiving fantasy points) |
| `total_fantasy_points_exp` | double | Expected number of total_fantasy_points in this game (weekly) or on this play (pbp_rush/pbp_pass) given situation |
| `pass_completions_diff` | double | Difference between actual and expected number of pass_completions - often interpreted as efficiency for a given play/game |
| `receptions_diff` | double | Difference between actual and expected number of receptions - often interpreted as efficiency for a given play/game |
| `pass_yards_gained_diff` | double | Difference between actual and expected number of pass_yards_gained - often interpreted as efficiency for a given play/game |
| `rec_yards_gained_diff` | double | Difference between actual and expected number of rec_yards_gained - often interpreted as efficiency for a given play/game |
| `rush_yards_gained_diff` | double | Difference between actual and expected number of rush_yards_gained - often interpreted as efficiency for a given play/game |
| `pass_touchdown_diff` | double | Difference between actual and expected number of pass_touchdown - often interpreted as efficiency for a given play/game |
| `rec_touchdown_diff` | double | Difference between actual and expected number of rec_touchdown - often interpreted as efficiency for a given play/game |
| `rush_touchdown_diff` | double | Difference between actual and expected number of rush_touchdown - often interpreted as efficiency for a given play/game |
| `pass_two_point_conv_diff` | double | Difference between actual and expected number of pass_two_point_conv - often interpreted as efficiency for a given play/game |
| `rec_two_point_conv_diff` | double | Difference between actual and expected number of rec_two_point_conv - often interpreted as efficiency for a given play/game |
| `rush_two_point_conv_diff` | double | Difference between actual and expected number of rush_two_point_conv - often interpreted as efficiency for a given play/game |
| `pass_first_down_diff` | double | Difference between actual and expected number of pass_first_down - often interpreted as efficiency for a given play/game |
| `rec_first_down_diff` | double | Difference between actual and expected number of rec_first_down - often interpreted as efficiency for a given play/game |
| `rush_first_down_diff` | double | Difference between actual and expected number of rush_first_down - often interpreted as efficiency for a given play/game |
| `pass_interception_diff` | double | Difference between actual and expected number of pass_interception - often interpreted as efficiency for a given play/game |
| `rec_interception_diff` | double | Difference between actual and expected number of rec_interception - often interpreted as efficiency for a given play/game |
| `pass_fantasy_points_diff` | double | Difference between actual and expected number of pass_fantasy_points - often interpreted as efficiency for a given play/game |
| `rec_fantasy_points_diff` | double | Difference between actual and expected number of rec_fantasy_points - often interpreted as efficiency for a given play/game |
| `rush_fantasy_points_diff` | double | Difference between actual and expected number of rush_fantasy_points - often interpreted as efficiency for a given play/game |
| `total_yards_gained_diff` | double | Difference between actual and expected number of total_yards_gained - often interpreted as efficiency for a given play/game |
| `total_touchdown_diff` | double | Difference between actual and expected number of total_touchdown - often interpreted as efficiency for a given play/game |
| `total_first_down_diff` | double | Difference between actual and expected number of total_first_down - often interpreted as efficiency for a given play/game |
| `total_fantasy_points_diff` | double | Difference between actual and expected number of total_fantasy_points - often interpreted as efficiency for a given play/game |
| `pass_attempt_team` | double | Team-level total pass_attempt for a game, summed across all plays/players for that team. |
| `rec_attempt_team` | double | Team-level total rec_attempt for a game, summed across all plays/players for that team. |
| `rush_attempt_team` | double | Team-level total rush_attempt for a game, summed across all plays/players for that team. |
| `pass_air_yards_team` | double | Team-level total pass_air_yards for a game, summed across all plays/players for that team. |
| `rec_air_yards_team` | double | Team-level total rec_air_yards for a game, summed across all plays/players for that team. |
| `pass_completions_team` | double | Team-level total pass_completions for a game, summed across all plays/players for that team. |
| `receptions_team` | double | Team-level total receptions for a game, summed across all plays/players for that team. |
| `pass_completions_exp_team` | double | Team-level total expected pass_completions_exp for a game, summed across all plays & players for that team. |
| `receptions_exp_team` | double | Team-level total expected receptions_exp for a game, summed across all plays & players for that team. |
| `pass_yards_gained_team` | double | Team-level total pass_yards_gained for a game, summed across all plays/players for that team. |
| `rec_yards_gained_team` | double | Team-level total rec_yards_gained for a game, summed across all plays/players for that team. |
| `rush_yards_gained_team` | double | Team-level total rush_yards_gained for a game, summed across all plays/players for that team. |
| `pass_yards_gained_exp_team` | double | Team-level total expected pass_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rec_yards_gained_exp_team` | double | Team-level total expected rec_yards_gained_exp for a game, summed across all plays & players for that team. |
| `rush_yards_gained_exp_team` | double | Team-level total expected rush_yards_gained_exp for a game, summed across all plays & players for that team. |
| `pass_touchdown_team` | double | Team-level total pass_touchdown for a game, summed across all plays/players for that team. |
| `rec_touchdown_team` | double | Team-level total rec_touchdown for a game, summed across all plays/players for that team. |
| `rush_touchdown_team` | double | Team-level total rush_touchdown for a game, summed across all plays/players for that team. |
| `pass_touchdown_exp_team` | double | Team-level total expected pass_touchdown_exp for a game, summed across all plays & players for that team. |
| `rec_touchdown_exp_team` | double | Team-level total expected rec_touchdown_exp for a game, summed across all plays & players for that team. |
| `rush_touchdown_exp_team` | double | Team-level total expected rush_touchdown_exp for a game, summed across all plays & players for that team. |
| `pass_two_point_conv_team` | double | Team-level total pass_two_point_conv for a game, summed across all plays/players for that team. |
| `rec_two_point_conv_team` | double | Team-level total rec_two_point_conv for a game, summed across all plays/players for that team. |
| `rush_two_point_conv_team` | double | Team-level total rush_two_point_conv for a game, summed across all plays/players for that team. |
| `pass_two_point_conv_exp_team` | double | Team-level total expected pass_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rec_two_point_conv_exp_team` | double | Team-level total expected rec_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `rush_two_point_conv_exp_team` | double | Team-level total expected rush_two_point_conv_exp for a game, summed across all plays & players for that team. |
| `pass_first_down_team` | double | Team-level total pass_first_down for a game, summed across all plays/players for that team. |
| `rec_first_down_team` | double | Team-level total rec_first_down for a game, summed across all plays/players for that team. |
| `rush_first_down_team` | double | Team-level total rush_first_down for a game, summed across all plays/players for that team. |
| `pass_first_down_exp_team` | double | Team-level total expected pass_first_down_exp for a game, summed across all plays & players for that team. |
| `rec_first_down_exp_team` | double | Team-level total expected rec_first_down_exp for a game, summed across all plays & players for that team. |
| `rush_first_down_exp_team` | double | Team-level total expected rush_first_down_exp for a game, summed across all plays & players for that team. |
| `pass_interception_team` | double | Team-level total pass_interception for a game, summed across all plays/players for that team. |
| `rec_interception_team` | double | Team-level total rec_interception for a game, summed across all plays/players for that team. |
| `pass_interception_exp_team` | double | Team-level total expected pass_interception_exp for a game, summed across all plays & players for that team. |
| `rec_interception_exp_team` | double | Team-level total expected rec_interception_exp for a game, summed across all plays & players for that team. |
| `rec_fumble_lost_team` | double | Team-level total rec_fumble_lost for a game, summed across all plays/players for that team. |
| `rush_fumble_lost_team` | double | Team-level total rush_fumble_lost for a game, summed across all plays/players for that team. |
| `pass_fantasy_points_exp_team` | double | Team-level total expected pass_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rec_fantasy_points_exp_team` | double | Team-level total expected rec_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `rush_fantasy_points_exp_team` | double | Team-level total expected rush_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `pass_fantasy_points_team` | double | Team-level total pass_fantasy_points for a game, summed across all plays/players for that team. |
| `rec_fantasy_points_team` | double | Team-level total rec_fantasy_points for a game, summed across all plays/players for that team. |
| `rush_fantasy_points_team` | double | Team-level total rush_fantasy_points for a game, summed across all plays/players for that team. |
| `pass_completions_diff_team` | double | Team-level difference between actual and expected number of pass_completions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `receptions_diff_team` | double | Team-level difference between actual and expected number of receptions_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_yards_gained_diff_team` | double | Team-level difference between actual and expected number of pass_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rec_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_yards_gained_diff_team` | double | Team-level difference between actual and expected number of rush_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_touchdown_diff_team` | double | Team-level difference between actual and expected number of pass_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_touchdown_diff_team` | double | Team-level difference between actual and expected number of rec_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_touchdown_diff_team` | double | Team-level difference between actual and expected number of rush_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of pass_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rec_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_two_point_conv_diff_team` | double | Team-level difference between actual and expected number of rush_two_point_conv_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_first_down_diff_team` | double | Team-level difference between actual and expected number of pass_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_first_down_diff_team` | double | Team-level difference between actual and expected number of rec_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_first_down_diff_team` | double | Team-level difference between actual and expected number of rush_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_interception_diff_team` | double | Team-level difference between actual and expected number of pass_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_interception_diff_team` | double | Team-level difference between actual and expected number of rec_interception_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `pass_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of pass_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rec_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rec_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `rush_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of rush_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_yards_gained_team` | double | Team-level total total_yards_gained for a game, summed across all plays/players for that team. |
| `total_yards_gained_exp_team` | double | Team-level total expected total_yards_gained_exp for a game, summed across all plays & players for that team. |
| `total_yards_gained_diff_team` | double | Team-level difference between actual and expected number of total_yards_gained_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_touchdown_team` | double | Team-level total total_touchdown for a game, summed across all plays/players for that team. |
| `total_touchdown_exp_team` | double | Team-level total expected total_touchdown_exp for a game, summed across all plays & players for that team. |
| `total_touchdown_diff_team` | double | Team-level difference between actual and expected number of total_touchdown_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_first_down_team` | double | Team-level total total_first_down for a game, summed across all plays/players for that team. |
| `total_first_down_exp_team` | double | Team-level total expected total_first_down_exp for a game, summed across all plays & players for that team. |
| `total_first_down_diff_team` | double | Team-level difference between actual and expected number of total_first_down_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |
| `total_fantasy_points_team` | double | Team-level total total_fantasy_points for a game, summed across all plays/players for that team. |
| `total_fantasy_points_exp_team` | double | Team-level total expected total_fantasy_points_exp for a game, summed across all plays & players for that team. |
| `total_fantasy_points_diff_team` | double | Team-level difference between actual and expected number of total_fantasy_points_diff for a game, summed across all plays/players for that team. Often interpreted as team-level efficiency. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

# Pass play-by-play opportunity stats

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

# Rush play-by-play opportunity stats with pinned model version

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### `load_nfl_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_ff_playerids}

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

| col_name | type | description |
|---|---|---|
| `mfl_id` | integer | MyFantasyLeague.com ID - this is the primary key for this table and is unique and complete. Usually an integer of 5 digits. |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `fantasypros_id` | character | FantasyPros.com ID - usually an integer of 5 digits. |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `sleeper_id` | integer | Sleeper ID - usually an integer with ~4 digits. |
| `nfl_id` | character | NFL ID of player (this is used in Big Data Bowl Data) |
| `espn_id` | integer | ESPN ID - usual format is an integer with ~5 digits |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `fleaflicker_id` | character | Fleaflicker ID - usual format is an integer with ~4 digits. Fleaflicker API also has sportradar and that's generally preferred. |
| `cbs_id` | integer | CBS ID - usual format is an integer with ~ 7 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `cfbref_id` | character | College Football Reference ID - usual format is firstname-lastname-integer |
| `rotowire_id` | integer | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `rotoworld_id` | character | Rotoworld ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `ktc_id` | integer | KeepTradeCut ID - usual format is an integer with ~four digits. |
| `stats_id` | integer | Stats ID - usual format is five digit integer |
| `stats_global_id` | integer | Stats Global ID - usual format is a six digit integer |
| `fantasy_data_id` | integer | FantasyData ID - usual format five digit integer |
| `swish_id` | character | Player ID for Swish Analytics |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `merge_name` | character | Name but formatted for name joins via ffscrapr::dp_cleannames() - coerced to lowercase, stripped of punctuation and suffixes, and common substitutions performed. |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `birthdate` | character | Birthdate |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `draft_year` | integer | Year that player was drafted |
| `draft_round` | integer | Round that player was drafted in |
| `draft_pick` | integer | Draft pick within round, i.e. 32nd pick of second round. |
| `draft_ovr` | integer | Overall draft pick selection. This can be a little bit patchy, since MFL does not report this number. |
| `twitter_username` | character | Official twitter handle, if known |
| `height` | integer | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `db_season` | integer | Year of database build. Previous years may also be available via dynastyprocess. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

# Filter to active QBs

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### `load_nfl_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_ff_rankings}

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of `"draft"` (current draft rankings), `"week"` (weekly rankings), or `"all"` (full historical rankings). Defaults to `"draft"`. Kept for nflreadpy parity since its parameter is also called `type`; the forward-going preferred name is `kind`. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as `type`. If both are supplied, `kind` wins. If neither is supplied, defaults to `"draft"` via `type`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

| col_name | type | description |
|---|---|---|
| `fp_page` | character | The relative url that the data was scraped from (add the prefix https://www.fantasypros.com/ to visit the page) |
| `page_type` | character | Two word identifier separated by a dash identifying the type of fantasy ranking (best = bestball; dynasty; redraft) and what position it applies to |
| `ecr_type` | character | A two letter identifier combining the ranking type (b = bestball; d = dynasty; r = redraft) and position type (o = overall; p = positional; sf = superflex; rk = rookie) |
| `player` | character | Player name |
| `id` | integer | ID of the player in the 'name' column. |
| `pos` | character | Position as tracked by FP |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `ecr` | double | Average (mean) expert ranking for this player |
| `sd` | double | Standard deviation of expert rankings for this player |
| `best` | integer | The highest ranking given for this player by any one expert |
| `worst` | integer | The lowest ranking given for this player by any one expert |
| `sportsdata_id` | character | ID - also known as sportradar_id (they are equivalent!) |
| `player_filename` | character | base URL for this player on fantasypros.com |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `cbs_id` | character | CBS ID - usual format is an integer with ~ 7 digits. |
| `player_owned_avg` | double | The average percentage this player is rostered across ESPN and Yahoo |
| `player_owned_espn` | character | The percentage that this player is rostered in ESPN leagues |
| `player_owned_yahoo` | character | The percentage that this player is rostered in Yahoo leagues |
| `player_image_url` | character | An image of the player |
| `player_square_image_url` | character | An square image of the player |
| `rank_delta` | integer | Change in ranks over a recent period |
| `bye` | integer | NFL bye week |
| `mergename` | character | Player name after being cleaned by dp_cleannames - generally strips punctuation and suffixes as well as performing common name substitutions. |
| `scrape_date` | character | Date this dataframe was last updated |
| `tm` | character | Team ID as used on MyFantasyLeague.com |

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

# Weekly rankings

weekly = load_nfl_ff_rankings(kind="week")

# Full historical rankings (parquet)

history = load_nfl_ff_rankings(kind="all")

# nflreadpy-parity ``type=`` parameter (still supported)

draft = load_nfl_ff_rankings(type="draft")
```

### `load_nfl_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_nextgen_stats}

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats
accessors. Mirrors the API surface of nflreadpy's
`load_nextgen_stats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — `seasons` is applied as a post-filter on the `season` column. |
| `stat_type` | `str` | `'passing'` | One of `"passing"`, `"rushing"`, `"receiving"`. Defaults to `"passing"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested `stat_type` and `seasons`.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_time_to_throw` | double | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `avg_completed_air_yards` | double | Average air yards on completed passes |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `avg_air_yards_differential` | double | Air Yards Differential is calculated by subtracting the passer's average Intended Air Yards from his average Completed Air Yards. This stat indicates if he is on average attempting deep passes than he on average completes. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `max_completed_air_distance` | double | Air Distance is the amount of yards the ball has traveled on a pass, from the point of release to the point of reception (as the crow flies). Unlike Air Yards, Air Distance measures the actual distance the passer throws the ball. |
| `avg_air_yards_to_sticks` | double | Air Yards to the Sticks shows the amount of Air Yards ahead or behind the first down marker on all attempts for a passer. The metric indicates if the passer is attempting his passes past the 1st down marker, or if he is relying on his skill position players to make yards after catch. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_touchdowns` | integer | Number of touchdowns scored on pass plays |
| `interceptions` | integer | The number of interceptions thrown. |
| `passer_rating` | double | Overall NFL passer rating |
| `completions` | integer | The number of completed passes. |
| `completion_percentage` | double | Percentage of completed passes |
| `expected_completion_percentage` | double | Using a passer's Completion Probability on every play, determine what a passer's completion percentage is expected to be. |
| `completion_percentage_above_expectation` | double | A passer's actual completion percentage compared to their Expected Completion Percentage. |
| `avg_air_distance` | double | A receiver's average depth of target |
| `max_air_distance` | double | A receiver's maximum depth of target |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

# Rushing NextGen stats

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

# Receiving NextGen stats with a follow-up filter

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

# Pandas round-trip

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### `load_nfl_ngs_passing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_ngs_passing}

Deprecated alias for `load_nfl_nextgen_stats(stat_type='passing')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_time_to_throw` | double | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `avg_completed_air_yards` | double | Average air yards on completed passes |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `avg_air_yards_differential` | double | Air Yards Differential is calculated by subtracting the passer's average Intended Air Yards from his average Completed Air Yards. This stat indicates if he is on average attempting deep passes than he on average completes. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `max_completed_air_distance` | double | Air Distance is the amount of yards the ball has traveled on a pass, from the point of release to the point of reception (as the crow flies). Unlike Air Yards, Air Distance measures the actual distance the passer throws the ball. |
| `avg_air_yards_to_sticks` | double | Air Yards to the Sticks shows the amount of Air Yards ahead or behind the first down marker on all attempts for a passer. The metric indicates if the passer is attempting his passes past the 1st down marker, or if he is relying on his skill position players to make yards after catch. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `pass_yards` | integer | Number of yards gained on pass plays |
| `pass_touchdowns` | integer | Number of touchdowns scored on pass plays |
| `interceptions` | integer | The number of interceptions thrown. |
| `passer_rating` | double | Overall NFL passer rating |
| `completions` | integer | The number of completed passes. |
| `completion_percentage` | double | Percentage of completed passes |
| `expected_completion_percentage` | double | Using a passer's Completion Probability on every play, determine what a passer's completion percentage is expected to be. |
| `completion_percentage_above_expectation` | double | A passer's actual completion percentage compared to their Expected Completion Percentage. |
| `avg_air_distance` | double | A receiver's average depth of target |
| `max_air_distance` | double | A receiver's maximum depth of target |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")
```

### `load_nfl_ngs_receiving(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_ngs_receiving}

Deprecated alias for `load_nfl_nextgen_stats(stat_type='receiving')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `avg_cushion` | double | The distance (in yards) measured between a WR/TE and the defender they're lined up against at the time of snap on all targets. |
| `avg_separation` | double | The distance (in yards) measured between a WR/TE and the nearest defender at the time of catch or incompletion. |
| `avg_intended_air_yards` | double | Average air yards on all attempted passes |
| `percent_share_of_intended_air_yards` | double | The sum of the receivers total intended air yards (all attempts) over the sum of his team's total intended air yards. Represented as a percentage, this statistic represents how much of a team's deep yards does the player account for. |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `catch_percentage` | double | Percentage of caught passes relative to targets |
| `yards` | integer | The number of receiving yards |
| `rec_touchdowns` | integer | The number of touchdown receptions |
| `avg_yac` | double | Average yards gained after catch by a receiver. |
| `avg_expected_yac` | double | Average expected yards after catch, based on numerous factors using tracking data such as how open the receiver is, how fast they're traveling, how many defenders/blockers are in space, etc |
| `avg_yac_above_expectation` | double | A receiver's YAC compared to their Expected YAC. |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
```

### `load_nfl_ngs_rushing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_ngs_rushing}

Deprecated alias for `load_nfl_nextgen_stats(stat_type='rushing')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_nextgen_stats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `player_display_name` | character | Full name of the player |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_abbr` | character | Official team abbreveation |
| `efficiency` | double | Rushing efficiency is calculated by taking the total distance a player traveled on rushing plays as a ball carrier according to Next Gen Stats (measured in yards) per rushing yards gained. The lower the number, the more of a North/South runner. |
| `percent_attempts_gte_eight_defenders` | double | On every play, Next Gen Stats calculates how many defenders are stacked in the box at snap. Using that logic, DIB% calculates how often does a rusher see 8 or more defenders in the box against them. |
| `avg_time_to_los` | double | Next Gen Stats measures the amount of time a ball carrier spends (measured to the 10th of a second) before crossing the Line of Scrimmage. TLOS is the average time behind the LOS on all rushing plays where the player is the rusher. |
| `rush_attempts` | integer | The number of rushing attempts |
| `rush_yards` | integer | The number of rushing yards gained |
| `avg_rush_yards` | double | AVerage rush yards gained |
| `rush_touchdowns` | integer | The number of scored rushing touchdowns |
| `player_gsis_id` | character | Unique identifier of the player |
| `player_first_name` | character | Player's first name |
| `player_last_name` | character | Player's last name |
| `player_jersey_number` | integer | Player's jersey number |
| `player_short_name` | character | Short version of player's name |
| `expected_rush_yards` | double | Expected rushing yards based on Nextgenstats' Big Data Bowl model |
| `rush_yards_over_expected` | double | A rusher's rush yards gained compared to the expected rush yards |
| `rush_yards_over_expected_per_att` | double | Average rush yards above expectation |
| `rush_pct_over_expected` | double | Rushing percentage above expectation |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")
```

### `load_nfl_officials(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_officials}

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `game_key` | character | Unique nflverse game identifier linking the officiating record to a specific NFL game. |
| `official_name` | character | Official name. |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | character | Unique official / referee identifier. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

# Pandas round-trip

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### `load_nfl_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_advstats}

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level
PFR advstats accessors. Mirrors the API surface of nflreadpy's
`load_pfr_advstats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For `summary_level='week'` this drives the per-season parquet fan-out; for `summary_level='season'` it post-filters the combined parquet by the `season` column. |
| `stat_type` | `str` | `'pass'` | One of `"pass"`, `"rush"`, `"rec"`, `"def"`. Defaults to `"pass"`. |
| `summary_level` | `str` | `'week'` | One of `"week"` or `"season"`. Defaults to `"week"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested `stat_type`, `summary_level`, and `seasons`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `passing_drops` | double | Total number of catchable passes thrown by the passer that were dropped by receivers per Pro Football Reference. |
| `passing_drop_pct` | double | Percentage of a passer's catchable targets that were dropped by the intended receiver. |
| `receiving_drop` | double | Number of catchable targets the receiver dropped during the game or season period per Pro Football Reference. |
| `receiving_drop_pct` | double | Percentage of the receiver's catchable targets that were dropped during the game or season period. |
| `passing_bad_throws` | double | Total number of passes thrown by the passer that were classified as inaccurate or poor-quality throws per Pro Football Reference. |
| `passing_bad_throw_pct` | double | Percentage of a passer's attempts classified as bad throws (inaccurate, off-target, or uncatchable). |
| `times_sacked` | double | Total number of times the passer was sacked during the game or season period per Pro Football Reference. |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double | Percentage of the passer's dropbacks during which they faced pressure from the opposing defense. |
| `def_times_blitzed` | double | Number of times the defensive player sent additional rushers on a blitz during the game or season period. |
| `def_times_hurried` | double | Number of times the defensive player hurried or pressured the quarterback without recording a sack. |
| `def_times_hitqb` | double | Number of times the defensive player made contact with the quarterback (hit on the QB) during pass rushes. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)

# Season-level rushing summaries (one row per player per season)

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

# Defensive stats with a follow-up filter

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

# Pandas round-trip

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### `load_nfl_pfr_def(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_def}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double | Goals (skaters). |
| `gs` | double | Number of games the player started during the season or period covered by this row. |
| `int` | double | Binary flag for an interception. |
| `tgt` | double | Total number of times the player was the nearest defender on a pass attempt (targets in coverage) per Pro Football Reference. |
| `cmp` | double | Number of passes completed by the opposing quarterback when targeting the player in coverage. |
| `cmp_percent` | double | Completion percentage allowed by the player in coverage (completions divided by targets). |
| `yds` | double | Total passing yards allowed by the player in coverage. |
| `yds_cmp` | double | Average yards allowed per completion when the player was in coverage. |
| `yds_tgt` | double | Average yards allowed per target thrown at the player in coverage. |
| `td` | double | Number of touchdowns allowed by the player in coverage. |
| `rat` | double | Passer rating allowed by the player in coverage — the NFL passer rating of quarterbacks when targeting this defender. |
| `dadot` | double | Depth of target air yards on defended passes — average distance downfield at the point of the throw when the player was in coverage. |
| `air` | double | Total air yards (depth of target) on passes thrown at the player in coverage, as tracked by Pro Football Reference. |
| `yac` | double | Yards after catch allowed by the player — yards gained by receivers after the catch when the player was the nearest defender. |
| `bltz` | double | Number of snaps on which the player blitzed the quarterback, as recorded by Pro Football Reference. |
| `hrry` | double | Number of times the player hurried the opposing quarterback without recording a full sack, per Pro Football Reference. |
| `qbkd` | double | Number of times the player knocked down the quarterback, making contact after or during a pass attempt. |
| `sk` | double | Number of sacks recorded by the player, bringing the quarterback down behind the line of scrimmage. |
| `prss` | double | Number of times the player pressured the quarterback (combining sacks, hits, and hurries) per Pro Football Reference. |
| `comb` | double | Total combined tackles (solo plus assisted) recorded by the player per Pro Football Reference. |
| `m_tkl` | double | Number of missed tackles attributed to the player by Pro Football Reference. |
| `m_tkl_percent` | double | Percentage of tackle attempts the player missed out of total tackle opportunities. |
| `loaded` | character | Indicator or metadata field from the Pro Football Reference data load, typically flagging the data source state or row completeness. |
| `bats` | double | Number of passes batted down at the line of scrimmage by the player. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="season"
)
```

### `load_nfl_pfr_pass(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_pass}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `player` | character | Player name |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `pass_attempts` | double | Career pass attempts |
| `throwaways` | double | Throwaways |
| `spikes` | double | Spikes |
| `drops` | double | Throws dropped |
| `drop_pct` | double | Percent of throws dropped |
| `bad_throws` | double | Bad throws |
| `bad_throw_pct` | double | Percent of throws that were bad |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pocket_time` | double | Average time in pocket |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `pressure_pct` | double | Percent of the time pressured |
| `batted_balls` | double | Batted balls |
| `on_tgt_throws` | double | On target throws |
| `on_tgt_pct` | double | Percent of throws on target |
| `rpo_plays` | double | Number of RPO plays |
| `rpo_yards` | double | Yards on RPOs |
| `rpo_pass_att` | double | Number of pass attempts on RPOs |
| `rpo_pass_yards` | double | Passing yards on RPOs |
| `rpo_rush_att` | double | Rush attempts on RPOs |
| `rpo_rush_yards` | double | Rushing yards on RPOs |
| `pa_pass_att` | double | Play action pass attempts |
| `pa_pass_yards` | double | Play action passing yards |
| `intended_air_yards` | double | Total air yards on all pass attempts including incompletions, measuring aggregate downfield targeting intent from Pro Football Reference. |
| `intended_air_yards_per_pass_attempt` | double | Average intended air yards per pass attempt, capturing the passer's average depth of target regardless of completion outcome. |
| `completed_air_yards` | double | Total air yards on completed passes only, measuring how far the ball traveled downfield through the air to the point of completion. |
| `completed_air_yards_per_completion` | double | Average air yards per completed pass, representing the passer's typical depth of target on successful throws. |
| `completed_air_yards_per_pass_attempt` | double | Average completed air yards per pass attempt (including incompletions), a rate measure of downfield passing efficiency. |
| `pass_yards_after_catch` | double | Total yards gained by receivers after the catch, isolating the yards generated after initial ball reception from Pro Football Reference. |
| `pass_yards_after_catch_per_completion` | double | Average yards after catch per completion, measuring how much yardage receivers generate on the ground after catching the ball. |
| `scrambles` | double | Total number of quarterback scrambles (designed dropback converted to a run) recorded by Pro Football Reference. |
| `scramble_yards_per_attempt` | double | Average yards gained per scramble attempt by the quarterback, from Pro Football Reference advanced passing stats. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="season"
)
```

### `load_nfl_pfr_rec(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_rec}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double | Goals (skaters). |
| `gs` | double | Number of games the player started at a receiver position during the period covered. |
| `tgt` | double | Total number of times the player was the intended receiver on a pass attempt. |
| `rec` | double | Total receptions made by the player during the period covered. |
| `yds` | double | Total receiving yards gained by the player on all receptions. |
| `td` | double | Total receiving touchdowns scored by the player. |
| `x1d` | double | Number of receptions by the player that resulted in a first down. |
| `ybc` | double | Total yards the ball traveled in the air (before the catch) on receptions by the player. |
| `ybc_r` | double | Average air yards before the catch per reception. |
| `yac` | double | Total yards gained by the player after the catch. |
| `yac_r` | double | Average yards after the catch per reception. |
| `adot` | double | Average depth of target — mean air yards at point of throw on pass attempts directed at the receiver, per Pro Football Reference. |
| `brk_tkl` | double | Number of broken tackles credited to the player after a reception, per Pro Football Reference. |
| `rec_br` | double | Receptions per broken tackle — number of receptions for each broken tackle the player forced after the catch, per Pro Football Reference. |
| `drop` | double | Number of catchable passes the player dropped (failed to secure after the ball reached the receiver's hands). |
| `drop_percent` | double | Percentage of catchable targets that the player dropped. |
| `int` | double | Binary flag for an interception. |
| `rat` | double | Passer rating generated on passes thrown to the player — the NFL passer rating when the receiver is targeted. |
| `loaded` | character | Indicator or metadata field from the Pro Football Reference data load, flagging the row's data source state or completeness. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="season"
)
```

### `load_nfl_pfr_rush(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_rush}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='season')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `player` | character | Player name |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `tm` | character | Team ID as used on MyFantasyLeague.com |
| `age` | double | Age as of last pipeline build, rounded to one decimal. Pipeline is built on a weekly basis. |
| `pos` | character | Position as tracked by FP |
| `g` | double | Goals (skaters). |
| `gs` | double | Number of games started by the player during the period covered. |
| `att` | double | Total rushing attempts by the player during the period covered. |
| `yds` | double | Total rushing yards gained during the period covered. |
| `td` | double | Total rushing touchdowns scored during the period covered. |
| `x1d` | double | Number of first downs gained via rushing during the period covered. |
| `ybc` | double | Yards before contact accumulated on rushing plays, measuring yards gained in open field before being touched. |
| `ybc_att` | double | Yards before contact per rushing attempt. |
| `yac` | double | Yards after contact accumulated on rushing plays. |
| `yac_att` | double | Yards after contact per rushing attempt. |
| `brk_tkl` | double | Number of broken tackles recorded on rushing plays. |
| `att_br` | double | Rushing attempts per broken tackle, measuring how often the player required contact to break free. |
| `loaded` | character | Source or load-batch identifier indicating which data file or release this row was pulled from. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)
```

### `load_nfl_pfr_weekly_def(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_weekly_def}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `def_ints` | double | Career interceptions |
| `def_targets` | double | Number of passing attempts thrown at or into the coverage area of this defender during the week. |
| `def_completions_allowed` | double | Number of completions allowed by the defender on passes thrown into their coverage during the week. |
| `def_completion_pct` | double | Completion percentage allowed by the defender on targets thrown in their coverage during the week. |
| `def_yards_allowed` | double | Total receiving yards allowed by this defender in coverage during the week per Pro Football Reference. |
| `def_yards_allowed_per_cmp` | double | Receiving yards allowed per completion by this defender in coverage during the week. |
| `def_yards_allowed_per_tgt` | double | Receiving yards allowed per target thrown at this defender in coverage during the week. |
| `def_receiving_td_allowed` | double | Number of receiving touchdowns allowed by the defender while in coverage during the week. |
| `def_passer_rating_allowed` | double | NFL passer rating of quarterbacks when targeting this defender in coverage during the week. |
| `def_adot` | double | Average depth of target (in yards) against this defender on passing plays during the week. |
| `def_air_yards_completed` | double | Total air yards on completed passes allowed by the defender during the week. |
| `def_yards_after_catch` | double | Total yards gained by receivers after the catch on completions allowed by this defender during the week. |
| `def_times_blitzed` | double | Number of times this defender was sent as a blitzer on a passing play during the week. |
| `def_times_hurried` | double | Number of times this defender hurried the quarterback on a pass rush without recording a sack during the week. |
| `def_times_hitqb` | double | Number of times this defender made contact with the quarterback as part of a pass rush during the week. |
| `def_sacks` | double | Number of sacks form this player |
| `def_pressures` | double | Total number of quarterback pressures (hurries + hits + sacks) generated by the defender during the week. |
| `def_tackles_combined` | double | Total combined tackles (solo + assisted) recorded by the defender during the week per Pro Football Reference. |
| `def_missed_tackles` | double | Number of missed tackles recorded against this defender during the week per Pro Football Reference. |
| `def_missed_tackle_pct` | double | Percentage of the defender's tackle opportunities that resulted in a missed tackle during the week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="week"
)
```

### `load_nfl_pfr_weekly_pass(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_weekly_pass}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `passing_drops` | double | Number of catchable passes thrown by the quarterback that were dropped by receivers. |
| `passing_drop_pct` | double | Percentage of catchable targets that were dropped by receivers on passes thrown by the quarterback. |
| `receiving_drop` | double | Number of catchable targets the player (as receiver) dropped in this weekly game. |
| `receiving_drop_pct` | double | Percentage of catchable targets the player dropped in this weekly game. |
| `passing_bad_throws` | double | Number of pass attempts charted as poor or inaccurate throws by the quarterback in this weekly log. |
| `passing_bad_throw_pct` | double | Percentage of pass attempts that were charted as poor throws (inaccurate, off-target, or otherwise below expectation), per Pro Football Reference. |
| `times_sacked` | double | Number of times the quarterback was sacked in this weekly game log. |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double | Percentage of pass plays on which the quarterback faced defensive pressure in this weekly game. |
| `def_times_blitzed` | double | Number of pass plays on which the defense sent a blitz, as experienced by the quarterback in this weekly log. |
| `def_times_hurried` | double | Number of times the quarterback was hurried (pressured into an early throw) without being sacked or hit. |
| `def_times_hitqb` | double | Number of times the quarterback was hit by a defender on a pass play in this weekly game log. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)
```

### `load_nfl_pfr_weekly_rec(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_weekly_rec}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `rushing_broken_tackles` | double | Number of broken tackles recorded by the player on rushing plays during the week. |
| `receiving_broken_tackles` | double | Number of broken tackles recorded by the receiver after the catch. |
| `passing_drops` | double | Number of passes dropped by intended receivers on targeted throws. |
| `passing_drop_pct` | double | Percentage of pass targets that resulted in a drop, as tracked by Pro Football Reference. |
| `receiving_drop` | double | Number of catchable passes dropped by the receiver during the week. |
| `receiving_drop_pct` | double | Percentage of catchable targets that were dropped by the receiver during the week. |
| `receiving_int` | double | Number of passes intended for the receiver that were intercepted. |
| `receiving_rat` | double | Passer rating when targeting this receiver, reflecting QB efficiency on those throws. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="week"
)
```

### `load_nfl_pfr_weekly_rush(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_nfl_pfr_weekly_rush}

Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='week')`.

Will be removed in a future release. Migrate callers to the unified
`load_nfl_pfr_advstats` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `carries` | double | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards_before_contact` | double | Total rushing yards gained by the player before making contact with a defender in this weekly game log. |
| `rushing_yards_before_contact_avg` | double | Average rushing yards gained before first contact per rushing attempt in this weekly game log. |
| `rushing_yards_after_contact` | double | Total rushing yards gained by the player after initial contact with a defender in this weekly game log. |
| `rushing_yards_after_contact_avg` | double | Average rushing yards gained after initial contact per rushing attempt in this weekly game log. |
| `rushing_broken_tackles` | double | Number of broken tackles the player recorded on rushing plays in this weekly game log. |
| `receiving_broken_tackles` | double | Number of broken tackles the player recorded after a reception in this weekly game log. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="week"
)
```

### `load_nfl_player_stats(kicking=False, return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'` {#load_nfl_player_stats}

Load NFL player stats data

One combined week-level parquet (all seasons, offense) mirroring nflverse's
`player_stats`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which player-stats release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse published `player_stats.parquet`. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_player_stats` release built by `sportsdataverse.nfl.build_nfl_player_stats` from SDV-native play-by-play (1999-present, week-level, REG+POST). Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing player stats.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | character | Most recent team player appears in `pbp` with. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character | Abbreviation or name of the opposing team faced by the player in a given game or week. |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `interceptions` | double | The number of interceptions thrown. |
| `sacks` | double | The Number of times sacked. |
| `sack_yards` | double | Yards lost on sack plays. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | double | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | double | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | double | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `pacr` | double | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | double | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | double | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | double | The number of rushes with a lost fumble. |
| `rushing_first_downs` | double | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | double | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | double | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | double | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | double | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | double | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `racr` | double | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | double | "Player's share of team receiving targets in this game" |
| `air_yards_share` | double | Player's share of the team's air yards in this game |
| `wopr` | double | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | double | Total number of kick/punt return touchdowns |
| `fantasy_points` | double | Standard fantasy points. |
| `fantasy_points_ppr` | double | PPR fantasy points. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

# SDV-native player stats (week-level, built from SDV play-by-play)

stats_sdv = load_nfl_player_stats(source="sdv")
stats_sdv.select(["season", "week", "player_id", "attempts"]).head()

# Kicking-only stats (nflverse source only)

kicking = load_nfl_player_stats(kicking=True)

# Filter to a single season after load

import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

### `load_nfl_players(return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'` {#load_nfl_players}

Load the nflverse NFL player-identity master.

Reads nflverse's published `players.parquet` — a one-row-per-player
identity master that is the union of **seven** upstream systems (GSIS, ESPN,
NGS roster, Pro-Football-Reference, OverTheCap, PFF, and the Sleeper / Yahoo
cross-walk). It is the canonical source for cross-system identifier
columns (`gsis_id`, `espn_id`, `pfr_id`, `pff_id`, `otc_id`,
`smart_id`, `esb_id`, `nfl_id`) plus name, position, physical, draft,
and status fields.

This is the **full identity master**. For an SDV-native, public-source-only
alternative that does not depend on the nflverse release, see
`sportsdataverse.nfl.build_nfl_players` (ESPN-athletes tier only) and
`sportsdataverse.nfl.nfl_players_crosswalk` (a thin ID-only slice of
this same parquet).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |
| `source` | `str` | `'nflverse'` | Which player-master release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse seven-system `players.parquet` identity master described above. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_players` release built by `sportsdataverse.nfl.build_nfl_players` from the **public NFL Shield / ESPN-athletes** surface only. The SDV tier is a partial build: its columns are a subset of nflverse's and cross-system IDs are sparser (notably pre-2016), though `espn_id` is populated. The default stays `"nflverse"`. Any other value raises `ValueError`. |

**Returns**

One-row-per-player identity master. `return_as_pandas` narrows the return to a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gsis_id` | character | NFL Game Statistics & Information System player identifier, the canonical nflverse player key. |
| `display_name` | character | Player's full display name as published by nflverse. |
| `common_first_name` | character | Player's commonly used first name (the name they go by, which may differ from their legal first name). |
| `first_name` | character | Player's legal first name. |
| `last_name` | character | Player's last name. |
| `short_name` | character | Abbreviated name (typically first initial plus last name). |
| `football_name` | character | Player's preferred on-field name as used in broadcast and box-score contexts. |
| `suffix` | character | Generational or honorific name suffix (e.g., Jr., Sr., III), when present. |
| `esb_id` | character | Elias Sports Bureau player identifier. |
| `nfl_id` | character | NFL.com / Shield player identifier. |
| `pfr_id` | character | Pro-Football-Reference player identifier. |
| `pff_id` | character | Pro Football Focus player identifier. |
| `otc_id` | character | OverTheCap player identifier (salary-cap data source). |
| `espn_id` | character | ESPN athlete identifier. |
| `smart_id` | character | NFL SMART (Standard Media and Reference Table) globally unique player identifier. |
| `birth_date` | character | Player's date of birth (ISO YYYY-MM-DD). |
| `position_group` | character | Broad positional grouping the player belongs to (e.g., QB, RB, WR, DL). |
| `position` | character | Player's specific listed position abbreviation. |
| `ngs_position_group` | character | Positional grouping as classified by NFL Next Gen Stats. |
| `ngs_position` | character | Specific position as classified by NFL Next Gen Stats. |
| `height` | integer | Player's height in inches. |
| `weight` | integer | Player's listed weight in pounds. |
| `headshot` | character | URL to the player's official headshot image. |
| `college_name` | character | Name of the college the player attended. |
| `college_conference` | character | Athletic conference of the player's college. |
| `jersey_number` | character | Player's uniform / jersey number. |
| `rookie_season` | integer | Season (year) the player entered the league as a rookie. |
| `last_season` | integer | Most recent season (year) the player appeared on an NFL roster. |
| `latest_team` | character | Abbreviation of the most recent team the player was rostered on. |
| `status` | character | Player's current roster status (e.g., active, retired, free agent). |
| `ngs_status` | character | Player status as reported by NFL Next Gen Stats. |
| `ngs_status_short_description` | character | Short human-readable description of the NFL Next Gen Stats status. |
| `years_of_experience` | integer | Number of accrued NFL seasons of experience. |
| `pff_position` | character | Player's position as classified by Pro Football Focus. |
| `pff_status` | character | Player's status as classified by Pro Football Focus. |
| `draft_year` | integer | Year the player was selected in the NFL Draft (null if undrafted). |
| `draft_round` | integer | Round in which the player was drafted (null if undrafted). |
| `draft_pick` | integer | Overall pick number at which the player was drafted (null if undrafted). |
| `draft_team` | character | Abbreviation of the team that drafted the player (null if undrafted). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
print(players.shape)

# Pandas round-trip

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()

# SDV-native player master (public Shield/ESPN-athletes build; subset of nflverse columns, sparser cross-IDs)

players_sdv = load_nfl_players(source="sdv")
players_sdv.select(["display_name", "position", "espn_id"]).head()

# Pipeline next step (one line)

import polars as pl
load_nfl_players().select(["gsis_id", "display_name", "position"]).head()
```

### `load_nfl_schedule(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_schedule}

Load NFL schedule data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `gameday` | character | The date on which the game occurred. |
| `weekday` | character | The day of the week on which the game occcured. |
| `gametime` | character | The kickoff time of the game. This is represented in 24-hour time and the Eastern time zone, regardless of what time zone the game was being played in. |
| `away_team` | character | String abbreviation for the away team. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `overtime` | integer | Binary indicator of whether or not game went to overtime. |
| `old_game_id` | character | Legacy NFL game ID. |
| `gsis` | integer | The id of the game issued by the NFL Game Statistics & Information System. |
| `nfl_detail_id` | character | The id of the game issued by NFL Detail. |
| `pfr` | character | The id of the game issued by [Pro-Football-Reference](https://www.pro-football-reference.com/) |
| `pff` | integer | The id of the game issued by [Pro Football Focus](https://www.pff.com/) |
| `espn` | character | The id of the game issued by [ESPN](https://www.espn.com/) |
| `ftn` | integer | FTN Data game identifier corresponding to this scheduled game. |
| `away_rest` | integer | Days of rest that the away team is coming off of. |
| `home_rest` | integer | Days of rest that the home team is coming off of. |
| `away_moneyline` | integer | Odds for away team to win the game. |
| `home_moneyline` | integer | Odds for home team to win the game. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `away_spread_odds` | integer | Odds for away team to cover the spread. |
| `home_spread_odds` | integer | Odds for home team to cover the spread. |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `under_odds` | integer | Odds that total score of game would be under the total_line. |
| `over_odds` | integer | Odds that total score of game would be over the total_ine. |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `away_qb_id` | character | GSIS Player ID for away team starting quarterback. |
| `home_qb_id` | character | GSIS Player ID for home team starting quarterback. |
| `away_qb_name` | character | Name of away team starting QB. |
| `home_qb_name` | character | Name of home team starting QB. |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `referee` | character | Name of the game's referee (head official) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium` | character | Name of the stadium |

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

# Multi-season range

schedule = load_nfl_schedule(seasons=range(2020, 2025))

# Filter to a single week

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

# Pandas round-trip

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

### `load_nfl_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'` {#load_nfl_team_stats}

Load NFL team stats data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `summary_level` | `str` | `'week'` | Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week". Ignored when `source` is the SDV-native release (a single week-level parquet covering all seasons; filter post-load). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which team-stats release to read. `"nflverse"` (the default) reads the per-season nflverse `stats_team` releases. `"sportsdataverse"` / `"sdv"` reads the SDV-native `nfl_team_stats` release (a single combined week-level parquet, built by `sportsdataverse.nfl.build_nfl_team_stats` from the SDV play-by-play and filtered to the requested seasons post-load). |

**Returns**

Polars dataframe containing team stats available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character | Team abbreviation or identifier of the opposing team faced during the game or period. |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | integer | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `passing_interceptions` | integer | Total number of interceptions thrown by the team's passers during the game or season period. |
| `sacks_suffered` | integer | Total number of times the team's quarterback was sacked by the opposing defense during the period. |
| `sack_yards_lost` | integer | Total offensive yards lost by the team on plays where the quarterback was sacked during the period. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | integer | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | integer | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | integer | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | double | Completion percentage over expectation (CPOE) for the team's passing attack during the period, relative to a model-based baseline. Percentage points (100 * the completion-rate gap), not a 0-1 rate. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | integer | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | integer | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | integer | The number of rushes with a lost fumble. |
| `rushing_first_downs` | integer | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | integer | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | integer | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | integer | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | integer | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | integer | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | integer | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `special_teams_tds` | integer | Total number of kick/punt return touchdowns |
| `def_tackles_solo` | integer | Total number of solo tackles for this player |
| `def_tackles_with_assist` | integer | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | integer | Number of assisted tackles for this player |
| `def_tackles_for_loss` | integer | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | integer | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | integer | Number of times a fumble was forced from this player |
| `def_sacks` | double | Number of sacks form this player |
| `def_sack_yards` | double | Yards lost from sacks forced by this player |
| `def_qb_hits` | integer | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | integer | Number of interceptions forced by this player |
| `def_interception_yards` | integer | yards gained/lost by interception returns from this player |
| `def_pass_defended` | integer | Number of passes defended/broken up by this player |
| `def_tds` | integer | Number of defensive touchdowns scored by this player |
| `def_fumbles` | integer | Number of fumbles by this player |
| `def_safeties` | integer | Number of safeties scored by the defense (opponent tackled in their own end zone) during the period. |
| `misc_yards` | integer | Miscellaneous yards not attributed to passing, rushing, or standard return categories during the period. |
| `fumble_recovery_own` | integer | Number of fumbles recovered by the team that were originally fumbled by their own players. |
| `fumble_recovery_yards_own` | integer | Total yards gained (or lost) on recoveries of the team's own fumbles during the period. |
| `fumble_recovery_opp` | integer | Number of fumbles recovered by the team that were originally lost by the opposing team. |
| `fumble_recovery_yards_opp` | integer | Total yards gained on returns of fumbles recovered from the opposing team during the period. |
| `fumble_recovery_tds` | integer | Number of touchdowns scored by the team on fumble recoveries during the game or season period. |
| `penalties` | integer | Total number of penalties. |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer | Number of timeouts remaining or used by the team during the game or period. |
| `punt_returns` | integer | Number of punt returns. |
| `punt_return_yards` | integer | Team punt return yards. |
| `kickoff_returns` | integer | Total number of kickoff returns recorded by the team during the game or season period. |
| `kickoff_return_yards` | integer | Total yards gained by the team on kickoff returns during the game or season period. |
| `fg_made` | integer | TRUE when the field goal attempt was successful. |
| `fg_att` | integer | Total number of field goal attempts by the team's kicker during the game or season period. |
| `fg_missed` | integer | Total number of field goal attempts missed (not blocked) by the team's kicker during the period. |
| `fg_blocked` | integer | Total number of field goal attempts that were blocked by the opposing defense during the period. |
| `fg_long` | integer | Distance in yards of the longest successful field goal made by the team's kicker during the period. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg_made_0_19` | integer | Number of successful field goals made from 0–19 yards during the game or season period. |
| `fg_made_20_29` | integer | Number of successful field goals made from 20–29 yards during the game or season period. |
| `fg_made_30_39` | integer | Number of successful field goals made from 30–39 yards during the game or season period. |
| `fg_made_40_49` | integer | Number of successful field goals made from 40–49 yards during the game or season period. |
| `fg_made_50_59` | integer | Number of successful field goals made from 50–59 yards during the game or season period. |
| `fg_made_60_` | integer | Number of successful field goals made from 60 yards or longer during the game or season period. |
| `fg_missed_0_19` | integer | Number of field goal attempts from 0–19 yards that were missed (not blocked) during the period. |
| `fg_missed_20_29` | integer | Number of field goal attempts from 20–29 yards that were missed (not blocked) during the period. |
| `fg_missed_30_39` | integer | Number of field goal attempts from 30–39 yards that were missed (not blocked) during the period. |
| `fg_missed_40_49` | integer | Number of field goal attempts from 40–49 yards that were missed (not blocked) during the period. |
| `fg_missed_50_59` | integer | Number of field goal attempts from 50–59 yards that were missed (not blocked) during the period. |
| `fg_missed_60_` | integer | Number of field goal attempts from 60 yards or longer that were missed (not blocked) during the period. |
| `fg_made_list` | character | List of distances (in yards) of each successful field goal made during the game or season period. |
| `fg_missed_list` | character | List of distances (in yards) of each missed field goal attempt during the game or season period. |
| `fg_blocked_list` | character | List of distances (in yards) of each blocked field goal attempt during the game or season period. |
| `fg_made_distance` | integer | Cumulative distance in yards of all successful field goals made during the game or season period. |
| `fg_missed_distance` | integer | Cumulative distance in yards of all missed field goal attempts during the game or season period. |
| `fg_blocked_distance` | integer | Distance (in yards) of field goal attempts that were blocked by the opposing defense during the period. |
| `pat_made` | integer | Total number of successful point-after-touchdown kicks made by the team's kicker during the period. |
| `pat_att` | integer | Total number of point-after-touchdown (PAT / extra point) attempts by the team's kicker during the period. |
| `pat_missed` | integer | Number of point-after-touchdown attempts that were missed (not blocked) during the period. |
| `pat_blocked` | integer | Number of point-after-touchdown attempts that were blocked by the opposing defense during the period. |
| `pat_pct` | double | Percentage of point-after-touchdown attempts that were successfully converted during the period. |
| `gwfg_made` | integer | Number of successful game-winning field goals made to secure a victory in the final moments. |
| `gwfg_att` | integer | Number of game-winning field goal attempts made in the final moments to win the game. |
| `gwfg_missed` | integer | Number of game-winning field goal attempts that were missed (no good) in the final moments. |
| `gwfg_blocked` | integer | Number of game-winning field goal attempts that were blocked by the opposing defense. |
| `gwfg_distance` | integer | Distance in yards of the game-winning field goal attempt (or attempts) during the period. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])

# Regular-season-only team stats

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

# SDV-native team stats (built from SDV play-by-play)

sdv = load_nfl_team_stats(seasons=[2024], source="sdv")
```

### `load_nfl_teams(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_teams}

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_abbr` | character | Official team abbreveation |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_id` | integer | ESPN team id. |
| `team_nick` | character | Team nickname (e.g., 'Chiefs', 'Eagles', 'Patriots') without the city or state prefix. |
| `team_conf` | character | Conference affiliation of the team (e.g., 'AFC' or 'NFC'). |
| `team_division` | character | Division affiliation of the team (e.g., 'AFC North', 'NFC West'). |
| `team_color` | character | Primary team color; `team_detail = TRUE` only. |
| `team_color2` | character | Secondary brand color for the team in hexadecimal format (e.g., '#FFB612'). |
| `team_color3` | character | Tertiary brand color for the team in hexadecimal format, used in alternate uniforms or accents. |
| `team_color4` | character | Quaternary brand color for the team in hexadecimal format, part of the team's full brand palette. |
| `team_logo_wikipedia` | character | URL to the team's primary logo image as hosted on Wikimedia Commons / Wikipedia. |
| `team_logo_espn` | character | URL to the team's primary logo image as hosted by ESPN. |
| `team_wordmark` | character | URL to the team's wordmark image (team name rendered in official typography without the primary logo mark). |
| `team_conference_logo` | character | URL to the logo image for the team's conference (AFC or NFC). |
| `team_league_logo` | character | URL to the NFL league logo image. |
| `team_logo_squared` | character | URL to a square-cropped version of the team's logo suitable for thumbnails and grid layouts. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### `load_nfl_trades(return_as_pandas=False) -> 'pl.DataFrame'` {#load_nfl_trades}

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

| col_name | type | description |
|---|---|---|
| `trade_id` | integer | ID of Trade |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `trade_date` | character | Exact date that trade occurred |
| `gave` | character | Team that gave pick/player in row |
| `received` | character | Team that received pick/player in row |
| `pick_season` | integer | Draft in which traded pick was in |
| `pick_round` | integer | Round in which traded pick was in |
| `pick_number` | integer | Pick number of traded pick |
| `conditional` | integer | Binary indicator of whether or not traded pick was conditional |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pfr_name` | character | Full name of traded player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

# Filter to a single season

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

### `load_officials(return_as_pandas=False) -> 'pl.DataFrame'` {#load_officials}

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `game_key` | character | Unique numeric key assigned by the NFL to identify the specific game in official records. |
| `official_name` | character | Official name. |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `official_id` | character | Unique official / referee identifier. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

# Pandas round-trip

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### `load_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_participation}

Load NFL play-by-play participation data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2016 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing play-by-play participation data available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `nflverse_game_id` | character | nflverse identifier for games. Format is season, week, away_team, home_team |
| `old_game_id` | character | Legacy NFL game ID. |
| `play_id` | double | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `possession_team` | character | String abbreviation for the team with possession. |
| `offense_formation` | character | Formation the offense lines up in to snap the ball. |
| `offense_personnel` | character | The positions of the offensive personnel lined up on the field for a play. |
| `defenders_in_box` | integer | Number of defensive players lined up in the box at the snap. |
| `defense_personnel` | character | The positions of the defensive personnel lined up on the field for a play. |
| `number_of_pass_rushers` | integer | Number of defensive player who rushed the passer. |
| `players_on_play` | character | A list of every player on the field for the play, by gsis_id |
| `offense_players` | character | A list of every offensive player on the field for the play, by gsis_id |
| `defense_players` | character | A list of every defensive player on the field for the play, by gsis_id |
| `n_offense` | integer | Number of offensive players on the field for the play |
| `n_defense` | integer | Number of defensive players on the field for the play |
| `ngs_air_yards` | double | Legacy column. For 2023 and prior years, reflects the distance (in yards) that the ball traveled in the air on a given passing play as tracked by NGS. Is NA for 2024 on--we advise instead using the air_yards column from nflreadr::load_pbp() moving forward. |
| `time_to_throw` | double | Duration (in seconds) between the time of the ball being snapped and the time of release of a pass attempt |
| `was_pressure` | logical | A boolean indicating whether or not the QB was pressured on a play |
| `route` | character | A string indicating the route the primary receiver on a play took. Has the following possible values: "CORNER", "DEEP OUT", "GO", "HITCH/CURL", "IN/DIG", "POST", "QUICK OUT", "SCREEN", "SHALLOW CROSS/DRAG", "SLANT", "SWING", "TEXAS/ANGLE", "WHEEL". |
| `defense_man_zone_type` | character | A string indicating whether the defense was in man or zone coverage on a play |
| `defense_coverage_type` | character | A string indicating what type of cover the defense was in on a play. Has one of the following values: "COVER_0", "COVER_1", "COVER_2", "2_MAN", "COVER_3", "COVER_4", "COVER_6", "COVER_9", "COMBO", "BLOWN". |
| `offense_names` | character | A string listing all of the names of offensive players in the order of their gsis_ids in offense_players. |
| `defense_names` | character | A string listing all of the names of defensive players in the order of their gsis_ids in defense_players. |
| `offense_positions` | character | A string listing all of the positions of offensive players in the order of their gsis_ids in offense_players. |
| `defense_positions` | character | A string listing all of the positions of defensive players in the order of their gsis_ids in defense_players. |
| `offense_numbers` | character | A string listing all of the numbers of offensive players in the order of their gsis_ids in offense_players. |
| `defense_numbers` | character | A string listing all of the numbers of defensive players in the order of their gsis_ids in defense_players. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

# Multi-season range

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

### `load_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_pfr_advstats}

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level
PFR advstats accessors. Mirrors the API surface of nflreadpy's
`load_pfr_advstats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For `summary_level='week'` this drives the per-season parquet fan-out; for `summary_level='season'` it post-filters the combined parquet by the `season` column. |
| `stat_type` | `str` | `'pass'` | One of `"pass"`, `"rush"`, `"rec"`, `"def"`. Defaults to `"pass"`. |
| `summary_level` | `str` | `'week'` | One of `"week"` or `"season"`. Defaults to `"week"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested `stat_type`, `summary_level`, and `seasons`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `passing_drops` | double | Raw count of catchable passes dropped by the intended receiver, from Pro Football Reference charting. |
| `passing_drop_pct` | double | Percentage of pass attempts dropped by the receiver, isolating receiver-side incompletions from passer error. |
| `receiving_drop` | double | Number of catchable targets dropped by the receiver in the given game or season, from Pro Football Reference advanced receiving data. |
| `receiving_drop_pct` | double | Percentage of targets that resulted in a drop by the receiver, from Pro Football Reference advanced receiving data. |
| `passing_bad_throws` | double | Raw count of bad throws by the passer, as charted and defined by Pro Football Reference advanced passing data. |
| `passing_bad_throw_pct` | double | Percentage of pass attempts classified as bad throws by Pro Football Reference (passes the passer should not have attempted or severely underthreww/overthrew). |
| `times_sacked` | double | Total number of times the defensive player recorded a sack of the quarterback, from Pro Football Reference. |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double | Percentage of pass-blocking snaps on which the lineman or back allowed the quarterback to be pressured, from Pro Football Reference. |
| `def_times_blitzed` | double | Number of plays on which the defensive player sent five or more pass rushers, from Pro Football Reference advanced defensive stats. |
| `def_times_hurried` | double | Number of times the defensive player hurried (pressured but did not sack or hit) the quarterback, from Pro Football Reference. |
| `def_times_hitqb` | double | Number of times the defensive player hit the quarterback on a pass play without recording a sack, from Pro Football Reference. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)

# Season-level rushing summaries (one row per player per season)

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

# Defensive stats with a follow-up filter

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

# Pandas round-trip

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### `load_player_stats(kicking=False, return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'` {#load_player_stats}

Load NFL player stats data

One combined week-level parquet (all seasons, offense) mirroring nflverse's
`player_stats`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which player-stats release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse published `player_stats.parquet`. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_player_stats` release built by `sportsdataverse.nfl.build_nfl_player_stats` from SDV-native play-by-play (1999-present, week-level, REG+POST). Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing player stats.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | character | Most recent team player appears in `pbp` with. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character | Abbreviation of the opposing team the player faced in the game or week represented by this row. |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `interceptions` | double | The number of interceptions thrown. |
| `sacks` | double | The Number of times sacked. |
| `sack_yards` | double | Yards lost on sack plays. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | double | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | double | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | double | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `pacr` | double | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | double | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | double | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | double | The number of rushes with a lost fumble. |
| `rushing_first_downs` | double | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | double | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | double | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | double | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | double | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | double | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `racr` | double | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | double | "Player's share of team receiving targets in this game" |
| `air_yards_share` | double | Player's share of the team's air yards in this game |
| `wopr` | double | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | double | Total number of kick/punt return touchdowns |
| `fantasy_points` | double | Standard fantasy points. |
| `fantasy_points_ppr` | double | PPR fantasy points. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

# SDV-native player stats (week-level, built from SDV play-by-play)

stats_sdv = load_nfl_player_stats(source="sdv")
stats_sdv.select(["season", "week", "player_id", "attempts"]).head()

# Kicking-only stats (nflverse source only)

kicking = load_nfl_player_stats(kicking=True)

# Filter to a single season after load

import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

### `load_players(return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'` {#load_players}

Load the nflverse NFL player-identity master.

Reads nflverse's published `players.parquet` — a one-row-per-player
identity master that is the union of **seven** upstream systems (GSIS, ESPN,
NGS roster, Pro-Football-Reference, OverTheCap, PFF, and the Sleeper / Yahoo
cross-walk). It is the canonical source for cross-system identifier
columns (`gsis_id`, `espn_id`, `pfr_id`, `pff_id`, `otc_id`,
`smart_id`, `esb_id`, `nfl_id`) plus name, position, physical, draft,
and status fields.

This is the **full identity master**. For an SDV-native, public-source-only
alternative that does not depend on the nflverse release, see
`sportsdataverse.nfl.build_nfl_players` (ESPN-athletes tier only) and
`sportsdataverse.nfl.nfl_players_crosswalk` (a thin ID-only slice of
this same parquet).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |
| `source` | `str` | `'nflverse'` | Which player-master release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse seven-system `players.parquet` identity master described above. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_players` release built by `sportsdataverse.nfl.build_nfl_players` from the **public NFL Shield / ESPN-athletes** surface only. The SDV tier is a partial build: its columns are a subset of nflverse's and cross-system IDs are sparser (notably pre-2016), though `espn_id` is populated. The default stays `"nflverse"`. Any other value raises `ValueError`. |

**Returns**

One-row-per-player identity master. `return_as_pandas` narrows the return to a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gsis_id` | character | NFL Game Statistics & Information System player identifier, the canonical nflverse player key. |
| `display_name` | character | Player's full display name as published by nflverse. |
| `common_first_name` | character | Player's commonly used first name (the name they go by, which may differ from their legal first name). |
| `first_name` | character | Player's legal first name. |
| `last_name` | character | Player's last name. |
| `short_name` | character | Abbreviated name (typically first initial plus last name). |
| `football_name` | character | Player's preferred on-field name as used in broadcast and box-score contexts. |
| `suffix` | character | Generational or honorific name suffix (e.g., Jr., Sr., III), when present. |
| `esb_id` | character | Elias Sports Bureau player identifier. |
| `nfl_id` | character | NFL.com / Shield player identifier. |
| `pfr_id` | character | Pro-Football-Reference player identifier. |
| `pff_id` | character | Pro Football Focus player identifier. |
| `otc_id` | character | OverTheCap player identifier (salary-cap data source). |
| `espn_id` | character | ESPN athlete identifier. |
| `smart_id` | character | NFL SMART (Standard Media and Reference Table) globally unique player identifier. |
| `birth_date` | character | Player's date of birth (ISO YYYY-MM-DD). |
| `position_group` | character | Broad positional grouping the player belongs to (e.g., QB, RB, WR, DL). |
| `position` | character | Player's specific listed position abbreviation. |
| `ngs_position_group` | character | Positional grouping as classified by NFL Next Gen Stats. |
| `ngs_position` | character | Specific position as classified by NFL Next Gen Stats. |
| `height` | integer | Player's height in inches. |
| `weight` | integer | Player's listed weight in pounds. |
| `headshot` | character | URL to the player's official headshot image. |
| `college_name` | character | Name of the college the player attended. |
| `college_conference` | character | Athletic conference of the player's college. |
| `jersey_number` | character | Player's uniform / jersey number. |
| `rookie_season` | integer | Season (year) the player entered the league as a rookie. |
| `last_season` | integer | Most recent season (year) the player appeared on an NFL roster. |
| `latest_team` | character | Abbreviation of the most recent team the player was rostered on. |
| `status` | character | Player's current roster status (e.g., active, retired, free agent). |
| `ngs_status` | character | Player status as reported by NFL Next Gen Stats. |
| `ngs_status_short_description` | character | Short human-readable description of the NFL Next Gen Stats status. |
| `years_of_experience` | integer | Number of accrued NFL seasons of experience. |
| `pff_position` | character | Player's position as classified by Pro Football Focus. |
| `pff_status` | character | Player's status as classified by Pro Football Focus. |
| `draft_year` | integer | Year the player was selected in the NFL Draft (null if undrafted). |
| `draft_round` | integer | Round in which the player was drafted (null if undrafted). |
| `draft_pick` | integer | Overall pick number at which the player was drafted (null if undrafted). |
| `draft_team` | character | Abbreviation of the team that drafted the player (null if undrafted). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
print(players.shape)

# Pandas round-trip

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()

# SDV-native player master (public Shield/ESPN-athletes build; subset of nflverse columns, sparser cross-IDs)

players_sdv = load_nfl_players(source="sdv")
players_sdv.select(["display_name", "position", "espn_id"]).head()

# Pipeline next step (one line)

import polars as pl
load_nfl_players().select(["gsis_id", "display_name", "position"]).head()
```

### `load_rosters_weekly(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_rosters_weekly}

Load NFL weekly roster data for the requested seasons.

Reads nflverse's published weekly-roster parquet (one row per player per
team per week), so the roster snapshot reflects mid-season transactions
(signings, releases, IR moves) rather than a single season-end view. Like
`load_nfl_rosters` it is sourced from nflverse's full multi-tier
roster product and carries densely populated cross-system identifier columns
plus a `week` / `game_type` pair identifying each snapshot.

Unlike `load_nfl_rosters` and `load_nfl_players`, this loader has
**no SDV-native (`source="sdv"`) tier**: the SDV roster build
(`build_nfl_rosters`) is season-only, and weekly snapshots require the
credential-gated NFL Data Exchange that the public build cannot reach.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Seasons to load (e.g. `[2024]` or `range(2022, 2025)`). A single `int` is accepted and wrapped. 2002 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe (default). |

**Returns**

Polars dataframe of weekly rosters for the requested seasons (`pandas.DataFrame` when `return_as_pandas=True`).

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (year) the weekly roster snapshot applies to. |
| `team` | character | Team abbreviation in the nflverse standard (relocations folded, e.g. 'OAK' -> 'LV', 'SD' -> 'LAC', 'STL' -> 'LA'). |
| `position` | character | Position the player is listed at on the roster (e.g. 'QB', 'WR', 'CB'). |
| `depth_chart_position` | character | Fine-grained depth-chart position label, which may differ from the broader position group. |
| `jersey_number` | integer | Uniform (jersey) number the player wears. |
| `status` | character | Roster status code for the player (e.g. 'ACT' active, 'INA' inactive, 'RES' reserve/injured). |
| `full_name` | character | Player's full display name. |
| `first_name` | character | Player's first (given) name. |
| `last_name` | character | Player's last (family) name. |
| `birth_date` | character | Player's date of birth (YYYY-MM-DD). |
| `height` | double | Player's height in inches. |
| `weight` | integer | Player's listed weight in pounds. |
| `college` | character | College or university the player attended. |
| `gsis_id` | character | NFL GSIS player identifier — the canonical nflverse player key used to join across datasets. |
| `espn_id` | character | ESPN player identifier for cross-system joins. |
| `sportradar_id` | character | Sportradar player identifier for cross-system joins. |
| `yahoo_id` | character | Yahoo Sports player identifier for cross-system joins. |
| `rotowire_id` | character | RotoWire player identifier for cross-system joins. |
| `pff_id` | character | Pro Football Focus (PFF) player identifier for cross-system joins. |
| `pfr_id` | character | Pro Football Reference (PFR) player identifier for cross-system joins. |
| `fantasy_data_id` | character | FantasyData player identifier for cross-system joins. |
| `sleeper_id` | character | Sleeper player identifier for cross-system joins. |
| `years_exp` | integer | Number of accrued NFL seasons of experience for the player. |
| `headshot_url` | character | URL of the player's headshot image. |
| `ngs_position` | character | Player's position as classified by NFL Next Gen Stats. |
| `week` | integer | Week of the season the weekly roster snapshot applies to. |
| `game_type` | character | Type of game the weekly roster snapshot applies to (e.g. 'REG', 'POST'). |
| `status_description_abbr` | character | Abbreviated roster status description code from the source feed. |
| `football_name` | character | Player's preferred football (commonly used) first name. |
| `esb_id` | character | Elias Sports Bureau (ESB) player identifier used for official NFL record-keeping. |
| `gsis_it_id` | character | NFL GSIS internal tracking identifier for the player. |
| `smart_id` | character | NFL SMART player identifier (GUID) used across modern NFL data feeds. |
| `entry_year` | integer | Calendar year the player first entered the NFL. |
| `rookie_year` | integer | Calendar year of the player's rookie season. |
| `draft_club` | character | Team abbreviation of the club that drafted the player. |
| `draft_number` | integer | Overall pick number at which the player was selected in the NFL draft. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])

# Multi-season range with a follow-up week filter

import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

### `load_schedules(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_schedules}

Load NFL schedule data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `gameday` | character | The date on which the game occurred. |
| `weekday` | character | The day of the week on which the game occcured. |
| `gametime` | character | The kickoff time of the game. This is represented in 24-hour time and the Eastern time zone, regardless of what time zone the game was being played in. |
| `away_team` | character | String abbreviation for the away team. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `overtime` | integer | Binary indicator of whether or not game went to overtime. |
| `old_game_id` | character | Legacy NFL game ID. |
| `gsis` | integer | The id of the game issued by the NFL Game Statistics & Information System. |
| `nfl_detail_id` | character | The id of the game issued by NFL Detail. |
| `pfr` | character | The id of the game issued by [Pro-Football-Reference](https://www.pro-football-reference.com/) |
| `pff` | integer | The id of the game issued by [Pro Football Focus](https://www.pff.com/) |
| `espn` | character | The id of the game issued by [ESPN](https://www.espn.com/) |
| `ftn` | integer | FTN Data game identifier used to join schedule records with FTN charting and tracking data. |
| `away_rest` | integer | Days of rest that the away team is coming off of. |
| `home_rest` | integer | Days of rest that the home team is coming off of. |
| `away_moneyline` | integer | Odds for away team to win the game. |
| `home_moneyline` | integer | Odds for home team to win the game. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `away_spread_odds` | integer | Odds for away team to cover the spread. |
| `home_spread_odds` | integer | Odds for home team to cover the spread. |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `under_odds` | integer | Odds that total score of game would be under the total_line. |
| `over_odds` | integer | Odds that total score of game would be over the total_ine. |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `away_qb_id` | character | GSIS Player ID for away team starting quarterback. |
| `home_qb_id` | character | GSIS Player ID for home team starting quarterback. |
| `away_qb_name` | character | Name of away team starting QB. |
| `home_qb_name` | character | Name of home team starting QB. |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `referee` | character | Name of the game's referee (head official) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium` | character | Name of the stadium |

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

# Multi-season range

schedule = load_nfl_schedule(seasons=range(2020, 2025))

# Filter to a single week

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

# Pandas round-trip

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

### `load_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'` {#load_snap_counts}

Load NFL snap counts data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2012 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing snap counts available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `week` | integer | Season week. |
| `player` | character | Player name |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `offense_snaps` | double | Number of snaps on offense |
| `offense_pct` | double | Percent of offensive snaps taken |
| `defense_snaps` | double | Number of snaps on defense |
| `defense_pct` | double | Percent of defensive snaps taken |
| `st_snaps` | double | Number of snaps on special teams |
| `st_pct` | double | Percent of special teams snaps taken |

**Example**

```python
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])

# Multi-season range with offense-only filter

import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```

### `load_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'` {#load_team_stats}

Load NFL team stats data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `summary_level` | `str` | `'week'` | Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week". Ignored when `source` is the SDV-native release (a single week-level parquet covering all seasons; filter post-load). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which team-stats release to read. `"nflverse"` (the default) reads the per-season nflverse `stats_team` releases. `"sportsdataverse"` / `"sdv"` reads the SDV-native `nfl_team_stats` release (a single combined week-level parquet, built by `sportsdataverse.nfl.build_nfl_team_stats` from the SDV play-by-play and filtered to the requested seasons post-load). |

**Returns**

Polars dataframe containing team stats available for the requested seasons.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character | Abbreviation of the opposing team the team faced in the game or week represented by this row. |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | integer | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `passing_interceptions` | integer | Total number of interceptions thrown by the team's quarterbacks during the period covered. |
| `sacks_suffered` | integer | Total number of times the team's quarterback was sacked during the period covered. |
| `sack_yards_lost` | integer | Total yards lost by the team's offense as a result of being sacked. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | integer | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | integer | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | integer | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | double | Completion percentage over expectation for the team's passing game — how much better or worse actual completion rate was versus the model-predicted rate. Percentage points (100 * the completion-rate gap), not a 0-1 rate. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | integer | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | integer | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | integer | The number of rushes with a lost fumble. |
| `rushing_first_downs` | integer | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | integer | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | integer | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | integer | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | integer | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | integer | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | integer | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `special_teams_tds` | integer | Total number of kick/punt return touchdowns |
| `def_tackles_solo` | integer | Total number of solo tackles for this player |
| `def_tackles_with_assist` | integer | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | integer | Number of assisted tackles for this player |
| `def_tackles_for_loss` | integer | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | integer | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | integer | Number of times a fumble was forced from this player |
| `def_sacks` | double | Number of sacks form this player |
| `def_sack_yards` | double | Yards lost from sacks forced by this player |
| `def_qb_hits` | integer | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | integer | Number of interceptions forced by this player |
| `def_interception_yards` | integer | yards gained/lost by interception returns from this player |
| `def_pass_defended` | integer | Number of passes defended/broken up by this player |
| `def_tds` | integer | Number of defensive touchdowns scored by this player |
| `def_fumbles` | integer | Number of fumbles by this player |
| `def_safeties` | integer | Number of safeties recorded by the team's defense (tackling an opponent in their own end zone). |
| `misc_yards` | integer | Yards gained by the team through miscellaneous means not captured in standard rushing, passing, or return categories. |
| `fumble_recovery_own` | integer | Number of the team's own fumbles that were recovered by the team itself. |
| `fumble_recovery_yards_own` | integer | Total yards gained after recovering their own fumbles. |
| `fumble_recovery_opp` | integer | Number of fumbles recovered by the team from the opposing offense (defensive fumble recoveries). |
| `fumble_recovery_yards_opp` | integer | Total yards gained by the team on returns of opponent fumble recoveries. |
| `fumble_recovery_tds` | integer | Number of touchdowns scored by the team on fumble recoveries (own or opponent). |
| `penalties` | integer | Total number of penalties. |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer | Number of timeouts remaining or used by the team during the game or period covered. |
| `punt_returns` | integer | Number of punt returns. |
| `punt_return_yards` | integer | Team punt return yards. |
| `kickoff_returns` | integer | Total number of kickoff return attempts by the team. |
| `kickoff_return_yards` | integer | Total yards gained by the team on kickoff returns during the period covered. |
| `fg_made` | integer | TRUE when the field goal attempt was successful. |
| `fg_att` | integer | Total field goal attempts by the team's kicker during the period covered. |
| `fg_missed` | integer | Total number of field goal attempts that were missed (not blocked, not made) by the team's kicker. |
| `fg_blocked` | integer | Total number of field goal attempts that were blocked by the opposing defense. |
| `fg_long` | integer | Distance in yards of the team's longest successful field goal during the period covered. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg_made_0_19` | integer | Number of field goals made by the team from 0–19 yards. |
| `fg_made_20_29` | integer | Number of field goals made by the team from 20–29 yards. |
| `fg_made_30_39` | integer | Number of field goals made by the team from 30–39 yards. |
| `fg_made_40_49` | integer | Number of field goals made by the team from 40–49 yards. |
| `fg_made_50_59` | integer | Number of field goals made by the team from 50–59 yards. |
| `fg_made_60_` | integer | Number of field goals made by the team from 60 yards or longer. |
| `fg_missed_0_19` | integer | Number of field goal attempts missed from 0–19 yards. |
| `fg_missed_20_29` | integer | Number of field goal attempts missed from 20–29 yards. |
| `fg_missed_30_39` | integer | Number of field goal attempts missed from 30–39 yards. |
| `fg_missed_40_49` | integer | Number of field goal attempts missed from 40–49 yards. |
| `fg_missed_50_59` | integer | Number of field goal attempts missed from 50–59 yards. |
| `fg_missed_60_` | integer | Number of field goal attempts missed from 60 yards or longer. |
| `fg_made_list` | character | Comma-separated list of distances (in yards) for each successful field goal made by the team. |
| `fg_missed_list` | character | Comma-separated list of distances (in yards) for each missed field goal attempt by the team. |
| `fg_blocked_list` | character | Comma-separated list of distances (in yards) for field goal attempts blocked by or against the team. |
| `fg_made_distance` | integer | Total cumulative distance in yards of all successful field goals made by the team. |
| `fg_missed_distance` | integer | Total cumulative distance in yards of all missed field goal attempts by the team. |
| `fg_blocked_distance` | integer | Distance in yards of the most recent or representative blocked field goal attempt. |
| `pat_made` | integer | Total number of extra points successfully kicked by the team. |
| `pat_att` | integer | Total number of extra point (PAT) kick attempts by the team. |
| `pat_missed` | integer | Number of extra point kick attempts that were missed (neither made nor blocked). |
| `pat_blocked` | integer | Number of extra point attempts that were blocked by the opposing defense. |
| `pat_pct` | double | Extra point conversion percentage (pat_made divided by pat_att) for the team's kicker. |
| `gwfg_made` | integer | Number of game-winning field goals successfully converted by the team's kicker. |
| `gwfg_att` | integer | Number of game-winning field goal attempts (potential go-ahead kicks in the final moments). |
| `gwfg_missed` | integer | Number of game-winning field goal attempts that were missed by the team's kicker. |
| `gwfg_blocked` | integer | Number of game-winning field goal attempts that were blocked by the opposing defense. |
| `gwfg_distance` | integer | Distance in yards of the game-winning field goal attempt(s) during the period covered. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])

# Regular-season-only team stats

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

# SDV-native team stats (built from SDV play-by-play)

sdv = load_nfl_team_stats(seasons=[2024], source="sdv")
```

### `load_teams(return_as_pandas=False) -> 'pl.DataFrame'` {#load_teams}

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

| col_name | type | description |
|---|---|---|
| `team_abbr` | character | Official team abbreveation |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_id` | integer | ESPN team id. |
| `team_nick` | character | Team nickname or mascot name (e.g., 'Chiefs', 'Patriots'). |
| `team_conf` | character | Conference the team belongs to (e.g., 'AFC', 'NFC'). |
| `team_division` | character | Division within the conference the team belongs to (e.g., 'AFC East'). |
| `team_color` | character | Primary team color; `team_detail = TRUE` only. |
| `team_color2` | character | Secondary brand color for the team, expressed as a hex color code. |
| `team_color3` | character | Tertiary brand color for the team, expressed as a hex color code. |
| `team_color4` | character | Quaternary brand color for the team, expressed as a hex color code. |
| `team_logo_wikipedia` | character | URL of the team's logo image as hosted on Wikipedia. |
| `team_logo_espn` | character | URL of the team's primary logo as hosted on ESPN. |
| `team_wordmark` | character | URL of the team's wordmark (text-based logo) image. |
| `team_conference_logo` | character | URL of the conference logo image associated with the team. |
| `team_league_logo` | character | URL of the NFL league logo image. |
| `team_logo_squared` | character | URL of a square-format version of the team's logo. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### `load_trades(return_as_pandas=False) -> 'pl.DataFrame'` {#load_trades}

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

| col_name | type | description |
|---|---|---|
| `trade_id` | integer | ID of Trade |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `trade_date` | character | Exact date that trade occurred |
| `gave` | character | Team that gave pick/player in row |
| `received` | character | Team that received pick/player in row |
| `pick_season` | integer | Draft in which traded pick was in |
| `pick_round` | integer | Round in which traded pick was in |
| `pick_number` | integer | Pick number of traded pick |
| `conditional` | integer | Binary indicator of whether or not traded pick was conditional |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `pfr_name` | character | Full name of traded player |

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

# Filter to a single season

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

## Utilities & helpers

### `NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, **kwargs)` {#NFLPlayProcess}

Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN `summary` endpoint (or a local JSON dump) and pipes the
result through a chain of feature-engineering steps -- down/distance,
play-type flags, EPA, WPA, QBR, drive aggregation, and an advanced
box score. Use `run_processing_pipeline()` for the full feature set
or `run_cleaning_pipeline()` for a lighter clean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` | `int` | `0` | ESPN `event` id (e.g. `401671801`). |
| `raw` | `bool` | `False` | If `True`, `espn_nfl_pbp()` returns the ESPN payload untouched. If `False` (default), it normalizes keys. |
| `path_to_json` | `str` | `'/'` | Directory containing `{gameId}.json` for the `nfl_pbp_disk()` flow (offline replay). |
| `return_keys` | `list[str] \| None` | `None` | If supplied, `run_processing_pipeline` returns only the listed keys from the result dict. |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])

# Offline replay from a JSON dump

proc = NFLPlayProcess(gameId=401671801, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
cleaned = proc.run_cleaning_pipeline()

# Subset the return payload

proc = NFLPlayProcess(gameId=401671801, return_keys=["plays", "boxscore"])
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())  # ['boxscore', 'plays']
```

**Methods**

#### `NFLPlayProcess.corrupt_pbp_check()`

Detect ESPN payloads that look corrupt or partial.

Returns `True` when one of three guard conditions trips:

* No plays at all.
* Fewer than 50 plays for a game ESPN reports as completed.
* More than 500 plays for a game ESPN reports as completed.

`run_processing_pipeline()` and `run_cleaning_pipeline()` use
this to skip feature engineering on obviously broken payloads.

**Returns**

`True` if the payload looks corrupt; `False` otherwise.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
if not proc.corrupt_pbp_check():
    result = proc.run_processing_pipeline()
```

#### `NFLPlayProcess.create_box_score(play_df)`

Build the advanced box score (passer / rusher / receiver / team / situational / defensive / turnover / drives)

from a feature-engineered plays DataFrame.

This is normally called by `run_processing_pipeline()` -- it
auto-runs the pipeline first if it hasn't been triggered yet, so
callers can also invoke it directly on a freshly-instantiated
processor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | The plays frame produced after the full feature-engineering chain (downs, play-type flags, EPA, WPA, drive aggregation). |

**Returns**

Box score keyed by `"pass"`, `"rush"`, `"receiver"`, `"team"`, `"situational"`, `"defensive"`, `"turnover"`, `"drives"` -- each value a list of dicts ready to be serialized.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
box = result["advBoxScore"]
sorted(box.keys())
```

#### `NFLPlayProcess.espn_nfl_pbp(**kwargs)`

espn_nfl_pbp() - Pull the game by id. Data from API endpoints: `nfl/playbyplay`, `nfl/summary`

**Returns**

Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder", "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
payload = proc.espn_nfl_pbp()
sorted(payload.keys())[:5]

# Raw ESPN passthrough (no key normalization)

proc_raw = NFLPlayProcess(gameId=401220403, raw=True)
espn_dump = proc_raw.espn_nfl_pbp()

# Chain into the full processing pipeline

proc = NFLPlayProcess(gameId=401220403)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
```

#### `NFLPlayProcess.nfl_pbp_disk()`

Load a previously-saved ESPN payload from `{path_to_json}/{gameId}.json`.

Use this to replay an old game offline without hitting the ESPN
endpoint -- handy for snapshot-driven tests and reproducible
feature engineering.

**Returns**

The parsed JSON content; also stored on `self.json`.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
result = proc.run_processing_pipeline()
```

#### `NFLPlayProcess.nfl_pbp_json(**kwargs)`

Set `self.json` to the imported `json` module reference (legacy stub).

Retained for API compatibility. Prefer `espn_nfl_pbp()` (live)
or `nfl_pbp_disk()` (offline) to populate `self.json` with an
actual ESPN payload.

**Returns**

The Python `json` module reference (mirrors legacy behavior).

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
proc.nfl_pbp_json()  # populates `self.json` with the json module
```

#### `NFLPlayProcess.run_cleaning_pipeline()`

Run the lighter cleaning pipeline against `self.json`.

Identical to `run_processing_pipeline()` up through the
add_spread_time` step but stops short of EPA / WPA / QBR /
drive aggregation and the advanced box score. Use this when you
want clean play structure without the modeled features.

**Returns**

The cleaned game dict (or the subset specified by `return_keys` at construction).

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
cleaned = proc.run_cleaning_pipeline()
"plays" in cleaned and "advBoxScore" not in cleaned
```

#### `NFLPlayProcess.run_processing_pipeline()`

Run the full feature-engineering pipeline against `self.json`.

Pipes the plays frame through the chain of helpers: downs,
play-type flags, rush/pass flags, team-score variables, new play
types, penalties, play-category flags, yardage cols, player cols,
post-play cols, spread time, EPA, WPA, drive data, and QBR --
followed by the advanced box score build.

**Returns**

Dict | None: The full processed game dict (or the subset specified by `return_keys` at construction). Returns the partial result when `corrupt_pbp_check()` short-circuits.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"]), len(result["drives"])

# Subset returned keys for downstream serialization

proc = NFLPlayProcess(
    gameId=401671801,
    return_keys=["plays", "advBoxScore", "winprobability"],
)
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())
```

### `get_current_nfl_season(roster: 'bool' = False) -> 'int'` {#get_current_nfl_season}

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

# Roster-year semantics (March 15 cutover)

roster_year = get_current_nfl_season(roster=True)

# Pair with a loader to fetch only the active season

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### `get_current_nfl_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'` {#get_current_nfl_week}

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

# Schedule-driven week (hits the live schedule parquet)

week_live = get_current_nfl_week(use_date=False)

# Roster-year season inference

week_roster = get_current_nfl_week(roster=True)

# Pair with a PBP fetch to grab only the most recent season+week

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### `get_current_season(roster: 'bool' = False) -> 'int'` {#get_current_season}

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

# Roster-year semantics (March 15 cutover)

roster_year = get_current_nfl_season(roster=True)

# Pair with a loader to fetch only the active season

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### `get_current_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'` {#get_current_week}

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

# Schedule-driven week (hits the live schedule parquet)

week_live = get_current_nfl_week(use_date=False)

# Roster-year season inference

week_roster = get_current_nfl_week(roster=True)

# Pair with a PBP fetch to grab only the most recent season+week

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### `most_recent_nfl_season(roster: 'bool' = False) -> 'int'` {#most_recent_nfl_season}

Alias for `get_current_nfl_season()` mirroring nflreadr's

`most_recent_season()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl.utils_date import most_recent_nfl_season
season = most_recent_nfl_season()

# Roster-year flavor

roster_year = most_recent_nfl_season(roster=True)
```

## Other

### `NflConfig(cache_mode: 'CacheMode' = 'memory', cache_dir: 'Optional[Path]' = None, cache_duration: 'int' = 86400, verbose: 'bool' = True, timeout: 'int' = 30, user_agent: 'str' = 'sportsdataverse-py-nfl') -> None` {#NflConfig}

Runtime configuration for sdv-py NFL loaders.

Fields mirror nflreadpy's `NflreadpyConfig` so users can swap engines
without changing call sites. The defaults are conservative: in-memory
caching with a 24-hour TTL, verbose progress bars on, 30-second
HTTP timeout.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache_mode` | `CacheMode` | `'memory'` |  |
| `cache_dir` | `Optional[Path]` | `None` |  |
| `cache_duration` | `int` | `86400` |  |
| `verbose` | `bool` | `True` |  |
| `timeout` | `int` | `30` |  |
| `user_agent` | `str` | `'sportsdataverse-py-nfl'` |  |

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()  # NflConfig instance
cfg.cache_mode      # "memory"
cfg.cache_duration  # 86400 (24h)
cfg.timeout         # 30 (seconds)

# Construct a fresh instance directly (rarely needed -- prefer ``update_config``)

from sportsdataverse.nfl import NflConfig
cfg = NflConfig(cache_mode="off", timeout=10)
```

### `adjust_pressure_pairs(pairs: 'pl.DataFrame', *, max_iter: 'int' = 50, tol: 'float' = 0.0001) -> 'pl.DataFrame'` {#adjust_pressure_pairs}

Opponent-adjust matchup pressure rates via an additive fixed point.

Fits `rate(off, def) ~ mu + alpha_off + beta_def` per season by
alternating dropback-weighted residual means (league-mean-centered);
league-agnostic (no NFL constant inside).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pairs` | `DataFrame` |  | Output of `pressure_pairs` (or any frame with `season`, `off_team`, `def_team`, `dropbacks`, `pressures`). |
| `max_iter` | `int` | `50` | Fixed-point iteration cap. |
| `tol` | `float` | `0.0001` | Max-abs-change convergence tolerance. |

**Returns**

Per `(season, team)`: raw allowed/generated rates + counts and `adj_pressure_rate_allowed` (`mu + alpha`) / `adj_pressure_rate_generated` (`mu + beta`).

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off (raw). |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def (raw). |
| `adj_pressure_rate_allowed` | double | Opponent-adjusted allowed pressure rate (mu + team offense effect from the additive fixed point). |
| `adj_pressure_rate_generated` | double | Opponent-adjusted generated pressure rate (mu + team defense effect from the additive fixed point). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_line_grades import (
    adjust_pressure_pairs, pressure_pairs,
)
adj = adjust_pressure_pairs(pressure_pairs(load_nfl_pbp([2023])))
print(adj.sort("adj_pressure_rate_generated", descending=True).head())
```

### `build_nfl_player_stats(seasons: 'List[int]', *, summary_level: 'str' = 'week', season_type: 'str' = 'REG', source: 'str' = 'sdv', return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"` {#build_nfl_player_stats}

Build nflverse **player_stats** by aggregating SDV-native play-by-play.

A faithful polars port of nflfastR's `calculate_player_stats`
(`aggregate_game_stats.R`): per-player passing / rushing / receiving frames
are full-outer-joined on the group keys, special-teams touchdowns and fantasy
points are added, and player metadata is joined from
`sportsdataverse.nfl.load_nfl_players`. See the module docstring for the
SDV-PBP column-gap handling (`passing_epa` uses the exact `qb_epa`;
`rushing_epa` / `receiving_epa` use plain `epa` per nflfastR).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Four-digit NFL seasons to aggregate (e.g. `[2023]`). |
| `summary_level` | `str` | `'week'` | `"week"` (group on season + week + player_id, with `opponent_team`) or `"season"` (group on season + player_id, with `recent_team` = last team and `games` = distinct game count). |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"REG+POST"`. Pre-filters the play-by-play before aggregation. |
| `source` | `str` | `'sdv'` | Play-by-play release passed to `load_nfl_pbp`. Defaults to `"sdv"` (the SDV-native enriched release). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame in the published `load_nfl_player_stats` schema. At `summary_level="season"` the `week` / `season_type` / `opponent_team` columns are replaced by a `games` column.

**Example**

```python
from sportsdataverse.nfl import build_nfl_player_stats
wk = build_nfl_player_stats([2023], summary_level="week")
print(wk.shape)

# Season totals as pandas

df_pd = build_nfl_player_stats([2023], summary_level="season",
                               return_as_pandas=True)

# Pipeline next step (one line)

wk.filter(pl.col("attempts") >= 5).sort("passing_epa", descending=True).head()
```

### `build_nfl_player_stats_def(pbp: 'pl.DataFrame', *, weekly: 'bool' = False, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"` {#build_nfl_player_stats_def}

Build player-level defensive stats from play-by-play (nflfastR parity).

A faithful polars port of nflfastR's deprecated
`calculate_player_stats_def()` (`aggregate_game_stats_def.R`). Tackle,
sack (half-sack = 0.5 weighting), pass-defense, interception, safety,
fumble (own/opponent recovery), penalty, and touchdown sub-frames are each
aggregated on `(season, week, team=defteam, player_id)` and full-outer
joined together, then player metadata is joined from
`sportsdataverse.nfl.load_nfl_players`.

Unlike `build_nfl_player_stats`, this function takes a
caller-supplied `pbp` frame directly rather than loading one -- matching
the R function's own signature.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame carrying the wide nflverse defensive columns (`solo_tackle_1_player_id`, `sack_player_id`, `half_sack_{1,2}_player_id`, `interception_player_id`, `pass_defense_{1,2}_player_id`, `fumbled_{1,2}_team` / `fumble_recovery_{1,2}_team`, etc. -- the same columns `sportsdataverse.nfl.load_nfl_pbp` serves). |
| `weekly` | `bool` | `False` | If `True` return one row per (season, week, player); if `False` collapse to one row per `(player_id, team)` -- note this does NOT retain a `season` column even if `pbp` spans multiple seasons (see the module-level note above), matching the R source's own `group_by(player_id, team)` (no `season`). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with the `def_*` column set documented in the nflfastR-parity reference (weekly grain carries `season`/`week`/`season_type`; the season collapse replaces those with `games`).

**Example**

```python
from sportsdataverse.nfl import build_nfl_player_stats_def, load_nfl_pbp
pbp = load_nfl_pbp([2023])
wk = build_nfl_player_stats_def(pbp, weekly=True)
print(wk.shape)

# Season totals (one season's worth of ``pbp`` at a time)

season = build_nfl_player_stats_def(pbp, weekly=False)

# Pipeline next step (one line)

wk.sort("def_sacks", descending=True).head()
```

### `build_nfl_player_stats_kicking(pbp: 'pl.DataFrame', *, weekly: 'bool' = False, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"` {#build_nfl_player_stats_kicking}

Build player-level kicking stats from play-by-play (nflfastR parity).

A faithful polars port of nflfastR's deprecated
`calculate_player_stats_kicking()` (`aggregate_game_stats_kicking.R`).
Field goals (made-distance buckets, `fg_long`, `fg_pct`, `;`-joined
distance lists), extra points, and game-winning-FG attempts (last drive of
the game, trailing by 2 or fewer points) are each aggregated on the kicker
and full-outer joined together, then player metadata is joined from
`sportsdataverse.nfl.load_nfl_players`.

Unlike `build_nfl_team_stats`, this function takes a caller-supplied
`pbp` frame directly rather than loading one -- matching the R
function's own signature.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame carrying `kicker_player_id` / `kicker_player_name`, `field_goal_attempt` / `field_goal_result` / `kick_distance`, `extra_point_attempt` / `extra_point_result`, `fixed_drive`, and `score_differential` (the same columns `sportsdataverse.nfl.load_nfl_pbp` serves). |
| `weekly` | `bool` | `False` | If `True` return one row per (season, week, player) with a `gwfg_distance` list column; if `False` collapse to one row per `(player_id, team)` with a `games` column and a `;`-joined `gwfg_distance_list` string column in place of `gwfg_distance` (the R source's own deliberate column-name change based on the `weekly` flag). Note this does NOT retain a `season` column even if `pbp` spans multiple seasons (see the module-level note above `build_nfl_player_stats_def`). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with the `fg_*`/`pat_*`/`gwfg_*` column set documented in the nflfastR-parity reference.

**Example**

```python
from sportsdataverse.nfl import build_nfl_player_stats_kicking, load_nfl_pbp
pbp = load_nfl_pbp([2023])
wk = build_nfl_player_stats_kicking(pbp, weekly=True)
print(wk.shape)

# Season totals (one season's worth of ``pbp`` at a time)

season = build_nfl_player_stats_kicking(pbp, weekly=False)

# Pipeline next step (one line)

wk.filter(pl.col("fg_att") >= 1).sort("fg_pct", descending=True).head()
```

### `build_nfl_players(*, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#build_nfl_players}

Build an SDV-native NFL players frame from ESPN's public athletes endpoint.

Walks ESPN's public NFL athletes index
(`sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes`),
resolves each athlete's detail resource, flattens it onto the SDV-native
players schema, **dedups to the highest numeric `espn_id` per
`(full_name, birth_date)`** (the ESPN ~2007 4-digit -> 7-digit id
migration left some players with two ids), and enriches `gsis_id` +
other cross-IDs by a best-effort join against
`sportsdataverse.nfl.load_nfl_players`.

This is the **public ESPN-athletes tier only** — a partial mirror of
nflverse's full seven-source `players.parquet` (three of those sources,
PFR / OTC / PFF, require private credentials). ESPN-native rows with no
nflverse match keep only their ESPN fields (cross-IDs left null). For the
full identity master prefer `sportsdataverse.nfl.load_nfl_players`;
use `build_nfl_players` when you need an SDV-native frame that depends
only on the live public ESPN API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

A one-row-per-player `DataFrame` with the documented schema (`espn_id`, `full_name`, `first_name`, `last_name`, `position`, `team`, `jersey`, `height`, `weight`, `birth_date`, `status`, `headshot_url`, `gsis_id`, `esb_id`, `pfr_id`, `pff_id`, `smart_id`, `college`). An empty / failed fetch yields a zero-row frame carrying the same column set (never a raise).

**Example**

```python
from sportsdataverse.nfl import build_nfl_players
players = build_nfl_players()
print(players.shape)

# Pandas output

df = build_nfl_players(return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
build_nfl_players().filter(pl.col("position") == "QB").head()
```

### `build_nfl_rosters(seasons: 'List[int]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#build_nfl_rosters}

Build SDV-native NFL season rosters from the public Shield API.

For each `(season, team)` the public NFL Shield endpoint
`/football/v2/rosters` returns (reached through
`sportsdataverse.nfl.nfl_rosters`), every player in the `persons[]`
array is flattened onto the SDV-native season-roster schema, team
abbreviations are folded to the nflverse standard (season-aware
relocations), and cross-system IDs + college are enriched by a best-effort
left join against `sportsdataverse.nfl.load_nfl_players` on
`gsis_id`.

This is the **public Shield tier only** — a partial mirror of nflverse's
full three-tier roster product. Shield supplies `gsis_id` densely across
all seasons, but the cross-system IDs (`espn_id`, `sportradar_id`,
`yahoo_id`, `rotowire_id`, `pff_id`, `pfr_id`, `fantasy_data_id`,
`sleeper_id`) and `college` are only as dense as the players-table
cross-walk, which is **sparse for pre-2016 seasons**. For the richest roster
data prefer `sportsdataverse.nfl.load_nfl_rosters` (reads nflverse's
published parquet); use `build_nfl_rosters` when you need an
SDV-native frame that depends only on the live NFL Shield API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Seasons to build (e.g. `[2023]` or `range(2020, 2025)`). A single `int` is accepted and wrapped. A season Shield returns no data for contributes no rows rather than raising. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

A one-row-per-player season-roster `DataFrame` with the documented schema. An empty / missing season yields a zero-row frame carrying the same column set (never a raise).

**Example**

```python
from sportsdataverse.nfl import build_nfl_rosters
rosters = build_nfl_rosters([2023])
print(rosters.shape)

# Multi-season build, pandas output

df = build_nfl_rosters(range(2021, 2024), return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
build_nfl_rosters([2023]).filter(pl.col("team") == "KC").head()
```

### `build_nfl_season(game_ids: 'list[int] | None' = None, *, seasons: 'list[int] | None' = None, source: 'str' = 'espn', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"` {#build_nfl_season}

Compile play-by-play for multiple NFL games into one tidy frame.

The `source` parameter determines which input parameter is required:

- `source="espn"` — requires *game_ids*; *seasons* must be `None`.
- `source="nflverse"` — requires *seasons*; *game_ids* must be `None`.

For ESPN games the function either loads a previously cached plays frame or
processes the game fresh via `NFLPlayProcess`.  Individual game failures
are logged and skipped so a single bad game does not abort the whole season
build.  The per-game frames are concatenated with `how="diagonal_relaxed"`
(schema union, missing columns filled with `null`) so games with slightly
different column sets merge cleanly.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `list[int] \| None` | `None` | ESPN event IDs to compile (e.g. `[401671801, 401671802]`). Required when `source="espn"`; must be `None` for other sources. |
| `seasons` | `list[int] \| None` | `None` | Season years to compile (e.g. `[2023, 2024]`). Required when `source="nflverse"`; must be `None` for other sources. |
| `source` | `str` | `'espn'` | Data source. - `"espn"` *(default)*: each game is processed via `NFLPlayProcess(gameId=gid).espn_nfl_pbp()` + `run_processing_pipeline()`. Pass *game_ids*. - `"nflverse"`: delegates to `sportsdataverse.nfl.load_nfl_pbp` for the requested seasons. Pass *seasons*. Returns the full pre-enriched season frame as-is. - `"shield"`: raises `NotImplementedError` — Shield (api.nfl.com) play-by-play lives in the native-pipeline (`nfl-data`) repository, not sdv-py. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of polars. |

**Returns**

All plays from the requested games/seasons, concatenated with schema-union semantics (missing columns are `null`). Returns a zero-row frame if every game failed (ESPN source only). When *return_as_pandas* is `True`, returns a `pandas.DataFrame` instead.

**Example**

```python
from sportsdataverse.nfl import build_nfl_season
df = build_nfl_season(game_ids=[401671801, 401671802])
print(df.shape)

# nflverse season compile (pass season years)

from sportsdataverse.nfl import build_nfl_season
df = build_nfl_season(seasons=[2023], source="nflverse")
print(df.shape)

# With filesystem cache enabled (ESPN)

from sportsdataverse.nfl import build_nfl_season, update_config
update_config(cache_mode="filesystem")
df = build_nfl_season(game_ids=[401671801, 401671802])  # processes + caches
df2 = build_nfl_season(game_ids=[401671801, 401671802]) # served from cache

# Pandas output

from sportsdataverse.nfl import build_nfl_season
df_pd = build_nfl_season(game_ids=[401671801], return_as_pandas=True)
print(df_pd.shape)
```

### `build_nfl_team_stats(seasons: 'List[int]', *, summary_level: 'str' = 'week', season_type: 'str' = 'REG', source: 'str' = 'sdv', return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"` {#build_nfl_team_stats}

Build nflverse **team_stats** by aggregating SDV-native play-by-play.

A faithful polars port of nflfastR's `calculate_stats(stat_type = "team")`
(the `aggregate_game_stats*` family). Offense is keyed on `posteam`,
defense on the tackler's team (per-play `*_team` slot tags -- NOT
`defteam`, which double-counts on return plays), kicking on `posteam`,
and returns / penalties / timeouts on the relevant play team tag. See the
module docstring for the full grouping + SDV-PBP gap notes (`passing_epa`
uses the exact `qb_epa`; `gwfg_*` derive from `fixed_drive`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Four-digit NFL seasons to aggregate (e.g. `[2023]`). |
| `summary_level` | `str` | `'week'` | `"week"` (group on season + week + team, with `opponent_team`) or `"season"` (group on season + team, with a `games` distinct-game count replacing week / season_type / opponent_team). |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"REG+POST"`. Pre-filters the play-by-play before aggregation. |
| `source` | `str` | `'sdv'` | Play-by-play release passed to `load_nfl_pbp`. Defaults to `"sdv"` (the SDV-native enriched release). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame in the published `load_nfl_team_stats` schema (~102 columns). At `summary_level="season"` the `week` / `season_type` / `opponent_team` columns are replaced by a `games` column.

**Example**

```python
from sportsdataverse.nfl import build_nfl_team_stats
wk = build_nfl_team_stats([2023], summary_level="week")
print(wk.shape)

# Season totals as pandas

df_pd = build_nfl_team_stats([2023], summary_level="season",
                             return_as_pandas=True)

# Pipeline next step (one line)

wk.sort("def_sacks", descending=True).head()
```

### `cached_loader(func: 'F') -> 'F'` {#cached_loader}

Decorator that adds caching to a `load_nfl_*` function.

Honors the active `NflConfig.cache_mode`:

- `memory`: dict-based per-process cache.
- `filesystem`: parquet-based cross-process cache under `cache_dir`.
- `off`: no caching, function runs every time.

The cache key is the hash of `(qualified_name, args, kwargs)` with
`return_as_pandas` excluded so memory / disk hits work regardless of
which return shape the caller asked for. The cache always stores the
polars frame internally and converts to pandas on read when requested.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `func` | `F` |  |  |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.cache import cached_loader

@cached_loader
def load_my_thing(season: int, return_as_pandas: bool = False):
    # ... fetch parquet, build a polars frame ...
    return pl.DataFrame({"season": [season]})

df1 = load_my_thing(2024)            # network hit, populates cache
df2 = load_my_thing(2024)            # served from cache
df_pd = load_my_thing(2024, return_as_pandas=True)
# `return_as_pandas` is excluded from the cache key, so the
# polars hit is reused and converted to pandas on the way out.

# Switch caching modes at runtime

from sportsdataverse.nfl import clear_cache, update_config

update_config(cache_mode="filesystem")  # parquet-on-disk reuse
df3 = load_my_thing(2024)               # writes parquet under cache_dir
clear_cache()                           # wipe both memory + filesystem
update_config(cache_mode="off")         # bypass cache entirely
```

### `calculate_completion_probability(pbp_data: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#calculate_completion_probability}

Compute completion probability (CP) and CPOE for pass plays.

Mirrors nflfastR's `helper_add_cp_cpoe.R`.  Scores only intended pass
plays (where `air_yards` is not null); non-pass plays receive null in
the `cp` column.  When `complete_pass` is present,
`cpoe = 100 * (complete_pass - cp)` is also added — on nflfastR's
percentage-point scale (`add_cp` in `helper_add_cp_cpoe.R`).

Drops and recomputes any existing `cp` / `cpoe` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | nflverse-format play-by-play DataFrame. Required: `air_yards`, `season`, `ydstogo`, `down`, `posteam`, `home_team`. Optional: `roof`, `pass_location` (for `pass_middle`), `qb_hit`, `complete_pass` (to derive `cpoe`). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus `cp` (null for non-pass plays) and `cpoe` (null when `complete_pass` absent).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_completion_probability

pbp = load_nfl_pbp([2023])
pbp_cp = calculate_completion_probability(pbp)
print(pbp_cp.select("cp", "cpoe").head())
```

### `calculate_epa(df: 'pl.DataFrame') -> 'pl.DataFrame'` {#calculate_epa}

Derive expected points added (EPA) from pre-scored EP point estimates.

This is the **derivation half** of `NFLPlayProcess.__process_epa` lifted
into a shared, model-free function so the same nflfastR-faithful EPA logic
can be reused by the streaming `enrich_nfl_pbp` pipeline and by
process_epa` itself.  It performs **no** model inference — the caller
must already have scored the per-play EP point estimates.

Derivation rules (mirror nflfastR / the original process_epa`):

* Scoring overlays rewrite `EP_end` to the realized point value
  (offense TD `+7` / `+6.92` / 2pt variants, made FG `+3`,
  defensive scores, extra points, etc.) using the same `type.text` /
  `text` classification as process_epa`.
* Turnovers (`end_change_vec` / `downs_turnover`), kickoff turnovers
  and recovered onside kicks flip `EP_end` to the opponent's
  perspective (`EP_end * -1`).
* `lag_EP_end` is the previous play's `EP_end`; `EP_between` flips
  its sign on a prior-play possession change.
* Kickoffs use `EP_start_touchback` as `EP_start`.
* `EPA = EP_end - EP_start` normally; `-EP_start` on a non-scoring
  end-of-half play; `0` on a timeout; `EP_end - EP_start + EP_between`
  on a (non-kickoff, non-`Penalty`) penalty-in-text play.

**Every** `shift` is grouped `.over("game_id")` so a concatenated
multi-game frame never leaks EP across game boundaries — this differs from
process_epa` (which runs one game per instance and therefore needs no
grouping).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Play-by-play DataFrame that already carries the EP point estimates under the ESPN-internal names `EP_start`, `EP_end` and `EP_start_touchback` (e.g. as produced by the EP-scoring half of process_epa`), plus the play-classification / flag columns: `game_id`, `type.text`, `text`, `change_of_pos_team`, `downs_turnover`, `kickoff_onside`, `scoring_play`, `end_of_half` and `penalty_in_text`. See EPA_REQUIRED_COLUMNS`. This function does **not** score EP itself — score it first via the EP feature pipeline (the `EP_*` triple is the ESPN-internal naming, distinct from `calculate_expected_points`'s lowercase `ep`). |

**Returns**

The input frame with the EPA derivation applied. `EP_start` is rewritten to `0.92` for scoring-attempt play types (`Extra Point Good`, `Extra Point Missed`, `Two-Point Conversion Good`, `Two-Point Conversion Missed`, `Two Point Pass`, `Two Point Rush`, `Blocked PAT`) before any other overlays fire. `EP_start` / `EP_end` are then rewritten in place (overlays, sign flips, touchback), `EP_between`, `lag_EP_end` and `lag_change_of_pos_team` are added, `EPA` is added, and lowercase nflverse aliases `ep` (`= EP_end`), `epa` (`= EPA`), `ep_start` (`= EP_start`) and `ep_end` (`= EP_end`) are added for downstream contract parity.

**Example**

```python
# For most use cases, call the high-level entry point instead. ``enrich_nfl_pbp`` scores EP, derives EPA, and adds WP/WPA/CP/CPOE in one shot on any nflverse-shape frame

    from sportsdataverse.nfl import load_nfl_pbp
    from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

    pbp = load_nfl_pbp([2023])
    enriched = enrich_nfl_pbp(pbp)
    print(enriched.select("game_id", "ep", "epa").head())

``calculate_epa`` directly requires ESPN-internal columns
(``EP_start``, ``EP_end``, ``EP_start_touchback``, ``type.text``,
etc.) produced by ``NFLPlayProcess``.  It is called internally by
``NFLPlayProcess.__process_epa`` and by the ``enrich_nfl_pbp``
orchestrator — a naked ``calculate_epa(load_nfl_pbp([2023]))``
will raise ``KeyError`` because those columns are absent from a
nflverse frame.
```

### `calculate_expected_points(pbp_data: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#calculate_expected_points}

Compute expected points for provided plays.

Mirrors nflfastR's `calculate_expected_points()`.  Drops and recomputes
any existing `ep` / `*_prob` columns so the output is always fresh.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | Play-by-play DataFrame with nflverse columns. Required: `season`, `posteam`, `home_team`, `roof`, `half_seconds_remaining`, `yardline_100`, `down`, `ydstogo`, `posteam_timeouts_remaining`, `defteam_timeouts_remaining`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus: `td_prob`, `opp_td_prob`, `fg_prob`, `opp_fg_prob`, `safety_prob`, `opp_safety_prob`, `no_score_prob`, and `ep` (expected points, clipped to [-10, 10]).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_expected_points

pbp = load_nfl_pbp([2023])
pbp_ep = calculate_expected_points(pbp)
print(pbp_ep.select("ep").head())
```

### `calculate_nfl_series_conversion_rates(pbp: 'pl.DataFrame', *, weekly: 'bool' = False, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"` {#calculate_nfl_series_conversion_rates}

Compute per-team offense + defense series conversion rates.

A faithful polars port of nflfastR's `calculate_series_conversion_rates`.
Series where `down` is null (kickoffs, PAT/2pt attempts, non-plays, no
`posteam`) and series ending in a `"QB kneel"` are excluded from the
series count before rates are computed, matching the R source.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame carrying `season`, `week`, `posteam`, `defteam`, `down`, `series`, `series_success`, and `series_result` (added by the `add_series_data` port). Rows must already be in play order within each series so the internal `first()`/`last()` series collapse is correct. |
| `weekly` | `bool` | `False` | If `True`, group on `(season, team, week)`; if `False` (default), group on `(season, team)` -- collapsing every week into one season-level rate. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with one row per team (per week when `weekly=True`), `off_n`/`def_n` (series count) plus the `off_*`/`def_*` rate columns documented in reference Sec 11. A team with offensive series but zero defensive series in a group (or vice versa -- effectively never happens in real data) carries nulls in the missing side rather than being dropped (full outer join).

**Example**

```python
from sportsdataverse.nfl import calculate_nfl_series_conversion_rates
rates = calculate_nfl_series_conversion_rates(pbp)
rates.filter(pl.col("team") == "KC").select("off_scr", "def_scr")

# Weekly grain

weekly = calculate_nfl_series_conversion_rates(pbp, weekly=True)

# Pipeline next step (one line)

rates.sort("off_scr", descending=True).head()
```

### `calculate_nfl_standings(games: 'pl.DataFrame', *, teams: 'pl.DataFrame | None' = None, tiebreaker_depth: 'int' = 3, playoff_seeds: 'int | None' = None, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"` {#calculate_nfl_standings}

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

### `calculate_win_probability(pbp_data: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#calculate_win_probability}

Compute win probability for provided plays.

Mirrors nflfastR's `calculate_win_probability()`.  Uses the
spread-adjusted model (`wp_spread.ubj`) when `spread_line` is
non-null, and falls back to the naive model (`wp_naive.ubj`) for plays
with a missing spread line.  Drops and recomputes any existing `wp` /
`vegas_wp` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | Play-by-play DataFrame. Required: all EP columns plus `score_differential`, `game_seconds_remaining`, `spread_line`, `receive_2h_ko`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus: `wp` (naive WP) and `vegas_wp` (spread-adjusted WP).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_win_probability

pbp = load_nfl_pbp([2023])
pbp_wp = calculate_win_probability(pbp)
print(pbp_wp.select("wp", "vegas_wp").head())
```

### `calculate_wpa(df: 'pl.DataFrame') -> 'pl.DataFrame'` {#calculate_wpa}

Derive win probability added (WPA) from pre-scored WP point estimates.

This is the **derivation half** of `NFLPlayProcess.__process_wpa` lifted
into a shared, model-free function so the same nflfastR-faithful WPA logic
can be reused by the streaming `enrich_nfl_pbp` pipeline and by
process_wpa` itself.  It performs **no** model inference — the caller
must already have scored the per-play WP point estimates
(`wp_spread.ubj`) for the start / touchback / end feature views and
attached them as `wp_before` / `wp_touchback` / `wp_after`.  This
mirrors `calculate_epa`, which likewise consumes pre-scored EP point
estimates and leaves prediction to the orchestrator.

Derivation rules (mirror the original process_wpa`):

* **Leading overlay (do not drop):** on a kickoff (`type.text` in
  `kickoff_vec`) `wp_before` is replaced by `wp_touchback` — the
  win-probability scored from the touchback feature view — before any
  other column derives.  This is the WP analogue of the EPA `0.92`
  scoring-attempt overlay and must fire first.
* `def_wp_before = 1 - wp_before`; `home_wp_before` / `away_wp_before`
  are the posteam->home perspective columns (the offense's `wp_before`
  flows to home when the start possession team is the home team, otherwise
  to the defense `def_wp_before`).
* `wp_after` is rewritten by the end-of-half / end-of-game / OT two-path:
  timeouts hold `wp_before`; a completed final play resolves to `1.0` /
  `0.0` by the winner; end-of-half and `End Period` / `End of Half`
  lead plays take `lead_wp_before` (or `1 - lead_wp_before` on a
  possession change); a possession change otherwise flips the lead;
  everything else keeps the model `wp_after`.
* `def_wp_after = 1 - wp_after`; `home_wp_after` / `away_wp_after`
  use the **end** possession team for the perspective flip.
* `wpa = wp_after - wp_before`.

**Every** `shift` / forward reference is grouped `.over("game_id")` so a
concatenated multi-game frame never leaks WP across game boundaries — the
`lead_wp_before` / `lead_wp_before2` shifts and the end-of-game
`game_play_number == max()` lookup are all per-game.  This differs from
process_wpa` (which runs one game per instance and therefore needs no
grouping).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Play-by-play DataFrame that already carries the WP point estimates `wp_before` (start feature view), `wp_touchback` (touchback feature view) and `wp_after` (end feature view), plus the play-classification / perspective columns `game_id`, `type.text`, `homeTeamId`, `start.pos_team.id`, `end.pos_team.id`, `start.pos_team_receives_2H_kickoff`, `change_of_pos_team`, `scoringPlay`, `kickoff_onside`, `end_of_half`, `status_type_completed`, `pos_score_diff_end`, `lead_play_type`, `lead_pos_team` and `game_play_number`. See WPA_REQUIRED_COLUMNS`. This function does **not** score WP itself — score it first via `calculate_win_probability` / the `wp_spread` feature pipeline. |

**Returns**

The input frame with the WPA derivation applied: `wp_before` rewritten by the kickoff-touchback overlay; `def_wp_before`, `home_wp_before`, `away_wp_before`, `lead_wp_before`, `lead_wp_before2`, the rewritten `wp_after`, `def_wp_after`, `home_wp_after`, `away_wp_after` and `wpa` added; plus first-class lowercase aliases `wp` (`= wp_before`), `def_wp` (`= def_wp_before`), `home_wp` (`= home_wp_before`) and `away_wp` (`= away_wp_before`) for downstream contract parity (the per-play offense win probability is the pre-snap `wp_before`, matching nflfastR's `wp` semantics).

**Example**

```python
# For most use cases, call the high-level entry point instead. ``enrich_nfl_pbp`` scores WP, derives WPA, and adds EP/EPA/CP/CPOE in one shot on any nflverse-shape frame

    from sportsdataverse.nfl import load_nfl_pbp
    from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

    pbp = load_nfl_pbp([2023])
    enriched = enrich_nfl_pbp(pbp)
    print(enriched.select("game_id", "wp", "def_wp", "home_wp", "away_wp", "wpa").head())

``calculate_wpa`` directly requires ESPN-internal columns
(``wp_before``, ``wp_touchback``, ``wp_after``, ``homeTeamId``,
``start.pos_team.id``, etc.) produced by ``NFLPlayProcess``.  It is
called internally by ``NFLPlayProcess.__process_wpa`` and by the
``enrich_nfl_pbp`` orchestrator — a naked
``calculate_wpa(load_nfl_pbp([2023]))`` will raise ``KeyError``
because those columns are absent from a nflverse frame.
```

### `calculate_xpass(pbp_data: 'pl.DataFrame', *, models_dir: 'Union[str, None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#calculate_xpass}

Compute expected dropback probability (`xpass`) and `pass_oe`.

Faithful polars port of nflfastR's `add_xpass` /
`prepare_xpass_data` (`helper_add_xpass.R`).  Scores a single
`binary:logistic` XGBoost model (17 features, in `XPASS_FEATURES`
order) over the rows that satisfy nflfastR's `valid_play` filter:

- `season >= 2006` (before this the NFL did not mark scrambles), and
- `play_type in {"no_play", "pass", "run"}`, and
- none of `posteam` / `down` / `defteam_timeouts_remaining` /
  `posteam_timeouts_remaining` / `yardline_100` /
  `score_differential` is null.

The era2..4 + `outdoors` / `retractable` / `dome` dummies and the
`home` indicator are produced by make_cp_mutations` (the same
nflfastR `make_model_mutations` logic CP uses) rather than re-derived.
`wp` / `vegas_wp` are the start-of-play win-probability columns and
must already be present (run after the WP step / inside
`enrich_nfl_pbp`).

The booster ships with no embedded `feature_names`, so the DMatrix is
built with `XPASS_FEATURES` as the column order — feeding the
features in any other order silently yields wrong predictions.

Drops and recomputes any existing `xpass` / `pass_oe` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | nflverse-format play-by-play DataFrame. Required: `season`, `play_type`, `posteam`, `home_team`, `down`, `ydstogo`, `yardline_100`, `qtr`, `wp`, `vegas_wp`, `score_differential`, `half_seconds_remaining`, `posteam_timeouts_remaining`, `defteam_timeouts_remaining`. Optional: `roof` (for the roof dummies), `pass` / `rush` (the 0/1 dropback / rush indicators used by `pass_oe`). |
| `models_dir` | `Union[str, None]` | `None` | Optional directory to load `xpass_model.ubj` from instead of downloading / caching it (offline or custom model). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus `xpass` (predicted pass probability, null outside the `valid_play` filter; float64) and `pass_oe` (`100 * (pass - xpass)`, null when `xpass` is null and null when `rush == 0 & pass == 0`; float64).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp, calculate_xpass

pbp = enrich_nfl_pbp(load_nfl_pbp([2023]))  # gives wp / vegas_wp
pbp_xp = calculate_xpass(pbp)
print(pbp_xp.select("xpass", "pass_oe").head())

# Pipeline next step

pbp_xp.filter(pl.col("play_type") == "pass").select("posteam", "xpass", "pass_oe").head()
```

### `calculate_xyac(pbp_data: 'pl.DataFrame', *, models_dir: 'Optional[Union[str, Path]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#calculate_xyac}

Compute expected yards after catch (xYAC) for intended pass plays.

Faithful polars port of nflfastR's `add_xyac`.  Unlike a per-statistic
regressor, xYAC is **one** `multi:softprob` model (`num_class=76`) that
predicts a distribution over YAC buckets (`yac = -5..70`); the five output
columns are *derived* from that distribution by re-scoring expected points on
every outcome.  `ep` is **not** required on the input — it is recomputed on
the outcome rows via `calculate_expected_points`.  The play's pre-snap
`ep` (`original_ep`) is the EPA baseline; `air_epa` is also part of the
baseline (`xyac_epa = Σ((ep − original_ep)·prob) − air_epa`).  `air_epa`
is **optional**: when present (the nflverse path) it is used verbatim so
parity is byte-for-byte preserved; when absent (the Shield-native / ESPN
path) it is computed from the already-scored `yac == 0` (catch-spot)
outcome — `air_epa = ep(yac == 0) − original_ep` — and, since it was
genuinely missing, surfaced as an extra `air_epa` output column.

Inference filter (nflfastR `valid_pass` ∧ `distance_to_goal != 0`):
`complete_pass == 1` OR `incomplete_pass == 1` OR `interception == 1`,
`air_yards` in `[-15, 70)`, non-null `receiver_player_name` and
`pass_location`, and `distance_to_goal != 0`.  Non-qualifying rows
receive null in all five columns.  Drops and recomputes any existing xYAC
output columns.

The xYAC model (`xyac_model.ubj`, ~34 MB) is **not** bundled in the
wheel: on first use it is downloaded from the `nfl_model_artifacts`
GitHub release and cached under `<cache_dir>/models/` (see
`sportsdataverse.nfl.get_config`).  Subsequent calls load it from the
cache; `clear_cache()` deliberately preserves the `models/` subdir so a
data-cache clear does not force a re-download.  Pass `models_dir=` to
point at a local directory containing `xyac_model.ubj` (offline / custom
model override).  If the model is genuinely unavailable (no cache + no
network) the underlying loader raises `FileNotFoundError`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | nflverse-format play-by-play DataFrame. Required: `air_yards`, `season`, `half_seconds_remaining`, `yardline_100`, `ydstogo`, `down`, `posteam`, `home_team`, `roof`, `ep`, `posteam_timeouts_remaining`, `defteam_timeouts_remaining`, `complete_pass`, `incomplete_pass`, `interception`, `pass_location`, `receiver_player_name`. Optional: `air_epa` (used verbatim when present for byte-for-byte nflverse parity; computed from the `yac == 0` outcome and added as an output column when absent), `qb_hit`. |
| `models_dir` | `Optional[Union[str, Path]]` | `None` | Optional directory to load `xyac_model.ubj` from instead of downloading/caching it (offline use or a custom-trained model). When `None` (default) the model is resolved bundled → cache → downloaded-from-release. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus the five nflfastR xYAC columns (`Float64`, null on non-qualifying rows): `xyac_epa`, `xyac_mean_yardage`, `xyac_median_yardage`, `xyac_success`, `xyac_fd`. When the input lacked `air_epa` and at least one qualifying pass was scored, a computed `air_epa` column (catch-spot air EPA) is also added.

**Example**

```python
import polars as pl

from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xyac

pbp = load_nfl_pbp([2023])
pbp = calculate_xyac(pbp)
print(pbp.select("xyac_epa", "xyac_mean_yardage").head())

# Pipeline next step (one line)

pbp.filter(pl.col("xyac_epa").is_not_null()).select("xyac_epa", "xyac_fd").head()
```

### `clean_nfl_pbp(df: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#clean_nfl_pbp}

Canonicalize names/ids/teams on a play-by-play frame (nflfastR `clean_pbp` port).

See the module docstring for the full column set added, the
compute-if-absent scope note on `pass`/`rush`, and the lookaround ->
capture-group regex rewrites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | An nflverse-shape (or ESPN/native) play-by-play `polars.DataFrame`. Required columns: `desc`, `epa`, `game_id`, `play_id`, `season`, `posteam`. See the module docstring for the full optional-column-with-default list. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

The input frame with every §6 column added/overwritten (idempotent -- pre-existing values of those columns, except `pass`/`rush`, are dropped and recomputed). A zero-row input yields a zero-row frame carrying the full documented schema rather than raising.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_clean import clean_nfl_pbp

pbp = load_nfl_pbp([2023])
cleaned = clean_nfl_pbp(pbp)
print(cleaned.select("name", "id", "fantasy").head())

# Pandas output

cleaned_pd = clean_nfl_pbp(pbp, return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
cleaned.filter(pl.col("play") == 1).group_by("passer").len()
```

### `clear_cache() -> 'None'` {#clear_cache}

Clear both memory and filesystem caches.

Memory: empties the in-process dict.
Filesystem: removes all entries under `config.cache_dir`. The
directory itself is preserved so subsequent writes succeed without
needing `mkdir`.

The `models/` subdirectory is **deliberately preserved** — it holds
download-on-demand model artifacts (e.g. the ~34 MB `xyac_model.ubj`)
that are expensive to re-fetch. Clearing the *data* cache should not force
a model re-download; delete `<cache_dir>/models/` by hand to drop those.

**Example**

```python
from sportsdataverse.nfl import clear_cache, load_nfl_pbp
clear_cache()
pbp = load_nfl_pbp(seasons=[2024])

# Pair with a cache-mode switch

from sportsdataverse.nfl import clear_cache, update_config
update_config(cache_mode="filesystem")
# ... lots of cached calls accumulate parquet files on disk ...
clear_cache()  # wipe disk + memory together
```

### `compose_counting_projection(rate_proj: 'pl.DataFrame', avail_proj: 'pl.DataFrame', *, rate_col: 'str' = 'proj_rate', volume_col: 'str' = 'proj_volume') -> 'pl.DataFrame'` {#compose_counting_projection}

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
| `player_id` | character | nflverse gsis player id (character join key; asserted Utf8 on both sides of the join). |
| `proj_rate` | double | Projected per-opportunity rate carried through from the skill projection (rate_col). |
| `proj_volume` | double | Projected opportunity volume carried through from the skill projection (volume_col). |
| `proj_availability` | double | Projected availability rate in [0, 1] from nfl_availability_projection. |
| `proj_counting` | double | Composed counting projection - proj_rate x proj_volume x proj_availability (the only place skill and availability meet). |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.nfl_availability import compose_counting_projection
out = compose_counting_projection(rate_frame, avail_frame)
```

### `efficiency_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'` {#efficiency_ratings}

One row per team: opponent-adjusted offense/defense EPA per play.

Filters `plays` to competitive non-special-teams scrimmage plays
(`special != 1`, `qb_kneel != 1`, `qb_spike != 1`,
`min_competitive_wp <= wp <= max_competitive_wp`, non-null
`epa`/`posteam`/`defteam`) and fits
`opponent_adjusted_ridge` on `epa`. Callers pass an already
as-of-date-filtered frame (the public `nfl_ratings` entry point does
the date filter) -- this function is pure.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | An `load_nfl_pbp`-schema frame carrying `game_id`, `posteam`, `defteam`, `home_team`, `epa`, `wp`, `special`, `qb_kneel`, `qb_spike`. |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs (`ridge_lambda` + the competitive-`wp` window); defaults to `RatingsConfig`. |

**Returns**

One row per `team_id` (Utf8) with `adj_off_epa` / `adj_def_epa` / `adj_net` (Float64, `adj_net = adj_off_epa - adj_def_epa`) and `games` (Int64). Zero-row, correctly-typed on empty/fully-filtered input.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()
```

### `env_adjusted_make_prob(pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#env_adjusted_make_prob}

Add `base_make_prob` + environment-adjusted `exp_make_prob`.

`exp_make_prob = sigmoid(logit(base) + b_wind*wind + b_temp*(temp-baseline)
+ b_alt*altitude_kft)` with coefficients from
`sportsdataverse.nfl.nfl_scheme_constants.ENVIRONMENT_FG_COEF` and
altitude from `STADIUM_ALTITUDE[home_team]`.  Dome / closed-roof kicks
(and missing readings) are treated as neutral (wind 0, temp = baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | FG-attempt rows with `yardline_100` / `roof` / `temp` / `wind` / `home_team` (+ `season` or `era0..era4` / `fg_roof`). |

**Returns**

The input plus `base_make_prob` and `exp_make_prob` (Float64).

| col_name | type | description |
|---|---|---|
| `base_make_prob` | double | Shipped fg_model make probability (with nfl4th long-kick clamps applied). |
| `exp_make_prob` | double | Environment-adjusted make probability (logit shift for long-kick clamp correction, wind, temperature and altitude). |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.nfl_kicker_rating import env_adjusted_make_prob
fg = pl.read_parquet("tests/fixtures/nfl_scheme/fg_attempts_2019_2023.parquet")
out = env_adjusted_make_prob(fg)
print(out.select("base_make_prob", "exp_make_prob").describe())
```

### `espn_nfl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_nfl_teams}

espn_nfl_teams - look up NFL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nfl.espn_nfl_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `team_alternate_color` | character | Alternate team color; `team_detail = TRUE` only. |
| `team_color` | character | Primary team color; `team_detail = TRUE` only. |
| `team_display_name` | character | Full team display name; `team_detail = TRUE` only. |
| `team_id` | character | ESPN team id. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_is_all_star` | logical | TRUE if the row represents an All-Star team. |
| `team_location` | character | Team location / school name; `team_detail = TRUE` only. |
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `team_nickname` | character | Team nickname label; `team_detail = TRUE` only. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_slug` | character | Team slug for the stat row. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |

**Example**

```python
from sportsdataverse.nfl import espn_nfl_teams
teams = espn_nfl_teams()
teams.shape

# Pandas round-trip

teams_pd = espn_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbreviation", "team_display_name"]].head()

# Force a refresh after upstream ESPN updates

espn_nfl_teams.cache_clear()  # underlying lru_cache
teams = espn_nfl_teams()
```

### `fg_make_probability(yardline_100: 'np.ndarray', fg_roof: 'np.ndarray', era: 'np.ndarray') -> 'Optional[np.ndarray]'` {#fg_make_probability}

Predict FG make probability from the bundled `fg_model` (public wrapper).

Thin supported alias over the private underscore-prefixed helper so downstream
consumers (e.g. the kicker-rating spine) reuse the shipped model through a
public import instead of a private reach.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `yardline_100` | `ndarray` |  | Kick spot (yards from the opponent end zone); the attempt distance is `yardline_100 + 18`. |
| `fg_roof` | `ndarray` |  | 1.0 when `roof == "outdoors"` else 0.0, per kick. |
| `era` | `ndarray` |  | `(n, 5)` one-hot era matrix (`era0`..`era4`, season cuts 2001/2005/2013/2017). |

**Returns**

Make probabilities (with nfl4th's long-kick clamps), or `None` when the bundled model is unavailable.

**Example**

```python
import numpy as np
from sportsdataverse.nfl.nfl_fourth_down import fg_make_probability
p = fg_make_probability(
    np.array([30.0]), np.array([1.0]),
    np.array([[0.0, 0.0, 0.0, 0.0, 1.0]]),
)
print(p)
```

### `get_2pt_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'` {#get_2pt_wp}

Win probability of the PAT-vs-2pt choice after a touchdown (nfl4th `get_2pt_wp`).

For each row, scores the post-touchdown state under three scoring outcomes
(0 / 1 / 2 added points) from the kicking-off team's ensuing-drive WP, and
combines them with the 2-pt conversion probability (`two_pt_model`) and the
PAT make probability (the FG model at `yardline_100 = 15`) into `wp_td` —
the better of go-for-2 and kick-the-PAT.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of post-touchdown states, already carrying the prepared state columns (see module docstring). |

**Returns**

A pandas frame with `go_index`, `yardline_100` (always 0) and `wp_td`. `wp_td` is NaN when the WP / 2-pt models are unavailable.

**Example**

```python
from sportsdataverse.nfl.nfl_fourth_down import get_2pt_wp
out = get_2pt_wp(touchdown_states)
print(out[["go_index", "wp_td"]].head())
```

### `get_4th_down_probs(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'` {#get_4th_down_probs}

Full 4th-down decision surface (nfl4th `add_4th_probs`) + recommendation.

Runs `get_go_wp`, `get_fg_wp`, `get_punt_wp` on the
fourth-down rows and adds the combined option columns plus:

* `go_boost` -- nfl4th's headline number: `100 * (go_wp - max(fg_wp,
  punt_wp))` in percentage points (a NaN `punt_wp` is treated as 0).
* `fourth_down_recommendation` -- the max-WP choice among `{go, punt,
  field_goal}` (NaN options are excluded).
* `go_wp_diff` / `punt_wp_diff` / `fg_wp_diff` -- each option's WP minus
  the recommended option's WP (the recommended option's diff is 0, the others
  <= 0).  NaN where the option WP is NaN.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations (the nflverse-shape output of `load_nfl_pbp`; see module docstring for required columns). |

**Returns**

A pandas copy of `pbp_df` with the decision columns added. Empty input returns the input plus empty decision columns.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_4th_down_probs

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_4th_down_probs(fourth)
print(out[["go_wp", "punt_wp", "fg_wp", "go_boost", "fourth_down_recommendation"]].head())
```

### `get_config() -> 'NflConfig'` {#get_config}

Return the live `NflConfig` singleton.

The same object is returned on every call; mutate via `update_config`
rather than reassigning fields directly so future hooks (e.g. logging
on config change) have a single choke point.

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()
print(cfg.cache_mode, cfg.cache_duration, cfg.cache_dir)

# Pair with ``update_config`` to verify a change took effect

from sportsdataverse.nfl import update_config, get_config
update_config(cache_mode="off")
assert get_config().cache_mode == "off"
```

### `get_fg_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'` {#get_fg_wp}

Expected win probability of attempting a field goal (nfl4th `get_fg_wp`).

The make probability comes from the self-trained `fg_model` (a
`binary:logistic` XGBoost re-train of the original mgcv GAM, features
`[yardline_100, fg_roof, fg_era]`), shrunk by 0.9 for kicks at/beyond
`yardline_100 = 38` and zeroed at/beyond `yardline_100 = 45`
(>= ~63-yard kicks).  The made-FG state (opponent receives a touchback
kickoff at the 25, kicking team +3) and the missed-FG state (opponent takes
over 8 yards back of the spot, capped at the 80) are each scored with win
probability; `fg_wp = make_prob * make_wp + (1 - make_prob) * miss_wp`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `fg_make_prob`, `make_fg_wp`, `miss_fg_wp` and `fg_wp` (from the kicking team's perspective). All four are NaN when the FG model or WP model is unavailable.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_fg_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_fg_wp(fourth)
print(out[["fg_make_prob", "fg_wp"]].head())
```

### `get_go_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'` {#get_go_wp}

Expected win probability of going for it on 4th down (nfl4th `get_go_wp`).

The fd_model 76-class yards-gained distribution is expanded per play; each
outcome's hypothetical post-play game state (turnover-on-downs flip, +6
touchdown with the PAT/2-pt branch routed through `get_2pt_wp`, 6-second
runoff, goal-to-go distance shrink) is scored with win probability and the
end-of-game kneel-out clamps are applied; the option value is the
prob-weighted WP.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the prepared state columns (see module docstring). The frame is prepared internally if it lacks the derived columns. |

**Returns**

A pandas copy of `pbp_df` plus `go_wp` (prob-weighted WP of going for it), `first_down_prob` (P(conversion)), `wp_succeed` (mean WP over conversion outcomes) and `wp_fail` (mean WP over failure outcomes). All are NaN when the fourth-down / WP models are unavailable (`FD_MODEL_AVAILABLE` / `WP_MODEL_AVAILABLE`).

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_go_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_go_wp(fourth)
print(out[["go_wp", "first_down_prob"]].head())
```

### `get_punt_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'` {#get_punt_wp}

Expected win probability of punting on 4th down (nfl4th `get_punt_wp`).

The punt landing distribution (`punt_data`: `yardline_after` / `pct` /
`muff` per `yardline_100`) is joined per play; possession is flipped to
the receiving team, with return-touchdown (`yardline_after == 100`) and muff
(`muff == 1`) recoveries flipping the ball back to the punting team; each
landing spot's ensuing-drive WP is scored and the option value is the
prob-weighted WP from the punting team's perspective.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `punt_wp`. `punt_wp` is NaN where the punt distribution has no support for the play's `yardline_100` (inside the punting team's own 31, where the table is empty — matching the R reference's left-join NA behavior) or when the WP model is unavailable.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_punt_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_punt_wp(fourth)
print(out[["punt_wp"]].head())
```

### `nfl_availability_projection(seasons: 'List[int]', target_season: 'int', *, team_games: 'int' = 17, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_availability_projection}

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

### `nfl_clear_token_cache() -> 'None'` {#nfl_clear_token_cache}

Drop the cached `api.nfl.com` token (forces a fresh mint on the next call).

### `nfl_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'Union[str, int]', *, rng: 'Optional[np.random.Generator]' = None, elo: 'Optional[Mapping[str, float]]' = None, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame]'` {#nfl_compute_results}

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

### `nfl_draft_projection(seasons: 'List[int]', target_class: 'int', *, lam: 'float' = 100.0, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_draft_projection}

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

### `nfl_fantasy_projection(seasons: 'List[int]', target_season: 'int', *, scoring: 'Union[Dict[str, float], str]' = 'ppr', calibrate: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_fantasy_projection}

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

### `nfl_game_details(game_id: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, raw: 'bool' = False) -> 'Dict'` {#nfl_game_details}

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

### `nfl_game_pbp(game_id: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_as_pandas: 'bool' = False)` {#nfl_game_pbp}

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

### `nfl_game_schedule(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'int' = 1, headers: 'Optional[Dict[str, str]]' = None, raw: 'bool' = False) -> 'Dict'` {#nfl_game_schedule}

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

### `nfl_game_script(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_game_script}

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

### `nfl_headers_gen(token: 'Optional[str]' = None) -> 'Dict[str, str]'` {#nfl_headers_gen}

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

### `nfl_kicker_rating(seasons: 'Union[int, List[int]]', *, as_of: 'Optional[Tuple[int, int]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_kicker_rating}

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

### `nfl_line_grades(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_line_grades}

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

### `nfl_ngs_gamecenter_overview(game_id, group: 'str' = 'passers', return_as_pandas: 'bool' = False)` {#nfl_ngs_gamecenter_overview}

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

### `nfl_ngs_leaders(category: 'str' = 'speed', season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)` {#nfl_ngs_leaders}

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

### `nfl_ngs_league_schedule(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)` {#nfl_ngs_league_schedule}

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

### `nfl_ngs_league_schedule_current(return_as_pandas: 'bool' = False)` {#nfl_ngs_league_schedule_current}

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

### `nfl_ngs_league_teams(return_as_pandas: 'bool' = False)` {#nfl_ngs_league_teams}

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

### `nfl_ngs_man_zone_rates(seasons: 'Union[int, Sequence[int]]', *, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nfl_ngs_man_zone_rates}

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

### `nfl_ngs_microsite_chart(season: 'int' = 2024, season_type: 'str' = 'REG', week=None, chart_type=None, team_id=None, limit: 'int' = 100, offset: 'int' = 0, return_as_pandas: 'bool' = False)` {#nfl_ngs_microsite_chart}

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

### `nfl_ngs_microsite_chart_players(season: 'int' = 2024, season_type: 'str' = 'REG', return_as_pandas: 'bool' = False)` {#nfl_ngs_microsite_chart_players}

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

### `nfl_ngs_play_is_highlight(game_id, play_id, return_as_pandas: 'bool' = False)` {#nfl_ngs_play_is_highlight}

Look up whether a single play is an NGS highlight -- one-row frame.

Wraps `/api/plays/isHighlight` (keyed by NGS `gameId` + `playId`). When
the play is a highlight, the response's nested `highlight` block (the play
metadata, the `players` involved, season/week/team) is flattened onto the
row alongside the top-level `gameId`/`playId`/`isHighlight` flag. Pull a
real `(gameId, playId)` pair from `nfl_ngs_leaders` -- each leader
entry's `play_gameId` / `play_playId` is a known highlight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  | NGS `gameId` (e.g. `"2024111800"`). |
| `play_id` |  |  | the play id within that game (e.g. `1214`). |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A one-row polars (or pandas) `DataFrame` with `gameId`, `playId`, `isHighlight` and (when true) flattened `highlight_*` columns.

| col_name | type | description |
|---|---|---|
| `gameId` | integer | NFL Shield API game identifier for the game this highlight record belongs to. |
| `playId` | integer | Unique play event identifier (UUID). |
| `isHighlight` | logical | Boolean flag indicating whether this play record has been designated as a highlight by the NFL Shield API. |
| `highlight_gameId` | integer | NFL Shield API game identifier associated with the specific highlight clip. |
| `highlight_playId` | integer | NFL Shield API play identifier for the specific play associated with the highlight clip. |
| `highlight_play_playType` | character | Text label for the type of play (e.g., 'PASS', 'RUSH', 'PUNT') as classified by the NFL Shield API. |
| `highlight_play_gameId` | integer | NFL Shield API game identifier embedded in the play detail record for the highlight. |
| `highlight_play_gameKey` | integer | NFL Shield internal game key integer used to identify the game in internal API routing. |
| `highlight_play_yardlineSide` | character | Team abbreviation indicating which team's side of the field the yardline is on. |
| `highlight_play_absoluteYardlineNumber` | integer | Absolute yard line number (1–100 from one end zone) of the line of scrimmage at the snap for the highlighted play. |
| `highlight_play_yardlineNumber` | integer | Numeric yard line (1–50 from nearest end zone) of the line of scrimmage at the snap. |
| `highlight_play_timeOfDayUTC` | character | Wall-clock timestamp in UTC at which the highlighted play occurred during the broadcast. |
| `highlight_play_isPenalty` | logical | Boolean flag indicating whether a penalty was assessed on the highlighted play. |
| `highlight_play_homeScore` | integer | Home team's score at the end of the highlighted play. |
| `highlight_play_visitorScore` | integer | Visiting team's score at the end of the highlighted play. |
| `highlight_play_playStats` | integer | Serialized or encoded statistical detail associated with the play outcomes (e.g., yards, player ids). |
| `highlight_play_playId` | integer | NFL Shield API unique integer identifier for the specific play within the game. |
| `highlight_play_playDescription` | character | Official NFL text description of the highlighted play (e.g., '(12:34) T.Brady pass short right to J.Edelman for 8 yards'). |
| `highlight_play_playTypeCode` | integer | Integer code corresponding to the play type classification in the NFL Shield API. |
| `highlight_play_quarter` | integer | Quarter number (1–4, or 5 for overtime) during which the highlighted play occurred. |
| `highlight_play_down` | integer | Down number (1–4) at the time of the highlighted play. |
| `highlight_play_yardsToGo` | integer | Integer yards needed for a first down on the highlighted play. |
| `highlight_play_actualYardsToGo` | double | Exact decimal yards required for a first down on the highlighted play. |
| `highlight_play_actualYardlineForFirstDown` | double | Actual yard line marker needed for a first down on the highlighted play. |
| `highlight_play_possessionTeamId` | character | NFL Shield team identifier for the team that had possession during the highlighted play. |
| `highlight_play_isGoalToGo` | logical | Boolean flag indicating whether the highlighted play occurred in a goal-to-go situation. |
| `highlight_play_health` | character | Data quality or tracking health status string indicating the reliability of player/ball tracking data for this play. |
| `highlight_play_endGameClock` | character | Game clock time remaining at the end of the highlighted play in MM:SS format. |
| `highlight_play_startGameClock` | character | Game clock time remaining at the start of the highlighted play in MM:SS format. |
| `highlight_play_playState` | character | State or status of the play record (e.g., 'FINAL', 'LIVE') in the NFL Shield API at time of capture. |
| `highlight_play_preSnapHomeScore` | integer | Home team's score immediately before the snap of the highlighted play. |
| `highlight_play_preSnapVisitorScore` | integer | Visitor team's score immediately before the snap of the highlighted play. |
| `highlight_play_sequence` | integer | Sequential order integer of this play within the game, as used by the NFL Shield API. |
| `highlight_play_gameClock` | character | Game clock time remaining at the start of the highlighted play in MM:SS format. |
| `highlight_play_yardline` | character | Human-readable yardline string (e.g., 'KC 35') indicating where the play began. |
| `highlight_play_isScoring` | logical | Boolean flag indicating whether the highlighted play resulted in a score. |
| `highlight_play_isEndQuarter` | logical | Boolean flag indicating whether the highlighted play was the final play of a quarter. |
| `highlight_play_isSTPlay` | logical | Boolean flag indicating whether the highlighted play was a special teams play. |
| `highlight_play_playDirection` | character | Direction of the play (left, right, middle) as recorded in the NFL Shield tracking data. |
| `highlight_play_isBigPlay` | logical | Boolean flag indicating whether the play was classified as a 'big play' (e.g., long gain or turnover) by the NFL Shield API. |
| `highlight_play_isChangeOfPossession` | logical | Boolean flag indicating whether the highlighted play resulted in a change of possession. |
| `highlight_players` | integer | Serialized list or count of player tracking records associated with the highlight clip. |
| `highlight_season` | integer | NFL season year (e.g., 2024) in which the highlighted play occurred. |
| `highlight_seasonType` | character | Season phase of the highlighted play (e.g., 'REG' for regular season, 'POST' for playoffs). |
| `highlight_teamAbbr` | character | Team abbreviation of the team featured in or associated with the highlight clip. |
| `highlight_teamId` | character | NFL Shield team identifier for the team featured in the highlight clip. |
| `highlight_week` | integer | Week number within the season during which the highlighted play occurred. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_leaders, nfl_ngs_play_is_highlight
lead = nfl_ngs_leaders(category="speed", season=2024, season_type="REG")
gid, pid = lead["play_gameId"][0], lead["play_playId"][0]
hl = nfl_ngs_play_is_highlight(game_id=gid, play_id=pid)
hl.select(["gameId", "playId", "isHighlight"]).head()
```

### `nfl_ngs_ryoe(seasons: 'Union[int, Sequence[int]]', *, min_attempts: 'int' = 20, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nfl_ngs_ryoe}

Rush yards over expected per rusher-season, stabilised with EB shrinkage.

`ryoe_per_att_raw` is the NGS-shipped
`rush_yards_over_expected_per_att` passed through unchanged (the NGS
tracking-model residual); `ryoe_total` is the season total
`rush_yards_over_expected`. `ryoe_per_att_shrunk` applies per-season
Efron-Morris empirical-Bayes shrinkage toward the attempt-weighted league
mean, weighted by `rush_attempts`. `pct_stacked_box`
(`percent_attempts_gte_eight_defenders`) is reported as a context
covariate — v1 does not adjust on it. The prior is fit at call time on
rows with `rush_attempts >= min_attempts` — no bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, Sequence[int]]` |  | Season(s) to compute, 2016+. |
| `min_attempts` | `int` | `20` | Qualification threshold for the prior fit and for receiving a `ryoe_rank`. Defaults to `sportsdataverse.nfl.nfl_ngs_constants.MIN_ATTEMPTS`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |
| `_loader` | `Optional[Callable]` | `None` | Injectable loader for offline tests. |

**Returns**

One row per `(season, player_gsis_id)` with raw + shrunk RYOE/attempt, `reliability` in [0, 1], and a dense descending `ryoe_rank` over qualified rows (null for unqualified rows). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (YYYY). |
| `player_gsis_id` | character | Player GSIS identifier (nflverse id, e.g. "00-0036223"), pinned Utf8. |
| `player_display_name` | character | Player display name as shipped by NGS. |
| `team_abbr` | character | Team abbreviation. |
| `position` | character | Player position (from NGS player_position). |
| `rush_attempts` | double | Season rush attempts — the shrinkage weight. |
| `rush_yards` | double | Season rushing yards. |
| `expected_rush_yards` | double | NGS tracking-model expected rushing yards for the season. |
| `ryoe_total` | double | Season rush yards over expected (NGS rush_yards_over_expected, passed through). |
| `ryoe_per_att_raw` | double | NGS rush_yards_over_expected_per_att passed through unchanged — the tracking-model residual per attempt. |
| `ryoe_per_att_shrunk` | double | ryoe_per_att_raw after per-season empirical-Bayes shrinkage toward the attempt-weighted league mean (sampling variance identified from weekly rows). |
| `pct_stacked_box` | double | Percent of attempts against 8+ defenders in the box (NGS percent_attempts_gte_eight_defenders) — reported as a context covariate, not adjusted on. |
| `reliability` | double | Shrinkage reliability tau2 / (tau2 + sigma2 / rush_attempts) in [0, 1]; the fraction of the raw deviation retained. |
| `ryoe_rank` | integer | Dense descending rank of ryoe_per_att_shrunk within season over qualified rows; null when rush_attempts < min_attempts. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_ryoe
df = nfl_ngs_ryoe([2023])
print(df.sort("ryoe_rank").head())

# Pandas output

df_pd = nfl_ngs_ryoe(2023, return_as_pandas=True)
```

### `nfl_ngs_separation_oe(seasons: 'Union[int, Sequence[int]]', *, min_targets: 'int' = 20, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nfl_ngs_separation_oe}

Separation over a built context expectation, per receiver-season.

Unlike YAC-OE and RYOE, NGS ships no expected-separation field, so this
model BUILDS one: a per-season weighted ridge
(`sportsdataverse.nfl.nfl_ngs_constants.expected_separation_ridge`)
of `avg_separation` on `avg_cushion`, `avg_intended_air_yards` and
a position one-hot, weighted by `targets`. `sep_oe_raw` is the
residual — a CONTEXT residual (role/scheme proxies), not a
tracking-model expectation; treat it as descriptive, not causal.
`sep_oe_shrunk` applies the same per-season empirical-Bayes shrinkage
as the sibling models, weighted by `targets`. All parameters are fit
from the requested seasons at call time — no bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, Sequence[int]]` |  | Season(s) to compute, 2016+. |
| `min_targets` | `int` | `20` | Qualification threshold for the shrinkage prior and for receiving a `sep_oe_rank`. Defaults to `sportsdataverse.nfl.nfl_ngs_constants.MIN_TARGETS`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |
| `_loader` | `Optional[Callable]` | `None` | Injectable loader for offline tests. |

**Returns**

One row per `(season, player_gsis_id)` with the built `expected_separation`, raw + shrunk separation-over-expected, `reliability` in [0, 1], and a dense descending `sep_oe_rank` over qualified rows. Rows with null separation/cushion/air-yards inputs are dropped before the fit. Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (YYYY). |
| `player_gsis_id` | character | Player GSIS identifier (nflverse id, e.g. "00-0036223"), pinned Utf8. |
| `player_display_name` | character | Player display name as shipped by NGS. |
| `team_abbr` | character | Team abbreviation. |
| `position` | character | Player position (from NGS player_position); one-hot feature in the expectation ridge. |
| `targets` | double | Season target count — the ridge weight and the shrinkage weight. |
| `avg_cushion` | double | Average defender cushion at snap, yards (ridge feature). |
| `avg_separation` | double | Average separation from the nearest defender at catch/incompletion, yards. |
| `avg_intended_air_yards` | double | Average intended air yards on targets (ridge feature). |
| `expected_separation` | double | Built per-season weighted-ridge expectation of avg_separation from cushion, intended air yards and position — a CONTEXT expectation, not a tracking model. |
| `sep_oe_raw` | double | avg_separation minus expected_separation (context residual). |
| `sep_oe_shrunk` | double | sep_oe_raw after per-season empirical-Bayes shrinkage toward the target-weighted league mean (sampling variance identified from weekly rows). |
| `reliability` | double | Shrinkage reliability tau2 / (tau2 + sigma2 / targets) in [0, 1]; the fraction of the raw deviation retained. |
| `sep_oe_rank` | integer | Dense descending rank of sep_oe_shrunk within season over qualified rows; null when targets < min_targets. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_separation_oe
df = nfl_ngs_separation_oe([2023])
print(df.sort("sep_oe_rank").head())

# Pandas output

df_pd = nfl_ngs_separation_oe(2023, return_as_pandas=True)
```

### `nfl_ngs_statboard(stat_type: 'str' = 'passing', season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)` {#nfl_ngs_statboard}

NGS season/week statboard leaderboard for a stat family (one row per player).

Wraps `/api/statboard/{passing,receiving,rushing}`. Each record is a flat
per-player stat line (e.g. for passing: `completionPercentageAboveExpectation`,
`avgTimeToThrow`, `aggressiveness`, `passerRating` ...). The player's bio
is nested under a `player` object and is flattened to `player_*` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` | `'passing'` | one of `"passing"`, `"receiving"`, `"rushing"`. (For the cross-stat highlight board use `nfl_ngs_statboard_leaders`.) |
| `season` | `int` | `2024` | season year, e.g. `2024`. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | single week to filter to; `None` returns the full-season board. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per qualifying player.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `avgAirDistance` | double | Average air distance in yards on all pass attempts, measuring how far the ball travels through the air regardless of direction. |
| `avgAirYardsDifferential` | double | Average difference between intended air yards and completed air yards, measuring accuracy relative to target depth. |
| `avgAirYardsToSticks` | double | Average air yards relative to the first-down marker on pass attempts, where positive values indicate throws beyond the sticks. |
| `avgCompletedAirYards` | double | Average air yards on completed passes only, measuring depth of actual completions. |
| `avgIntendedAirYards` | double | Average depth of target on all pass attempts, regardless of completion. |
| `avgTimeToThrow` | double | Average time in seconds from snap to release for the passer, as tracked by NFL Next Gen Stats. |
| `completionPercentage` | double | Actual completion percentage for the passer during the period covered. |
| `completionPercentageAboveExpectation` | double | Completion percentage above the model-expected completion rate (CPAE), measuring accuracy relative to difficulty of throws attempted. |
| `completions` | integer | The number of completed passes. |
| `expectedCompletionPercentage` | double | Model-expected completion percentage based on factors such as target depth, separation, and defensive coverage as calculated by NFL Next Gen Stats. |
| `gamesPlayed` | integer | Number of games played by the passer during the period covered. |
| `interceptions` | integer | The number of interceptions thrown. |
| `maxAirDistance` | double | Maximum air distance in yards recorded on any single pass attempt during the period covered. |
| `maxCompletedAirDistance` | double | Maximum air distance in yards recorded on any single completed pass during the period covered. |
| `passTouchdowns` | integer | Total passing touchdowns thrown by the passer during the period covered. |
| `passYards` | integer | Total passing yards accumulated by the passer during the period covered. |
| `passerRating` | double | NFL passer rating (0–158.3 scale) for the passer during the period covered. |
| `playerName` | character | Display name of the passer as returned at the top-level statboard row. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Season segment covered by this statboard record (e.g., 'REG' for regular season, 'POST' for playoffs). |
| `position` | character | Primary position as reported by NFL.com |
| `teamId` | character | NFL Next Gen Stats team identifier for the passer's team at the time of this record. |
| `player_season` | integer | NFL season year associated with this player's statboard record. |
| `player_currentTeamId` | character | NFL Next Gen Stats team identifier for the team the player is currently rostered on. |
| `player_displayName` | character | Full display name of the player as used in NFL Next Gen Stats records. |
| `player_esbId` | character | ESPN-Elias Sports Bureau (ESB) identifier for the player. |
| `player_firstName` | character | First name of the player. |
| `player_footballName` | character | Football name used by the player, which may differ from the legal first name. |
| `player_gsisId` | character | NFL GSIS (Game Statistics and Information System) identifier, the primary nflverse player key. |
| `player_gsisItId` | integer | NFL GSIS internal tracking integer identifier for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_jerseyNumber` | integer | Jersey number worn by the player. |
| `player_lastName` | character | Last name of the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `player_positionGroup` | character | Broad position group for the player (e.g., 'QB', 'WR') in the NGS player record. |
| `player_shortName` | character | Shortened display name for the player (e.g., 'P.Mahomes'). |
| `player_smartId` | character | NFL Next Gen Stats smart (UUID-style) identifier for the player. |
| `player_status` | character | Current roster status of the player (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_uniformNumber` | character | Uniform number worn by the player, stored as a string to preserve leading zeros if applicable. |
| `player_ngsPosition` | character | Player's position as classified by the NFL Next Gen Stats system. |
| `player_ngsPositionGroup` | character | Broader position group the player belongs to as classified by the NFL Next Gen Stats system. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_statboard
qb = nfl_ngs_statboard(stat_type="passing", season=2024, season_type="REG")
qb.select(["playerName", "passerRating", "completionPercentageAboveExpectation"]).head()
```

### `nfl_ngs_statboard_leaders(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)` {#nfl_ngs_statboard_leaders}

NGS cross-stat "leaders" board, stacked long with a `category` column.

Wraps `/api/statboard/leaders`, which bundles several short top-N lists of
mixed shape (`fastestBallCarriers`, `fastestSacks`, `longestCompletions`,
`highestSeparation`, `rushYardsOverExpected`, `completionPctAboveExpected`,
`avgYACAboveExpected`). Each list is normalized separately and concatenated
diagonally (union of columns; missing cells become null), with a `category`
column recording which board each row came from.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | optional single-week filter. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` stacking every leader list, with a `category` column. Empty frame if no lists are present.

| col_name | type | description |
|---|---|---|
| `category` | character | Broader category of player positions |
| `leader_esbId` | character | ESB (NFL's Enterprise Subscriber Base) identifier for the player featured in the leader record. |
| `leader_firstName` | character | First name of the player featured in the leader record. |
| `leader_gsisId` | character | GSIS (Game Statistics and Information System) identifier for the player featured in the leader record. |
| `leader_jerseyNumber` | integer | Jersey number of the player featured in the leader record. |
| `leader_lastName` | character | Last name of the player featured in the leader record. |
| `leader_playerName` | character | Full display name of the player featured in the leader record. |
| `leader_position` | character | Position designation of the player featured in the leader record (e.g., 'QB', 'WR'). |
| `leader_positionGroup` | character | Broad position group of the player featured in the leader record (e.g., 'OFFENSE', 'DEFENSE'). |
| `leader_shortName` | character | Abbreviated display name of the player featured in the leader record (e.g., 'P.Mahomes'). |
| `leader_teamAbbr` | character | Team abbreviation of the player featured in the leader record (e.g., 'KC'). |
| `leader_teamId` | character | NFL Shield team identifier for the team of the player featured in the leader record. |
| `leader_week` | integer | Week number associated with the weekly leader record. |
| `leader_yards` | integer | Yardage total associated with the featured leader statistic for this player. |
| `leader_inPlayDist` | double | Distance (in yards) the player traveled while the ball was in play on the featured leader statistic. |
| `leader_maxSpeed` | double | Maximum speed (in yards per second or mph) recorded by the player on the featured leader statistic play. |
| `leader_headshot` | character | URL to the headshot image of the player featured in the leader record. |
| `play_gameId` | integer | NFL Shield API game identifier for the game this play belongs to. |
| `play_playId` | integer | NFL Shield API unique integer identifier for this play within the game. |
| `play_sequence` | integer | Sequential order integer of this play within the game as used by the NFL Shield API. |
| `play_down` | integer | Down number (1–4) at the time of this play. |
| `play_gameClock` | character | Game clock time remaining at the start of this play in MM:SS format. |
| `play_gameKey` | integer | NFL Shield internal game key integer identifying the game in internal API routing. |
| `play_health_playerTracking` | character | Data quality status string for player-tracking data on this play (e.g., 'GOOD', 'PARTIAL'). |
| `play_health_ballTracking` | character | Data quality status string for ball-tracking data on this play (e.g., 'GOOD', 'MISSING'). |
| `play_homeScore` | integer | Home team's score at the end of this play. |
| `play_isBigPlay` | logical | Boolean flag indicating whether this play was classified as a 'big play' by the NFL Shield API. |
| `play_isEndQuarter` | logical | Boolean flag indicating whether this play was the final play of a quarter. |
| `play_isGoalToGo` | logical | Boolean flag indicating whether this play occurred in a goal-to-go situation. |
| `play_isPenalty` | logical | Boolean flag indicating whether a penalty was assessed on this play. |
| `play_isSTPlay` | logical | Boolean flag indicating whether this play was a special teams play. |
| `play_isScoring` | logical | Boolean flag indicating whether this play resulted in a score. |
| `play_playDescription` | character | Official NFL text description of the play (e.g., '(2:11) J.Allen scrambles right end for 9 yards'). |
| `play_playState` | character | Status of the play record (e.g., 'FINAL', 'LIVE') in the NFL Shield API at time of capture. |
| `play_playStats` | integer | Serialized or encoded statistical detail for the play outcomes (e.g., yards, player IDs). |
| `play_playType` | character | Text label classifying the play type (e.g., 'PASS', 'RUSH', 'PUNT') from the NFL Shield API. |
| `play_playTypeCode` | integer | Integer code corresponding to the play type classification in the NFL Shield API. |
| `play_possessionTeamId` | character | NFL Shield team identifier for the team that had possession during this play. |
| `play_preSnapHomeScore` | integer | Home team's score immediately before the snap of this play. |
| `play_preSnapVisitorScore` | integer | Visiting team's score immediately before the snap of this play. |
| `play_quarter` | integer | Quarter number (1–4, or 5 for overtime) in which this play occurred. |
| `play_timeOfDayUTC` | character | Wall-clock timestamp in UTC at which this play occurred during the broadcast. |
| `play_visitorScore` | integer | Visiting team's score at the end of this play. |
| `play_yardline` | character | Human-readable yardline string (e.g., 'DAL 22') indicating where this play began. |
| `play_yardlineNumber` | integer | Numeric yard line (1–50 from nearest end zone) of the line of scrimmage at the snap. |
| `play_yardlineSide` | character | Team abbreviation indicating which team's side of the field the yardline is on. |
| `play_yardsToGo` | integer | Integer yards needed for a first down on this play. |
| `play_absoluteYardlineNumber` | integer | Absolute yard line number (1–100 from one end zone) of the line of scrimmage at the snap for this play. |
| `play_actualYardlineForFirstDown` | double | Actual yard line marker needed for a first down on this play. |
| `play_actualYardsToGo` | double | Exact decimal yards required for a first down on this play. |
| `play_endGameClock` | character | Game clock time remaining at the end of this play in MM:SS format. |
| `play_isChangeOfPossession` | logical | Boolean flag indicating whether this play resulted in a change of possession. |
| `play_playDirection` | character | Direction of the play (left, right, middle) as recorded in NFL Shield tracking data. |
| `play_startGameClock` | character | Game clock time remaining at the moment this play began, in MM:SS format. |
| `leader_time` | double | Elapsed time value associated with the leader record, such as time of possession or time-to-throw. |
| `leader_seasonAvg` | double | Season-to-date average of the featured leader statistic for this player. |
| `leader_teamAvg` | double | Team-level average of the featured statistic across all players on the team. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `avgAirDistance` | double | Average air distance (in yards) the ball traveled on all pass attempts, from NFL Next Gen Stats. |
| `avgAirYardsDifferential` | double | Average difference between intended air yards and completed air yards per attempt, indicating whether the passer throws short or long relative to the targeted depth. |
| `avgAirYardsToSticks` | double | Average air yards relative to the first-down marker on pass attempts, from NFL Next Gen Stats (negative = short of sticks, positive = past sticks). |
| `avgCompletedAirYards` | double | Average air yards on completed passes only, from NFL Next Gen Stats. |
| `avgIntendedAirYards` | double | Average intended air yards per pass attempt (including incompletions), from NFL Next Gen Stats. |
| `avgTimeToThrow` | double | Average time in seconds from snap to throw for the passer, from NFL Next Gen Stats. |
| `completionPercentage` | double | Percentage of pass attempts completed by the passer, from NFL Next Gen Stats statboard leaders. |
| `completionPercentageAboveExpectation` | double | Passer's actual completion percentage minus their expected completion percentage based on target depth, coverage, and game context (CPOE), from NFL Next Gen Stats. |
| `completions` | integer | The number of completed passes. |
| `expectedCompletionPercentage` | double | Model-predicted completion percentage for the passer based on depth of target, receiver separation, and coverage, from NFL Next Gen Stats. |
| `gamesPlayed` | integer | Number of games played by the player in the given season or time period. |
| `interceptions` | integer | The number of interceptions thrown. |
| `maxAirDistance` | double | Maximum air distance (in yards) recorded on a single pass attempt by the passer, from NFL Next Gen Stats. |
| `maxCompletedAirDistance` | double | Maximum air distance on a completed pass for the passer, from NFL Next Gen Stats. |
| `passTouchdowns` | integer | Total passing touchdowns recorded by the player in the given period. |
| `passYards` | integer | Total passing yards recorded by the player in the given period. |
| `passerRating` | double | NFL passer rating (0–158.3 scale) for the quarterback in the given period. |
| `playerName` | character | Full display name of the player associated with this NGS statboard leader record. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Phase of the season for this record (e.g., 'REG' for regular season, 'POST' for playoffs). |
| `position` | character | Primary position as reported by NFL.com |
| `teamId` | character | NFL Shield team identifier for the team associated with this leader statistic. |
| `player_season` | integer | NFL season year in which this player record is valid. |
| `player_currentTeamId` | character | NFL Shield team identifier for the player's current team. |
| `player_displayName` | character | Player's full display name as used by NFL Next Gen Stats (e.g., 'Patrick Mahomes'). |
| `player_esbId` | character | ESB (Enterprise Subscriber Base) identifier assigned to this player by the NFL. |
| `player_firstName` | character | Player's first name as recorded in the NFL Next Gen Stats player roster. |
| `player_footballName` | character | Player's preferred football name (may differ from legal first name) used on the field and in broadcasts. |
| `player_gsisId` | character | GSIS (Game Statistics and Information System) identifier, the primary NFL player identifier used across nflverse datasets. |
| `player_gsisItId` | integer | GSIS integrated tracking identifier, an alternate integer-form player ID used in the NFL Shield tracking system. |
| `player_jerseyNumber` | integer | Player's jersey number as worn on the field. |
| `player_lastName` | character | Player's last name as recorded in the NFL Next Gen Stats player roster. |
| `player_position` | character | Position of the player accordinng to NGS |
| `player_positionGroup` | character | Official NFL position group for the player (e.g., 'OFFENSE', 'DEFENSE', 'SPECIAL_TEAMS'). |
| `player_shortName` | character | Abbreviated player name used in display contexts (e.g., 'P.Mahomes'). |
| `player_status` | character | Player's current roster status (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_uniformNumber` | character | Player's uniform number as a string, matching what appears on the jersey. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_smartId` | character | NFL Smart ID — a system-agnostic unique identifier for the player used across NFL Shield systems. |
| `player_ngsPosition` | character | Player's position as classified by NFL Next Gen Stats (may differ from official NFL position; e.g., NGS uses 'ILB' vs 'LB'). |
| `player_ngsPositionGroup` | character | Broad position group assigned by NFL Next Gen Stats (e.g., 'QB', 'WR', 'DB', 'DL'). |
| `avgCushion` | double | Average distance (in yards) between the receiver and the nearest defender at the snap, from NFL Next Gen Stats receiver tracking. |
| `avgExpectedYAC` | double | Average yards after catch expected based on game situation and receiver location, from NFL Next Gen Stats models. |
| `avgSeparation` | double | Average separation (in yards) between the receiver and the nearest defender at the moment of catch or incompletion, from NFL Next Gen Stats. |
| `avgYAC` | double | Average yards after catch per reception, from NFL Next Gen Stats receiver tracking. |
| `avgYACAboveExpectation` | double | Average yards after catch above statistical expectation (YAC - expected YAC), from NFL Next Gen Stats models. |
| `catchPercentage` | double | Percentage of targets caught by the receiver, from NFL Next Gen Stats statboard leaders. |
| `percentShareOfIntendedAirYards` | double | Receiver's share (as a percentage) of the team's total intended air yards, from NFL Next Gen Stats. |
| `recTouchdowns` | integer | Total receiving touchdowns recorded by the player in the given period. |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `yards` | integer | The number of receiving yards |
| `avgTimeToLos` | double | Average time (in seconds) for the ball carrier to reach the line of scrimmage on rush attempts, from NFL Next Gen Stats. |
| `expectedRushYards` | double | Model-predicted rushing yards based on field position, personnel, and pre-snap alignment, from NFL Next Gen Stats. |
| `rushAttempts` | integer | Total rushing attempts by the player in the given period. |
| `rushPctOverExpected` | double | Percentage of rush attempts on which the player gained more yards than the model-predicted expectation, from NFL Next Gen Stats. |
| `rushTouchdowns` | integer | Total rushing touchdowns recorded by the player in the given period. |
| `rushYards` | integer | Total rushing yards recorded by the player in the given period. |
| `rushYardsOverExpected` | double | Total rushing yards gained above statistical expectation (RYOE) in the given period, from NFL Next Gen Stats. |
| `rushYardsOverExpectedPerAtt` | double | Average rushing yards over expected per attempt (RYOE/attempt), from NFL Next Gen Stats. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_statboard_leaders
bd = nfl_ngs_statboard_leaders(season=2024, season_type="REG")
bd["category"].unique().to_list()
```

### `nfl_ngs_yac_oe(seasons: 'Union[int, Sequence[int]]', *, min_receptions: 'int' = 10, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'` {#nfl_ngs_yac_oe}

YAC over expected per receiver-season, stabilised with EB shrinkage.

`yac_oe_raw` is the NGS-shipped `avg_yac_above_expectation` passed
through unchanged (per-reception yards after catch minus the NGS
tracking-model expectation). `yac_oe_shrunk` applies per-season
Efron-Morris empirical-Bayes shrinkage toward the reception-weighted
league mean, weighted by `receptions`, so small-sample extremes are
pulled in. The shrinkage prior is fit at call time on rows with
`receptions >= min_receptions` — no bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, Sequence[int]]` |  | Season(s) to compute, 2016+. |
| `min_receptions` | `int` | `10` | Qualification threshold for the prior fit and for receiving a `yac_oe_rank`. Defaults to `sportsdataverse.nfl.nfl_ngs_constants.MIN_RECEPTIONS`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |
| `_loader` | `Optional[Callable]` | `None` | Injectable loader for offline tests. |

**Returns**

One row per `(season, player_gsis_id)` with raw + shrunk YAC-OE, `reliability` in [0, 1], and a dense descending `yac_oe_rank` over qualified rows (null for unqualified rows). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (YYYY). |
| `player_gsis_id` | character | Player GSIS identifier (nflverse id, e.g. "00-0036223"), pinned Utf8. |
| `player_display_name` | character | Player display name as shipped by NGS. |
| `team_abbr` | character | Team abbreviation. |
| `position` | character | Player position (from NGS player_position). |
| `receptions` | double | Season reception count — the shrinkage weight. |
| `avg_yac` | double | Average yards after catch per reception. |
| `avg_expected_yac` | double | NGS tracking-model expected YAC per reception. |
| `yac_oe_raw` | double | NGS avg_yac_above_expectation passed through unchanged — per-reception YAC minus the tracking-model expectation. |
| `yac_oe_shrunk` | double | yac_oe_raw after per-season empirical-Bayes shrinkage toward the reception-weighted league mean (sampling variance identified from weekly rows). |
| `reliability` | double | Shrinkage reliability tau2 / (tau2 + sigma2 / receptions) in [0, 1]; the fraction of the raw deviation retained. |
| `yac_oe_rank` | integer | Dense descending rank of yac_oe_shrunk within season over qualified rows; null when receptions < min_receptions. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_yac_oe
df = nfl_ngs_yac_oe([2023])
print(df.sort("yac_oe_rank").head())

# Pandas output

df_pd = nfl_ngs_yac_oe(2023, return_as_pandas=True)
```

### `nfl_play_call_probabilities(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None, *, models_dir: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_play_call_probabilities}

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
| `game_id` | character | nflverse game identifier (Utf8 join key). |
| `play_id` | integer | nflverse play identifier within the game (Int64 join key). |
| `season` | integer | Season of the play. |
| `week` | integer | Week of the play. |
| `posteam` | character | Offense (possession) team abbreviation. |
| `p_inside_run` | double | Predicted probability of an inside run (guard/center gap or middle). |
| `p_outside_run` | double | Predicted probability of an outside run (end/tackle or off-middle). |
| `p_short_pass` | double | Predicted probability of a short pass. |
| `p_deep_pass` | double | Predicted probability of a deep pass. |
| `p_scramble` | double | Predicted probability of a QB scramble. |
| `p_pass` | double | Predicted pass probability (short + deep + scramble family sum). |
| `pred_family` | character | Argmax family among the five class probabilities. |
| `pass_oe_model` | double | Pass-rate over model expectation for the play, 100 * (is_pass - p_pass). |

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

### `nfl_play_call_tendencies(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None, *, models_dir: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_play_call_tendencies}

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
| `season` | integer | Season of the aggregate. |
| `posteam` | character | Offense team abbreviation. |
| `plays` | integer | Offensive run/pass plays scored. |
| `mean_p_pass` | double | Mean model pass probability across the team's plays. |
| `pass_rate` | double | Actual pass rate (scrambles count as passes). |
| `proe` | double | Pass rate over expected, 100 * (pass_rate - mean_p_pass). |
| `share_inside_run` | double | Share of plays labeled inside_run. |
| `share_outside_run` | double | Share of plays labeled outside_run. |
| `share_short_pass` | double | Share of plays labeled short_pass. |
| `share_deep_pass` | double | Share of plays labeled deep_pass. |
| `share_scramble` | double | Share of plays labeled scramble. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_playcall import nfl_play_call_tendencies
t = nfl_play_call_tendencies(calculate_xpass(load_nfl_pbp([2023])))
print(t.sort("proe", descending=True).head())
```

### `nfl_player_projection(seasons: 'List[int]', target_season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_player_projection}

Marcel-style next-season player projection with delta-method aging.

Loads weekly player stats + rosters, aggregates to season rates, and for
every player visible in seasons **strictly before** `target_season`
(the as-of-date leakage boundary) produces a recency-weighted rate blend
regressed toward the volume-weighted position mean by
`k / (k + reliability)`, scaled by the position aging-curve ratio
`aging_mult(proj_age) / aging_mult(current_age)`. The aging curve is fit
only on the same pre-target history.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load (seasons `>= target_season` are discarded by the leakage split). |
| `target_season` | `int` |  | The season being projected. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

One row per projected player: `player_id:Utf8, target_season:Int64, position_group:Utf8, proj_age:Float64, proj_ppg:Float64, proj_volume:Float64, proj_games:Float64, aging_mult:Float64, reliability:Float64` plus `proj_<stat>_rate` component-rate columns. Empty history returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only - the as-of-date leakage boundary). |
| `position_group` | character | nflverse offensive position group (QB/RB/WR/TE plus fringe groups). |
| `proj_age` | double | Projected age at the target season (age at last visible season + season gap). |
| `proj_ppg` | double | Projected PPR fantasy points per game - recency-weighted rate blend regressed toward the volume-weighted position mean by k/(k + reliability), scaled by the damped aging-curve ratio. |
| `proj_volume` | double | Projected position-specific opportunity volume (QB = pass attempts, RB = carries + targets, WR/TE = targets). |
| `proj_games` | double | Recency-weighted mean of historical games played. |
| `aging_mult` | double | Applied aging multiplier - the damped, clamped ratio aging_curve(proj_age) / aging_curve(current_age). |
| `reliability` | double | Recency-weighted volume sum - the shrinkage evidence weight. |
| `proj_completions_rate` | double | Projected per-game pass completions (Marcel blend x aging ratio). |
| `proj_attempts_rate` | double | Projected per-game pass attempts (Marcel blend x aging ratio). |
| `proj_passing_yards_rate` | double | Projected per-game passing yards (Marcel blend x aging ratio). |
| `proj_passing_tds_rate` | double | Projected per-game passing touchdowns (Marcel blend x aging ratio). |
| `proj_interceptions_rate` | double | Projected per-game interceptions thrown (Marcel blend x aging ratio). |
| `proj_carries_rate` | double | Projected per-game rush attempts (Marcel blend x aging ratio). |
| `proj_rushing_yards_rate` | double | Projected per-game rushing yards (Marcel blend x aging ratio). |
| `proj_rushing_tds_rate` | double | Projected per-game rushing touchdowns (Marcel blend x aging ratio). |
| `proj_receptions_rate` | double | Projected per-game receptions (Marcel blend x aging ratio). |
| `proj_targets_rate` | double | Projected per-game targets (Marcel blend x aging ratio). |
| `proj_receiving_yards_rate` | double | Projected per-game receiving yards (Marcel blend x aging ratio). |
| `proj_receiving_tds_rate` | double | Projected per-game receiving touchdowns (Marcel blend x aging ratio). |
| `proj_receiving_air_yards_rate` | double | Projected per-game receiving air yards (Marcel blend x aging ratio). |
| `proj_fumbles_lost_rate` | double | Projected per-game fumbles lost (Marcel blend x aging ratio). |

**Example**

```python
from sportsdataverse.nfl.nfl_projection import nfl_player_projection
proj = nfl_player_projection([2021, 2022, 2023], 2024)
proj.sort("proj_ppg", descending=True).head()

# Pandas round-trip

proj_pd = nfl_player_projection([2021, 2022, 2023], 2024, return_as_pandas=True)
```

### `nfl_player_props(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, era: 'str' = 'modern', lines: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#nfl_player_props}

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
| `season` | integer | Season the projection belongs to. |
| `week` | integer | Week of the projected game; only weeks strictly before it feed the projection. |
| `game_id` | character | Game identifier from the schedule (nflverse id, e.g. "2023_06_DET_TB"). |
| `player_id` | character | nflverse GSIS player identifier (character join key). |
| `position` | character | Player position (QB, RB, WR, TE) selecting the projected stat family. |
| `team_id` | character | Player's team nflverse abbreviation as of the projection week. |
| `opp_team_id` | character | Opponent team nflverse abbreviation (drives the matchup multiplier). |
| `stat` | character | Projected stat name (passing_yards for QB, rushing_yards for RB, receiving_yards for WR/TE). |
| `proj_mean` | double | Projected stat mean - EB-shrunk usage x efficiency x opponent matchup x game-script. |
| `proj_sd` | double | Residual standard deviation for the stat family (fitted on the 2023 as-of backtest). |
| `line` | double | Market prop line joined from the caller-supplied lines frame (e.g. espn_nfl_game_propbets); null when no line is available. |
| `p_over` | double | Probability the player exceeds `line`, 1 - Phi((line - proj_mean) / proj_sd); null without a line. |

**Example**

```python
from sportsdataverse.nfl import nfl_player_props
props = nfl_player_props(2023)
props.filter(props["stat"] == "passing_yards").head()

# Upcoming-only, as-of a date

import datetime as dt
props = nfl_player_props(2024, as_of_date=dt.date(2024, 11, 1))
```

### `nfl_players_crosswalk(*, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_players_crosswalk}

Pure-consumer ID crosswalk sliced from `load_nfl_players`.

Reads nflverse's published players master and projects it down to just the
cross-system identifier columns it carries (`gsis_id`, `esb_id`,
`espn_id`, `pfr_id`, `pff_id`, `otc_id`, `nfl_id`, `smart_id` —
whichever the parquet exposes) plus `full_name` and `position`, deduped
on `gsis_id`. It is a convenience for joining nflverse identity IDs onto
PBP / rosters / stats frames without carrying the full ~40-column master.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

A one-row-per-`gsis_id` `DataFrame` of cross-system IDs + `full_name` / `position`. A failed / empty players load yields a zero-row frame carrying the same column set (never a raise).

**Example**

```python
from sportsdataverse.nfl import nfl_players_crosswalk
xwalk = nfl_players_crosswalk()
print(xwalk.columns)

# Join nflverse IDs onto a PBP frame (one line)

pbp.join(nfl_players_crosswalk(), left_on="passer_player_id", right_on="gsis_id", how="left")
```

### `nfl_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, era: 'str' = 'modern', odds: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#nfl_predict_games}

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
| `odds` | `DataFrame \| None` | `None` | Optional market frame (`game_id`, `close_spread_home` -- the market's expected home margin, positive = home favored). Games absent from `odds` get a null `market_edge`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

One row per input game: `game_id` / `home_team_id` / `away_team_id` (Utf8), `neutral_site` (Boolean), `exp_margin` / `home_win_prob` / `exp_total` / `market_edge` (Float64; `market_edge` null without odds). Zero-row, correctly-typed on empty input.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier carried through from the input schedule. |
| `home_team_id` | character | Home team nflverse abbreviation (character; the ratings `team_id` join key). |
| `away_team_id` | character | Away team nflverse abbreviation (character; the ratings `team_id` join key). |
| `neutral_site` | logical | Whether the game is at a neutral site (home-field advantage is dropped when true). |
| `exp_margin` | double | Expected home scoring margin in points (points_per_net * net rating differential + the fitted home-field advantage on non-neutral fields). |
| `home_win_prob` | double | Home win probability, Phi(exp_margin / margin_sd) under a Gaussian margin model. |
| `exp_total` | double | Expected combined point total (avg_total + total_scale * the four-way efficiency matchup sum). |
| `market_edge` | double | Display-only native-minus-market spread edge (exp_margin - close_spread_home); null when no odds frame is supplied. |

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

### `nfl_punter_value(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_punter_value}

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
| `season` | integer | Season of the aggregate. |
| `punter_player_id` | character | nflverse punter GSIS id (Utf8 join key). |
| `punts` | integer | Punts with a recorded kick distance. |
| `gross_avg` | double | Mean gross punt distance (yards). |
| `net_avg` | double | Mean net distance, kick_distance - return_yards - 20 * touchback. |
| `exp_net_avg` | double | Mean expected net from the shipped punt landing distribution at each punt's line of scrimmage. |
| `net_over_expected` | double | net_avg minus exp_net_avg (yards of field position per punt over expectation). |
| `epa` | double | Total EPA on the punter's punt plays (kicking-team perspective). |

**Example**

```python
from sportsdataverse.nfl.nfl_special_teams import nfl_punter_value
pv = nfl_punter_value([2023])
print(pv.head())
```

### `nfl_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#nfl_ratings}

One row per team: the native NFL ratings spine (off/def/ST EPA).

Public orchestrator over `efficiency_ratings` +
`special_teams_ratings`. Loads play-by-play + schedule via
`load_nfl_pbp` / `load_nfl_schedule`, joins each game's `gameday`
onto the plays, optionally applies the as-of-date leakage boundary
(only plays from games with `gameday < as_of_date` are used), then
fits both components and reshapes into one wide per-team table with
dense ranks and a net z-score.

The loaded pbp is down-selected to the ridge columns *before* any fit so
no market column (`spread_line` / `vegas_wp`) can leak into the
ratings (the binding non-market boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (e.g. `2023`) or a list of seasons pooled into one combined fit. |
| `as_of_date` | `date \| None` | `None` | When given, only plays from games strictly before this date are used (mirrors what was knowable heading into that date). `None` (default) uses the full season(s). |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs forwarded to both component fits; defaults to `RatingsConfig`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

A DataFrame with one row per `team_id`: `season` (Int64 -- the single passed season, `null` for a pooled multi-season call), `team_id` (Utf8), `adj_off_epa` / `adj_def_epa` / `adj_st_epa` / `adj_net` (Float64; `adj_net` is offense minus defense -- special teams stays a separate column), `games` (Int64), `off_rank` / `def_rank` / `net_rank` (Int64; `def_rank` ascends -- fewer EPA allowed ranks better), `net_z` (Float64). Zero-row, correctly-typed when the seasons have no data or `as_of_date` filters out every play.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the ratings cover (null for a pooled multi-season fit). |
| `team_id` | character | nflverse team abbreviation (character join key, e.g. "KC"). |
| `adj_off_epa` | double | Opponent-adjusted offensive EPA per play (higher is better); competitive-play ridge fit. |
| `adj_def_epa` | double | Opponent-adjusted defensive EPA allowed per play (lower is better); competitive-play ridge fit. |
| `adj_st_epa` | double | Opponent-adjusted special-teams EPA per play (ridge on special==1 plays; 0.0 for teams with no special-teams plays in the window). |
| `adj_net` | double | Opponent-adjusted net efficiency (adj_off_epa minus adj_def_epa; special teams not folded in). |
| `games` | integer | Number of games the team played in the fitted window. |
| `off_rank` | integer | Dense rank on adj_off_epa descending (best offense = 1). |
| `def_rank` | integer | Dense rank on adj_def_epa ascending (fewer EPA allowed ranks better). |
| `net_rank` | integer | Dense rank on adj_net descending (best net rating = 1). |
| `net_z` | double | Z-score of adj_net across the 32 teams. |

**Example**

```python
from sportsdataverse.nfl import nfl_ratings
ratings = nfl_ratings(2023)
ratings.sort("net_rank").head()

# As-of-date leakage boundary

import datetime as dt
week6 = nfl_ratings(2023, as_of_date=dt.date(2023, 10, 12))
```

### `nfl_season_standings(games: 'pl.DataFrame', *, ranks: 'str' = 'CONF', tiebreaker_depth: 'str' = 'SOS', playoff_seeds: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_season_standings}

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
| `season` | integer | Season identifier from the input games frame (named `sim` instead when the input used a `sim` column). |
| `conf` | character | Conference of the team (AFC or NFC). |
| `division` | character | Division of the team (e.g. "AFC East"). |
| `team` | character | Team abbreviation. |
| `games` | integer | Number of regular season games played. |
| `wins` | double | Regular season wins with ties counted as half a win. |
| `true_wins` | integer | Regular season wins excluding ties (outright wins only). |
| `losses` | integer | Regular season losses. |
| `ties` | integer | Regular season ties. |
| `pf` | integer | Points scored across regular season games (points for); present only when the input carries home_score and away_score. |
| `pa` | integer | Points allowed across regular season games (points against); present only when the input carries scores. |
| `pd` | integer | Regular season point differential (pf minus pa); present only when the input carries scores. |
| `win_pct` | double | Regular season win percentage with ties counted as half a win. |
| `div_pct` | double | Win percentage in games against division opponents (0 when the team played no division games). |
| `conf_pct` | double | Win percentage in games against conference opponents (0 when the team played no conference games). |
| `sov` | double | Strength of victory - combined win percentage of all opponents the team defeated (0 for winless teams). |
| `sos` | double | Strength of schedule - combined win percentage of all opponents the team faced. |
| `div_rank` | integer | Rank within the division (1-4) after applying the NFL division tiebreaking procedures. |
| `div_tie_broken_by` | character | Tiebreaker step that resolved the team's division rank (e.g. "Head-To-Head Win PCT (2)" or "Coin Toss"); null when the rank needed no tiebreaker. |
| `conf_rank` | integer | Conference rank, i.e. the playoff seed, after applying the NFL conference tiebreaking procedures; null beyond `playoff_seeds` when that argument is set. |
| `conf_tie_broken_by` | character | Tiebreaker step that resolved the team's conference rank; null when the rank needed no tiebreaker. |
| `exit` | character | Round of the team's final game - REG, WC, DIV, CON, SB, or SB_WIN for the Super Bowl winner (returned with ranks="DRAFT"). |
| `draft_rank` | integer | Draft pick position (1 = first overall pick) derived from postseason exit, win percentage, SOS and the draft tiebreaking procedures (returned with ranks="DRAFT"). |
| `draft_tie_broken_by` | character | Tiebreaker step that resolved the team's draft rank; null when the rank needed no tiebreaker. |

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

### `nfl_simulations(games: 'pl.DataFrame', compute_results: 'Optional[ComputeResultsFn]' = None, *, simulations: 'int' = 10000, playoff_seeds: 'int' = 7, byes_per_conf: 'int' = 1, tiebreaker_depth: 'str' = 'SOS', sim_include: 'str' = 'DRAFT', seed: 'Optional[int]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Dict[str, Union[pl.DataFrame, 'pd.DataFrame']]"` {#nfl_simulations}

Simulate an NFL season from a schedule with (partially) missing results.

Faithful port of `nflseedR::nfl_simulations()` +
`simulate_chunk()` (simulations.R L140-409,
simulations_simulate_chunks.R L1-284). Missing regular season results
are filled week by week via `compute_results`; standings, division
ranks and playoff seeds are then computed with the full NFL tiebreakers,
the postseason is simulated round by round (with reseeding and
`byes_per_conf` byes), and the draft order is derived. nflseedR's
furrr chunking is replaced by one vectorized pass over all simulated
seasons, so there is no `chunks` argument; reproducibility comes from
`seed`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Schedule frame for ONE season with columns `sim` or `season`, `game_type`, `week`, `away_team`, `home_team`, `away_rest`, `home_rest`, `location`, and `result` (home margin; missing = not yet played). |
| `compute_results` | `Optional[ComputeResultsFn]` | `None` | Function filling results for one week, called as `compute_results(teams, games, week_num, rng=rng, **kwargs)` and returning `{"teams": ..., "games": ...}`. Defaults to `nfl_compute_results` (dynamic ELO + Normal(estimate, 13) margins). Must only fill results where `week == week_num` and `result` is missing, and must not produce postseason ties. |
| `simulations` | `int` | `10000` | Number of seasons to simulate. |
| `playoff_seeds` | `int` | `7` | Number of playoff seeds per conference. |
| `byes_per_conf` | `int` | `1` | First-round byes per conference (drives the number of wildcard games). |
| `tiebreaker_depth` | `str` | `'SOS'` | `'SOS'` (default), `'PRE-SOV'`, or `'RANDOM'` (`'POINTS'` is unavailable because simulated games carry margins, not scores). |
| `sim_include` | `str` | `'DRAFT'` | `'REG'` (standings/seeds only), `'POST'` (+ postseason), or `'DRAFT'` (default; + draft order). |
| `seed` | `Optional[int]` | `None` | Seed for the numpy RNG driving results and coin tosses. |
| `return_as_pandas` | `bool` | `False` | If `True`, return pandas DataFrames. |

**Returns**

Dict of frames mirroring the nflseedR simulation list: `standings` (one row per sim x team), `games` (all simulated games), `overall` (per-team probabilities: wins, playoff, div1, seed1, won_conf, won_sb, draft1, draft5), `team_wins` (over/under probabilities vs. half-win lines), and `game_summary` (per-matchup home/away win rates).

| col_name | type | description |
|---|---|---|
| `standings.sim` | integer | Simulated season identifier (1 through `simulations`). |
| `standings.conf` | character | Conference of the team (AFC or NFC). |
| `standings.division` | character | Division of the team (e.g. "AFC East"). |
| `standings.team` | character | Team abbreviation. |
| `standings.games` | integer | Number of regular season games played in the simulated season. |
| `standings.wins` | double | Regular season wins in the simulated season with ties counted as half a win. |
| `standings.true_wins` | integer | Regular season wins in the simulated season excluding ties. |
| `standings.losses` | integer | Regular season losses in the simulated season. |
| `standings.ties` | integer | Regular season ties in the simulated season. |
| `standings.win_pct` | double | Regular season win percentage in the simulated season with ties counted as half a win. |
| `standings.div_pct` | double | Win percentage against division opponents in the simulated season (0 when no division games). |
| `standings.conf_pct` | double | Win percentage against conference opponents in the simulated season (0 when no conference games). |
| `standings.sov` | double | Strength of victory in the simulated season - combined win percentage of all defeated opponents. |
| `standings.sos` | double | Strength of schedule in the simulated season - combined win percentage of all opponents faced. |
| `standings.div_rank` | integer | Division rank (1-4) in the simulated season after the NFL division tiebreakers. |
| `standings.div_tie_broken_by` | character | Tiebreaker step that resolved the division rank in this simulated season; null when no tiebreaker was needed. |
| `standings.conf_rank` | integer | Conference rank (playoff seed) in the simulated season after the NFL conference tiebreakers; null beyond `playoff_seeds`. |
| `standings.conf_tie_broken_by` | character | Tiebreaker step that resolved the conference rank in this simulated season; null when no tiebreaker was needed. |
| `standings.exit` | character | Round of the team's final game in the simulated season - REG, WC, DIV, CON, SB, or SB_WIN for the Super Bowl winner. |
| `standings.draft_rank` | integer | Draft pick position (1 = first overall) in the simulated season (present when sim_include="DRAFT"). |
| `standings.draft_tie_broken_by` | character | Tiebreaker step that resolved the draft rank in this simulated season; null when no tiebreaker was needed. |
| `games.sim` | integer | Simulated season identifier the game row belongs to. |
| `games.game_type` | character | Game type - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `games.week` | integer | Week number of the game; simulated playoff rounds are numbered from the last regular season week (+1 for WC through +4 for SB). |
| `games.away_team` | character | Team abbreviation of the away team (simulated playoff matchups are filled by seed). |
| `games.home_team` | character | Team abbreviation of the home team (simulated playoff matchups are filled by seed). |
| `games.away_rest` | integer | Days of rest for the away team before the game. |
| `games.home_rest` | integer | Days of rest for the home team before the game (14 for the top seed's divisional round game). |
| `games.location` | character | Game site indicator - "Home" or "Neutral" (Super Bowl). |
| `games.result` | integer | Home margin (home score minus away score); real where the input schedule had one, simulated otherwise. |
| `overall.conf` | character | Conference of the team (AFC or NFC). |
| `overall.division` | character | Division of the team (e.g. "AFC East"). |
| `overall.team` | character | Team abbreviation. |
| `overall.wins` | double | Mean regular season wins across all simulated seasons (ties counted as half a win). |
| `overall.playoff` | double | Share of simulated seasons in which the team made the playoffs (conference rank within `playoff_seeds`). |
| `overall.div1` | double | Share of simulated seasons in which the team won its division. |
| `overall.seed1` | double | Share of simulated seasons in which the team earned the conference number one seed. |
| `overall.won_conf` | double | Share of simulated seasons in which the team won the conference championship; null when sim_include="REG". |
| `overall.won_sb` | double | Share of simulated seasons in which the team won the Super Bowl; null when sim_include="REG". |
| `overall.draft1` | double | Share of simulated seasons in which the team held the first overall draft pick; null unless sim_include="DRAFT". |
| `overall.draft5` | double | Share of simulated seasons in which the team held a top-five draft pick; null unless sim_include="DRAFT". |
| `team_wins.team` | character | Team abbreviation. |
| `team_wins.wins` | double | Half-win line the over/under probabilities are evaluated against (0, 0.5, ... up to the number of regular season games). |
| `team_wins.over_prob` | double | Probability across simulated seasons that the team's outright win total exceeds the line. |
| `team_wins.under_prob` | double | Probability across simulated seasons that the team's outright win total falls below the line (exact pushes are the remainder). |
| `game_summary.game_type` | character | Game type of the matchup - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `game_summary.week` | integer | Week number of the matchup. |
| `game_summary.away_team` | character | Team abbreviation of the away team in the matchup. |
| `game_summary.home_team` | character | Team abbreviation of the home team in the matchup. |
| `game_summary.away_wins` | integer | Number of simulated seasons in which the away team won the matchup. |
| `game_summary.home_wins` | integer | Number of simulated seasons in which the home team won the matchup. |
| `game_summary.ties` | integer | Number of simulated seasons in which the matchup ended in a tie. |
| `game_summary.result` | double | Mean home margin of the matchup across the simulated seasons in which it was played. |
| `game_summary.games_played` | integer | Number of simulated seasons in which this exact matchup occurred (playoff pairings only arise in the simulations that produce them). |
| `game_summary.away_percentage` | double | Share of played simulations won by the away team, with ties counted as half a win. |
| `game_summary.home_percentage` | double | Share of played simulations won by the home team, with ties counted as half a win. |

**Example**

```python
import sportsdataverse.nfl as nfl
games = nfl.load_schedules([2024])
sim = nfl.nfl_simulations(games, simulations=1000, seed=42)
print(sim["overall"].head())

# Custom initial ELO ratings

sim = nfl.nfl_simulations(games, simulations=500, seed=1,
                          elo={"KC": 1700, "BUF": 1650})

# Pipeline next step (one line)

sim["overall"].sort("won_sb", descending=True).head()
```

### `nfl_special_teams_epa(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_special_teams_epa}

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
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `unit` | character | Special-teams unit (punt, punt_return, kickoff, kickoff_return, field_goal, extra_point). |
| `plays` | integer | Plays credited to the unit. |
| `epa` | double | Total EPA credited to the unit (kicking team carries the play EPA signed to it; the return team carries its negation). |
| `epa_per_play` | double | EPA per play for the unit. |

**Example**

```python
from sportsdataverse.nfl.nfl_special_teams import nfl_special_teams_epa
st = nfl_special_teams_epa([2023])
print(st.filter(pl.col("unit") == "punt").sort("epa", descending=True).head())
```

### `nfl_token_gen(client_key: 'Optional[str]' = None, client_secret: 'Optional[str]' = None, force_refresh: 'bool' = False) -> 'str'` {#nfl_token_gen}

Return a valid `api.nfl.com` bearer token, minting + caching as needed.

The token is cached in-process and reused until ~2 min before its own JWT
`exp`, then transparently re-minted -- so callers never have to think about
expiry or refresh. The first call (or any call after expiry / `force_refresh`)
mints a fresh token via the anonymous device-token grant at `/identity/v3/token`.

Resolution order (all overrides optional):

1. `NFL_ACCESS_TOKEN` env var -- returned verbatim, skipping minting and
   caching (you supply + manage the token). Ignored if explicit credentials
   are passed.
2. Credentials: explicit `client_key`/`client_secret` args ->
   `NFL_CLIENT_KEY`/`NFL_CLIENT_SECRET` env vars -> the bundled public
   `WEB_DESKTOP` web-app pair.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `client_key` | `Optional[str]` | `None` | Override the client key (else env var, else the web default). |
| `client_secret` | `Optional[str]` | `None` | Override the client secret (else env var, else the default). |
| `force_refresh` | `bool` | `False` | Mint a new token even if a cached one is still valid. |

**Returns**

The bearer `accessToken` string.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_token_gen
token = nfl_token_gen()                # mints + caches
assert nfl_token_gen() == token        # served from cache
assert isinstance(token, str) and token.startswith("ey")
```

### `nfl_usage_projection(seasons: 'List[int]', target_season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#nfl_usage_projection}

Project next-season target share, air-yards share, and WOPR.

Projects each player's shares via the shared Marcel blend
(`sportsdataverse.nfl.nfl_projection._marcel_blend` — the same
recency/shrinkage engine as the rate projection), assigns each player to
their most recent team, **renormalizes shares within each projected team to
sum to 1.0** (the share invariant), and converts shares to volumes with a
team-level carry-forward of pass attempts (team targets) and air yards.
As-of-date clean: only seasons strictly before `target_season` are used.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load. |
| `target_season` | `int` |  | The season being projected. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

`player_id:Utf8, target_season:Int64, position_group:Utf8, proj_team:Utf8, proj_target_share:Float64, proj_air_yards_share:Float64, proj_wopr:Float64, proj_targets:Float64, proj_air_yards:Float64`. Empty history returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only). |
| `position_group` | character | nflverse offensive position group. |
| `proj_team` | character | Most recent team (max season, tiebreak most targets) - the renormalization group. |
| `proj_target_share` | double | Projected share of team targets - Marcel share blend renormalized to sum to 1.0 within proj_team. |
| `proj_air_yards_share` | double | Projected share of team air yards, renormalized within proj_team. |
| `proj_wopr` | double | Projected weighted opportunity rating - 1.5 x proj_target_share + 0.7 x proj_air_yards_share. |
| `proj_targets` | double | Projected targets - proj_target_share x team pass-target carry-forward. |
| `proj_air_yards` | double | Projected receiving air yards - proj_air_yards_share x team air-yards carry-forward. |

**Example**

```python
from sportsdataverse.nfl.nfl_usage_projection import nfl_usage_projection
usage = nfl_usage_projection([2021, 2022, 2023], 2024)
usage.sort("proj_wopr", descending=True).head()
```

### `nfl_week_games(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'int' = 1, headers: 'Optional[Dict[str, str]]' = None, return_as_pandas: 'bool' = False)` {#nfl_week_games}

Parsed `api.nfl.com` week schedule -- one row per game (polars/pandas frame).

Tidy wrapper over `nfl_game_schedule`: flattens the `games` list into a
DataFrame with `id` (uuid game id), `season`/`seasonType`/`week`,
`date`, `status_*`, and `homeTeam_*` / `awayTeam_*` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. season_type (str): `"PRE"`/`"REG"`/`"POST"`. |
| `season_type` | `str` | `'REG'` |  |
| `week` | `int` | `1` | week number. headers: reuse a `nfl_headers_gen` dict. |
| `headers` | `Optional[Dict[str, str]]` | `None` |  |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per game.

| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `category` | character | Broader category of player positions |
| `date` | character | Date of the poll release. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `gameType` | character | Game type identifier (3 for playoffs). |
| `international` | logical | Boolean flag indicating whether this game is designated as an international (outside the United States) game. |
| `neutralSite` | logical | Whether the game is at a neutral site. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Phase of the season in which this game takes place (e.g., 'REG', 'POST', 'PRE'). |
| `status` | character | Game status (e.g. "scheduled", "in_progress", "completed"). |
| `week` | integer | Season week. |
| `weekType` | character | Classification of the week type within the season (e.g., 'REG', 'WC', 'DIV', 'CONF', 'SB'). |
| `externalIds` | character | Serialized list of external system identifiers (e.g., partner IDs, league IDs) mapped to this game. |
| `ticketUrl` | character | URL to the official ticket purchasing page for this game. |
| `ticketVendors` | character | Serialized list of authorized ticket vendors or marketplaces for this game. |
| `extensions` | character | Serialized JSON or string of additional extension metadata attached to the game record by the NFL Shield API. |
| `version` | integer | API response version integer returned by the NFL Shield API for this game record. |
| `homeTeam_id` | character | NFL Shield team identifier for the home team. |
| `homeTeam_currentLogo` | character | URL for the home team's current primary logo image as served by the NFL Shield API. |
| `homeTeam_fullName` | character | Full name of the home team (e.g., 'Dallas Cowboys'). |
| `awayTeam_id` | character | NFL Shield team identifier for the away team. |
| `awayTeam_currentLogo` | character | URL for the away team's current primary logo image as served by the NFL Shield API. |
| `awayTeam_fullName` | character | Full name of the away team (e.g., 'Kansas City Chiefs'). |
| `broadcastInfo_homeNetworkChannels` | character | Serialized list of TV/streaming channels designated as the home team's local broadcast. |
| `broadcastInfo_awayNetworkChannels` | character | Serialized list of TV/streaming channels designated as the away team's local broadcast. |
| `broadcastInfo_internationalWatchOptions` | character | Serialized list of international streaming or broadcast options for viewers outside the United States. |
| `broadcastInfo_streamingNetworks` | character | Serialized list of streaming platforms (e.g., Peacock, Amazon Prime Video) carrying the game. |
| `broadcastInfo_territory` | character | Geographic territory or market designation for which this broadcast record applies. |
| `broadcastInfo_audioNetworks` | character | Serialized list of radio networks carrying the game's audio broadcast. |
| `venue_id` | character | Referencing venue id. |
| `venue_name` | character | Full name of the franchise's venue. |
| `venue_city` | character | City where the venue is located. |
| `venue_country` | character | Country name or ISO code indicating where the game's venue is located. |

**Example**

```python
from sportsdataverse.nfl import nfl_week_games
sched = nfl_week_games(season=2024, season_type="REG", week=1)
sched.select(["id", "homeTeam_fullName", "awayTeam_fullName"]).head()
```

### `opponent_adjusted_ridge(plays: 'pl.DataFrame', *, off_col: 'str', def_col: 'str', home_col: 'str', resp_col: 'str', lam: 'float', penalize_home: 'bool' = False) -> 'tuple[pl.DataFrame, float, float]'` {#opponent_adjusted_ridge}

Ridge-regress `resp_col` on offense + defense team indicators + HFA.

League-agnostic (column names are arguments): builds the full
offense/defense-indicator + intercept + home design and solves the
ridge normal equations `beta = (X'X + lam*R)^-1 X'y`. Only team
coefficients are penalised; the intercept (and, unless
`penalize_home`, the home term) is free. Moved verbatim (T7.2) from
`sportsdataverse.nfl.nfl_ratings` -- NFL is currently the sole
adopter of this exact dense-design encoding (CFB's ridge is the
genuinely different `dropped_level_ridge`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | One row per play. Rows with a null `off_col` / `def_col` / `resp_col` must be filtered by the caller. |
| `off_col` | `str` |  | Column naming the offense (possession) team. |
| `def_col` | `str` |  | Column naming the defense team. |
| `home_col` | `str` |  | Column naming the home team (HFA indicator is `off_col == home_col`). |
| `resp_col` | `str` |  | Numeric response column (e.g. `epa`). |
| `lam` | `float` |  | Ridge penalty applied to the team coefficients. |
| `penalize_home` | `bool` | `False` | Also penalise the home-field coefficient (default False). |

**Returns**

A `(frame, intercept, home_coef)` tuple: `frame` has one row per team (`team_id` Utf8, `off_coef` / `def_coef` Float64); `intercept` is the league baseline; `home_coef` the fitted HFA in response units. Zero-row frame + `(0.0, 0.0)` on empty input.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import opponent_adjusted_ridge
frame, intercept, hfa = opponent_adjusted_ridge(
    plays, off_col="posteam", def_col="defteam",
    home_col="home_team", resp_col="epa", lam=200.0,
)
frame.sort("off_coef", descending=True).head()
```

### `playcall_features(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None) -> 'pl.DataFrame'` {#playcall_features}

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
| `game_id` | character | nflverse game identifier (Utf8 join key). |
| `play_id` | integer | nflverse play identifier within the game (Int64 join key). |
| `season` | integer | Season of the play. |
| `week` | integer | Week of the play. |
| `posteam` | character | Offense (possession) team abbreviation. |
| `down` | double | Down (1-4) at the snap. |
| `ydstogo` | double | Yards to go for a first down. |
| `yardline_100` | double | Yards from the opponent end zone at the snap. |
| `score_differential` | double | Offense score minus defense score at the snap. |
| `half_seconds_remaining` | double | Seconds remaining in the half. |
| `game_seconds_remaining` | double | Seconds remaining in the game. |
| `wp` | double | Start-of-play win probability for the offense. |
| `shotgun` | double | 1 when the offense lined up in shotgun. |
| `no_huddle` | double | 1 when the play was run without a huddle. |
| `xpass` | double | Shipped nflfastR-parity expected-dropback probability for the play. |
| `n_rb` | double | Running backs in the offensive personnel grouping (null without participation data). |
| `n_te` | double | Tight ends in the offensive personnel grouping (null without participation data). |
| `n_wr` | double | Wide receivers in the offensive personnel grouping (null without participation data). |
| `has_participation` | integer | 1 when a participation row matched the play, else 0. |
| `family` | character | 5-class play-call label (inside_run, outside_run, short_pass, deep_pass, scramble). |
| `is_pass` | integer | 1 when the play was a pass (including scrambles), else 0. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_playcall import playcall_features
feat = playcall_features(calculate_xpass(load_nfl_pbp([2023])))
print(feat["family"].value_counts())
```

### `player_usage_efficiency(player_stats: 'pl.DataFrame', *, as_of_week: 'int', era: 'str' = 'modern') -> 'pl.DataFrame'` {#player_usage_efficiency}

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

**Example**

```python
import polars as pl
import sportsdataverse.nfl as nfl
stats = nfl.load_nfl_player_stats().filter(pl.col("season") == 2023)
usage = nfl.player_usage_efficiency(stats, as_of_week=10)
usage.sort("exp_attempts", descending=True).head()
```

### `predict_margin(home_adj_net: 'float', away_adj_net: 'float', neutral: 'bool', *, era: 'str' = 'modern') -> 'float'` {#predict_margin}

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

### `predict_total(home_adj_off: 'float', home_adj_def: 'float', away_adj_off: 'float', away_adj_def: 'float', *, era: 'str' = 'modern') -> 'float'` {#predict_total}

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

### `pressure_pairs(pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#pressure_pairs}

Per (season, off_team, def_team) dropbacks + pressures (matchup grid).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the matchup aggregate. |
| `off_team` | character | Offense team abbreviation. |
| `def_team` | character | Defense team abbreviation. |
| `dropbacks` | integer | Offense dropbacks in the matchup. |
| `pressures` | integer | Sacks plus QB hits in the matchup. |

### `reset_config() -> 'NflConfig'` {#reset_config}

Reset the active config to its env-var-derived defaults.

Convenience for tests / interactive sessions that want to undo a chain
of `update_config()` calls without restarting the interpreter.

**Example**

```python
from sportsdataverse.nfl import update_config, reset_config
update_config(cache_mode="off", timeout=5)
# ... do work ...
reset_config()  # back to env-derived defaults
```

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

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
from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.nfl_schedule import scoreboard_event_parsing
url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
payload = download(url=url).json()
for ev in payload.get("events", []):
    scoreboard_event_parsing(ev)
    ev["competitions"][0]["home"]["abbreviation"]
```

### `scrape_ngs_season(stat_type: 'str', season: 'int', *, include_season_totals: 'bool' = True, return_as_pandas: 'bool' = False)` {#scrape_ngs_season}

Scrape a full season of NGS statboard data, shaped like the nflverse parquet.

Port of nflverse ngs-data's R `save_ngs_type`: loop the regular-season weeks
(`1..max_reg` where `max_reg = 18` for `season >= 2021` else `17`) plus
the playoff weeks (`max_reg+1 .. max_reg+5`, fetched with
`season_type="POST"`), stack them diagonally, and -- when
`include_season_totals` -- prepend the season-aggregate rows (NGS `week=0`,
a `REG` call with no `week` param) tagged `week=0`. Duplicate rows (NGS
returned dupes for some 2022 weeks) are de-duplicated on
`(season, week, player_gsis_id)`.

Output columns match the published nflverse NGS parquet read by
`sportsdataverse.nfl.load_nfl_nextgen_stats` (snake_case, `team_abbr`
resolved). It will not be byte-identical -- nflverse post-processes (column
pruning / ordering) -- but the core metric columns and the
player/team/week keys align.

NGS statboard rows are **player-week aggregates**, NOT per-play rows.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` |  | one of `"passing"`, `"rushing"`, `"receiving"`. |
| `season` | `int` |  | season year (NGS coverage starts in 2016). |
| `include_season_totals` | `bool` | `True` | also fetch the season-aggregate (`week=0`) rows. Defaults to `True` (matches ngs-data, whose week loop starts at 0). |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` stacking every week (and, by default, the season totals) for the requested `stat_type` and `season`. An EMPTY frame carrying the documented key schema if nothing is returned.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Percentage of pass attempts thrown into tight windows (defender within one yard of the receiver at completion or incompletion), from NFL Next Gen Stats. Passing only. |
| `attempts` | integer | Pass attempts (including incompletions) recorded for the passer during the slice. Passing only. |
| `avg_air_distance` | double | Average air distance in yards on all pass attempts, measuring how far the ball travels through the air regardless of direction. Passing only. |
| `avg_air_yards_differential` | double | Average difference between intended air yards and completed air yards, measuring accuracy relative to target depth. Passing only. |
| `avg_air_yards_to_sticks` | double | Average air yards relative to the first-down marker on pass attempts (positive = beyond the sticks). Passing only. |
| `avg_completed_air_yards` | double | Average air yards on completed passes only, measuring depth of actual completions. Passing only. |
| `avg_intended_air_yards` | double | Average depth of target on all pass attempts, regardless of completion. Passing/receiving. |
| `avg_time_to_throw` | double | Average time in seconds from snap to release for the passer, as tracked by NFL Next Gen Stats. Passing only. |
| `completion_percentage` | double | Actual completion percentage for the passer during the slice. Passing only. |
| `completion_percentage_above_expectation` | double | Completion percentage above the model-expected completion rate (CPOE/CPAE). Passing only. |
| `completions` | integer | Completed passes for the passer during the slice. Passing only. |
| `expected_completion_percentage` | double | Model-expected completion percentage based on target depth, separation, and coverage, from NFL Next Gen Stats. Passing only. |
| `games_played` | integer | Number of games played by the player during the slice covered by the row. |
| `interceptions` | integer | Interceptions thrown by the passer during the slice. Passing only. |
| `max_air_distance` | double | Maximum air distance in yards recorded on any single pass attempt during the slice. Passing only. |
| `max_completed_air_distance` | double | Maximum air distance in yards recorded on any single completed pass during the slice. Passing only. |
| `pass_touchdowns` | integer | Passing touchdowns thrown by the passer during the slice. Passing only. |
| `pass_yards` | integer | Total passing yards accumulated by the passer during the slice. Passing only. |
| `passer_rating` | double | NFL passer rating (0–158.3 scale) for the passer during the slice. Passing only. |
| `player_name` | character | Display name of the player as returned at the top-level statboard row. |
| `season` | integer | NFL season year for the row. |
| `season_type` | character | Season segment for the row ('REG' for regular-season weeks and the week-0 aggregate, 'POST' for playoff weeks). |
| `week` | integer | NFL Next Gen Stats week tag; 0 is the season aggregate, 1..max_reg are regular-season weeks, and higher values are continuous playoff weeks. |
| `position` | character | Player position as returned at the top-level statboard row (e.g., 'QB', 'WR', 'RB'). |
| `team_id` | character | NFL Next Gen Stats team identifier for the player's team on this row; the key joined to resolve team_abbr. |
| `player_season` | integer | NFL season year recorded in the nested player record. |
| `player_season_type` | character | Season segment recorded in the nested player record. |
| `player_week` | integer | Week recorded in the nested player record (the loop week tag overrides this on the top-level week column). |
| `player_jersey_number` | integer | Jersey number worn by the player. |
| `player_last_name` | character | Last name of the player. |
| `player_football_name` | character | Football name used by the player, which may differ from the legal first name. |
| `player_first_name` | character | First name of the player. |
| `player_position` | character | Player position from the nested player record. |
| `player_gsis_it_id` | integer | NFL GSIS internal tracking integer identifier for the player. |
| `player_gsis_id` | character | NFL GSIS (Game Statistics and Information System) identifier, the primary nflverse player key. |
| `player_esb_id` | character | Elias Sports Bureau (ESB) identifier for the player. |
| `player_display_name` | character | Full display name of the player as used in NFL Next Gen Stats records. |
| `player_short_name` | character | Shortened display name for the player (e.g., 'P.Mahomes'). |
| `player_uniform_number` | character | Uniform number worn by the player, stored as a string to preserve leading zeros if applicable. |
| `player_status` | character | Current roster status of the player (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_current_team_id` | character | NFL Next Gen Stats team identifier for the team the player is currently rostered on. |
| `player_smart_id` | character | NFL Next Gen Stats smart (UUID-style) identifier for the player. |
| `player_headshot` | character | URL template for the player's headshot image. |
| `player_position_group` | character | Broad position group for the player (e.g., 'QB', 'WR') in the NGS player record. |
| `player_ngs_position` | character | Player's position as classified by the NFL Next Gen Stats system. |
| `player_ngs_position_group` | character | Broader position group the player belongs to as classified by the NFL Next Gen Stats system. |
| `team_abbr` | character | Team abbreviation resolved from team_id via the NGS team directory (relocated franchise abbreviations dropped to keep the mapping one-to-one). |

**Example**

```python
from sportsdataverse.nfl import scrape_ngs_season
pas = scrape_ngs_season("passing", 2023)
pas.select(["season", "week", "player_display_name", "team_abbr"]).head()

# Regular-season weeks only (skip the week-0 totals)

wk = scrape_ngs_season("receiving", 2023, include_season_totals=False)

# Column-compatible with the published parquet

from sportsdataverse.nfl import load_nfl_nextgen_stats
published = load_nfl_nextgen_stats(seasons=[2023], stat_type="passing")
shared = set(pas.columns) & set(published.columns)
```

### `scrape_ngs_week(stat_type: 'str', season: 'int', week: 'int', season_type: 'str' = 'REG', *, return_as_pandas: 'bool' = False)` {#scrape_ngs_week}

Scrape one (season, week) NGS statboard slice, shaped like the nflverse parquet.

Port of nflverse ngs-data's R `load_week_ngs`: fetch a single statboard
slice via `nfl_ngs_statboard`, resolve `team_abbr` from the team
directory, snake-case every column, and tag the row with the loop `week`.
`week=0` is the season-aggregate row (a `season_type="REG"` call with no
`week` query param); weeks `1..max_reg` are regular-season, and the
playoff weeks (`max_reg+1` upward) are fetched with `season_type="POST"`.

NGS statboard rows are **player-week aggregates** (`avg_intended_air_yards`,
`completion_percentage_above_expectation`, `avg_time_to_throw`, ...), NOT
per-play rows -- this is a season-stats source, not a per-play air-yards /
completion-probability source.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` |  | one of `"passing"`, `"rushing"`, `"receiving"`. |
| `season` | `int` |  | season year (NGS coverage starts in 2016). |
| `week` | `int` |  | NGS week. `0` -> season aggregate; `1..max_reg` -> REG; higher -> POST. The supplied value is what tags the returned rows. |
| `season_type` | `str` | `'REG'` | `"REG"` or `"POST"`; the caller (or `scrape_ngs_season`) selects this per week. Defaults to `"REG"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` of player-week NGS rows with snake-cased columns + a resolved `team_abbr`. An EMPTY frame carrying the documented key schema (not an exception) when the API yields no stats.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Percentage of pass attempts thrown into tight windows (defender within one yard of the receiver at completion or incompletion), from NFL Next Gen Stats. Passing only. |
| `attempts` | integer | Pass attempts (including incompletions) recorded for the passer during the week. Passing only. |
| `avg_air_distance` | double | Average air distance in yards on all pass attempts, measuring how far the ball travels through the air regardless of direction. Passing only. |
| `avg_air_yards_differential` | double | Average difference between intended air yards and completed air yards, measuring accuracy relative to target depth. Passing only. |
| `avg_air_yards_to_sticks` | double | Average air yards relative to the first-down marker on pass attempts (positive = beyond the sticks). Passing only. |
| `avg_completed_air_yards` | double | Average air yards on completed passes only, measuring depth of actual completions. Passing only. |
| `avg_intended_air_yards` | double | Average depth of target on all pass attempts, regardless of completion. Passing/receiving. |
| `avg_time_to_throw` | double | Average time in seconds from snap to release for the passer, as tracked by NFL Next Gen Stats. Passing only. |
| `completion_percentage` | double | Actual completion percentage for the passer during the week. Passing only. |
| `completion_percentage_above_expectation` | double | Completion percentage above the model-expected completion rate (CPOE/CPAE). Passing only. |
| `completions` | integer | Completed passes for the passer during the week. Passing only. |
| `expected_completion_percentage` | double | Model-expected completion percentage based on target depth, separation, and coverage, from NFL Next Gen Stats. Passing only. |
| `games_played` | integer | Number of games played by the player during the slice covered by the row. |
| `interceptions` | integer | Interceptions thrown by the passer during the week. Passing only. |
| `max_air_distance` | double | Maximum air distance in yards recorded on any single pass attempt during the week. Passing only. |
| `max_completed_air_distance` | double | Maximum air distance in yards recorded on any single completed pass during the week. Passing only. |
| `pass_touchdowns` | integer | Passing touchdowns thrown by the passer during the week. Passing only. |
| `pass_yards` | integer | Total passing yards accumulated by the passer during the week. Passing only. |
| `passer_rating` | double | NFL passer rating (0–158.3 scale) for the passer during the week. Passing only. |
| `player_name` | character | Display name of the player as returned at the top-level statboard row. |
| `season` | integer | NFL season year for the row. |
| `season_type` | character | Season segment for the row ('REG' or 'POST') as supplied by the caller. |
| `week` | integer | NFL Next Gen Stats week tag supplied by the caller; 0 is the season aggregate, 1..max_reg are regular-season weeks, and higher values are continuous playoff weeks. |
| `position` | character | Player position as returned at the top-level statboard row (e.g., 'QB', 'WR', 'RB'). |
| `team_id` | character | NFL Next Gen Stats team identifier for the player's team on this row; the key joined to resolve team_abbr. |
| `player_season` | integer | NFL season year recorded in the nested player record. |
| `player_season_type` | character | Season segment recorded in the nested player record. |
| `player_week` | integer | Week recorded in the nested player record (the loop week tag overrides this on the top-level week column). |
| `player_jersey_number` | integer | Jersey number worn by the player. |
| `player_last_name` | character | Last name of the player. |
| `player_football_name` | character | Football name used by the player, which may differ from the legal first name. |
| `player_first_name` | character | First name of the player. |
| `player_position` | character | Player position from the nested player record. |
| `player_gsis_it_id` | integer | NFL GSIS internal tracking integer identifier for the player. |
| `player_gsis_id` | character | NFL GSIS (Game Statistics and Information System) identifier, the primary nflverse player key. |
| `player_esb_id` | character | Elias Sports Bureau (ESB) identifier for the player. |
| `player_display_name` | character | Full display name of the player as used in NFL Next Gen Stats records. |
| `player_short_name` | character | Shortened display name for the player (e.g., 'P.Mahomes'). |
| `player_uniform_number` | character | Uniform number worn by the player, stored as a string to preserve leading zeros if applicable. |
| `player_status` | character | Current roster status of the player (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_current_team_id` | character | NFL Next Gen Stats team identifier for the team the player is currently rostered on. |
| `player_smart_id` | character | NFL Next Gen Stats smart (UUID-style) identifier for the player. |
| `player_headshot` | character | URL template for the player's headshot image. |
| `player_position_group` | character | Broad position group for the player (e.g., 'QB', 'WR') in the NGS player record. |
| `player_ngs_position` | character | Player's position as classified by the NFL Next Gen Stats system. |
| `player_ngs_position_group` | character | Broader position group the player belongs to as classified by the NFL Next Gen Stats system. |
| `team_abbr` | character | Team abbreviation resolved from team_id via the NGS team directory (relocated franchise abbreviations dropped to keep the mapping one-to-one). |

**Example**

```python
from sportsdataverse.nfl import scrape_ngs_week
wk1 = scrape_ngs_week("passing", 2023, week=1)
wk1.select(["season", "week", "player_display_name", "team_abbr"]).head()

# Season-aggregate row (week 0)

tot = scrape_ngs_week("rushing", 2023, week=0)
```

### `special_teams_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'` {#special_teams_ratings}

One row per team: opponent-adjusted special-teams EPA per play.

Reuses `opponent_adjusted_ridge` (no forked solver) restricted to
`special == 1` plays with `resp_col="epa"`; `adj_st_epa` is the
`off_coef` (the special-teams unit acting as "offense" on the play).
Teams appearing anywhere in `plays` but on no special-teams play get
the documented neutral fill `adj_st_epa = 0.0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | An `load_nfl_pbp`-schema frame carrying `posteam`, `defteam`, `home_team`, `epa`, `special`. Not pre-filtered -- this function selects the ST plays itself. |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs (only `ridge_lambda` is consulted); defaults to `RatingsConfig`. |

**Returns**

One row per `team_id` (Utf8) with `adj_st_epa` (Float64). Zero-row, correctly-typed when `plays` is empty.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import special_teams_ratings
st = special_teams_ratings(pbp)
st.sort("adj_st_epa", descending=True).head()
```

### `team_game_pace(pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#team_game_pace}

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
| `game_id` | character | nflverse game identifier (Utf8 join key). |
| `season` | integer | Season of the game. |
| `week` | integer | Week of the game. |
| `posteam` | character | Offense team abbreviation. |
| `off_plays` | integer | Offensive plays in the game (kneels, spikes and no_plays excluded). |
| `sec_per_play` | double | Mean over the team's drives of elapsed game clock divided by drive plays. |
| `neutral_plays` | integer | Offensive plays in neutral situations (wp in [0.2, 0.8], over 2 minutes left in the half). |
| `neutral_sec_per_play` | double | sec_per_play computed on neutral-situation plays only. |
| `proe` | double | Mean pass_oe over the team's dropbacks in the game (percentage points). |
| `dropbacks` | integer | Dropbacks with a non-null pass_oe (the proe denominator). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_gamescript import team_game_pace
pace = team_game_pace(load_nfl_pbp([2023]))
print(pace.sort("sec_per_play").head())
```

### `team_name_fn(expr: 'pl.Expr') -> 'pl.Expr'` {#team_name_fn}

Fold historical/relocated team codes onto their current abbreviation.

Verbatim port of nflfastR's `team_name_fn` (a plain
`stringr::str_replace_all` over a 10-entry named vector). Operates as a
**substring** replace (not a full-value lookup) so it also fixes
embedded codes like `"SD 49" -> "LAC 49"` on yard-line columns. The
10 from-codes are disjoint from all of their to-values, so the order of
the 10 sequential replacements does not matter (verified in
`tests.nfl.test_nfl_clean`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `expr` | `Expr` |  | A `polars.Expr` over a Utf8 column (e.g. `pl.col("posteam")`). |

**Returns**

The same expression with every occurrence of the 10 historical codes replaced by their current-franchise code.

### `team_pressure_rates(pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#team_pressure_rates}

Per (season, team) raw pressure rates, both sides of the ball.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp with `season` / `posteam` / `defteam` / `qb_dropback` / `sack` / `qb_hit`. |

**Returns**

Per `(season, team)`: `dropbacks_off`, `pressures_allowed`, `pressure_rate_allowed`, `dropbacks_def`, `pressures_generated`, `pressure_rate_generated`. Empty input yields a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off. |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_line_grades import team_pressure_rates
rates = team_pressure_rates(load_nfl_pbp([2023]))
print(rates.sort("pressure_rate_generated", descending=True).head())
```

### `update_config(**kwargs: 'object') -> 'NflConfig'` {#update_config}

Update the active config in place.

**Returns**

The (mutated) global config object, for chaining or inspection.

**Example**

```python
from sportsdataverse.nfl import update_config
update_config(cache_mode="filesystem", cache_duration=3600)

# Disable caching for development

update_config(cache_mode="off")

# Point cache at a custom directory

update_config(cache_dir="~/sdv-cache")
```

### `win_prob_from_margin(exp_margin: 'float', *, era: 'str' = 'modern') -> 'float'` {#win_prob_from_margin}

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
