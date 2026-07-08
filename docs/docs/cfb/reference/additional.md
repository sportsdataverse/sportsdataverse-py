---
title: CFB — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# CFB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.cfb`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_cfb_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_cfb_game_rosters}

espn_cfb_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_cfb_schedule(). |
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
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `athlete_display_name` | character | Player display name; `athlete_detail = TRUE` only. |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `weight` | double | Listed weight (lbs). |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `height` | double | Listed height (inches). |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `slug` | character | URL slug for the team. |
| `jersey` | character | Jersey number. |
| `linked` | logical | TRUE if the record is linked to a related entity. |
| `active` | logical | `TRUE` if the player was active for the game. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_country_alternate_id` | character | Alternate identifier for the athlete's birth country, used in ESPN's nationality lookup system. |
| `birth_country_abbreviation` | character | Birth country abbreviation. |
| `headshot_href` | character | URL of the athlete headshot image. |
| `headshot_alt` | character | Alternative-text label for the headshot. |
| `flag_href` | character | URL to the athlete's nationality flag image on ESPN's CDN. |
| `flag_alt` | character | Alt-text description for the athlete's country flag image, typically the country name. |
| `flag_rel` | character | Relationship descriptor for the flag image link (e.g., 'flag' or 'country'). |
| `experience_years` | integer | Years of experience. |
| `experience_display_value` | character | Experience display value. |
| `experience_abbreviation` | character | Experience abbreviation. |
| `status_id` | character | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Player date of birth (if published). |
| `starter` | logical | `TRUE` if the athlete started the game. |
| `jersey_right` | character | Secondary or alternate jersey number field, distinct from the primary jersey number. |
| `valid` | logical | `TRUE` if the roster entry is flagged valid by ESPN. |
| `did_not_play` | logical | `TRUE` if the athlete did not play. |
| `display_name` | character | Human-readable metric name. |
| `athlete_href` | character | ESPN API URL reference for the athlete's full profile resource. |
| `position_href` | character | ESPN API URL reference for the athlete's position resource. |
| `statistics_href` | character | ESPN API URL reference for the athlete's game or season statistics resource. |
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
| `is_active` | logical | Whether the team is currently active. |
| `is_all_star` | logical | Whether the team is an all-star team. |
| `team_alternate_ids_sdr` | character | ESPN SDR (Sports Data Repository) alternate team identifier for the athlete's team. |
| `logo_href` | character | URL of the default team logo. |
| `logo_dark_href` | character | URL of the dark-variant team logo. |
| `game_id` | integer | ESPN game identifier. |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_game_rosters
rosters = espn_cfb_game_rosters(game_id=401628334)
print(rosters.shape)

# Pandas round-trip

rosters_pd = espn_cfb_game_rosters(game_id=401628334, return_as_pandas=True)
rosters_pd.head()

# Pipeline next step (filter to game starters)

import polars as pl
starters = espn_cfb_game_rosters(game_id=401628334).filter(
    pl.col("starter") == True
)
```

### `espn_cfb_play_participants(game_id: 'int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, resolve_missing: 'bool' = True, resolve_missing_max: 'int' = 50, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_cfb_play_participants}

Pull ESPN per-play participants for a college-football game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game / event identifier. |
| `raw` | `bool` | `False` | If True, returns the raw list of play-items dicts (after following pagination) before any flattening. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |
| `resolve_missing` | `bool` | `True` | If True (default), athletes that the `cdn.espn.com` sidecar omits are fetched one-by-one from their canonical ESPN `$ref` URL so the resulting frame has populated `*_player_name` / `*_player_names` columns wherever an `*_player_id` is non-null. Setting this to False skips the extra HTTP fan-out and reproduces the pre-enhancement behavior — rows may then ship with `*_player_id` populated but `*_player_name` null on the handful of athletes the sidecar misses (most visible on split sacks, multi-lateral returns, and older games). |
| `resolve_missing_max` | `int` | `50` | Hard cap on the number of per-athlete `$ref` requests issued for a single game. Defaults to 50, which comfortably covers every probed game (typical max is ≤8 unique missing athletes). If breached, a warning is logged and the remaining missing athletes are left with null names. Ignored when `resolve_missing=False`. |

**Returns**

Polars (or pandas) DataFrame, one row per play. Columns include `game_id`, `play_id`, and TWO column families for every participant `type` ESPN ships for the game (typical types: `passer`, `rusher`, `receiver`, `tackler`, `sacked_by`, `forced_by`, `pass_defender`, `kicker`, `punter`, `returner`, `recoverer`, `scorer`, `pat_scorer`, `penalized`, `assisted_by`): * **Scalar** — `{type}_player_id` / `{type}_player_name`: the first occurrence of that participant type on the play. Backwards compatible with the legacy regex-extractor shape. * **List** — `{type}_player_ids` / `{type}_player_names`: `List(Utf8)` columns containing **every** occurrence of that participant type on the play, in the order ESPN shipped them. Plays with no participant of a given type carry an empty list `[]` (not null) for downstream consumption simplicity. This family preserves multi-entry participant types (split sacks where ESPN ships two `sackedBy` entries, multi-tacklers, etc.) that the scalar family collapses to first-only. If `raw=True`, returns the parsed JSON list of play dicts.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN game identifier. |
| `play_id` | integer | ESPN play id. |
| `kicker_player_name` | character | String name for the kicker on FG or kickoff. |
| `passer_player_name` | character | Name of the passer on a passing play. |
| `receiver_player_name` | character | Name of the receiver on a passing play. |
| `rusher_player_name` | character | Name of the rusher on a rushing play. |
| `scorer_player_name` | character | Display name of the primary player credited with a touchdown or field goal score on the play. |
| `returner_player_name` | character | Display name of the primary player who returned a kick, punt, or interception on the play. |
| `pass_defender_player_name` | character | Display name of the primary pass defender who contested the target or broke up the pass on the play. |
| `penalized_player_name` | character | Display name of the player assessed a penalty on the play. |
| `sacked_by_player_name` | character | Display name of the primary pass rusher credited with the sack on the play. |
| `pat_scorer_player_name` | character | Display name of the player who scored the point-after-touchdown conversion on the play. |
| `punter_player_name` | character | Name of the punter. |
| `kicker_player_id` | character | Unique identifier for the kicker on FG or kickoff. |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `scorer_player_id` | character | ESPN athlete ID for the primary player who scored a touchdown or field goal on the play. |
| `returner_player_id` | character | ESPN athlete ID for the primary player who returned a kick, punt, or interception on the play. |
| `pass_defender_player_id` | character | ESPN athlete ID for the primary pass defender (cornerback or safety) who contested the target on the play. |
| `penalized_player_id` | character | ESPN athlete ID for the primary player who committed the penalty on the play. |
| `sacked_by_player_id` | character | ESPN athlete ID for the primary pass rusher who recorded the sack on the play. |
| `pat_scorer_player_id` | character | ESPN athlete ID for the primary player who scored a point-after-touchdown (PAT) conversion on the play. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `kicker_player_names` | character | List of display names for all kickers credited on the play. |
| `passer_player_names` | character | List of display names for all passers credited on the play. |
| `receiver_player_names` | character | List of display names for all intended receivers credited on the play. |
| `rusher_player_names` | character | List of display names for all ball carriers credited on the play. |
| `scorer_player_names` | character | List of display names for all players credited with scoring on the play. |
| `returner_player_names` | character | List of display names for all returners credited on the play. |
| `pass_defender_player_names` | character | List of display names for all pass defenders credited on the play. |
| `penalized_player_names` | character | List of display names for all players penalized on the play. |
| `sacked_by_player_names` | character | List of display names for all pass rushers credited with the sack, including secondary participants on split sacks. |
| `pat_scorer_player_names` | character | List of display names for all players credited with a PAT conversion on the play. |
| `punter_player_names` | character | List of display names for all punters credited on the play. |
| `kicker_player_ids` | character | List of ESPN athlete IDs for all kickers credited on the play (e.g., kickoff, field goal, or PAT attempt). |
| `passer_player_ids` | character | List of ESPN athlete IDs for all passers credited on the play (supports multi-player lateral/trick plays). |
| `receiver_player_ids` | character | List of ESPN athlete IDs for all intended receivers on the play (supports lateral chains). |
| `rusher_player_ids` | character | List of ESPN athlete IDs for all ball carriers credited on the play (supports lateral handoffs). |
| `scorer_player_ids` | character | List of ESPN athlete IDs for all players credited with a scoring event on the play. |
| `returner_player_ids` | character | List of ESPN athlete IDs for all returners credited on the play (e.g., during a lateral after a return). |
| `pass_defender_player_ids` | character | List of ESPN athlete IDs for all pass defenders who contested the target or were credited with a pass breakup on the play. |
| `penalized_player_ids` | character | List of ESPN athlete IDs for all players penalized on the play. |
| `sacked_by_player_ids` | character | List of ESPN athlete IDs for all pass rushers credited with the sack on the play (includes split-sack participants). |
| `pat_scorer_player_ids` | character | List of ESPN athlete IDs for all players credited with a PAT conversion on the play. |
| `punter_player_ids` | character | List of ESPN athlete IDs for all punters credited on the play. |

**Example**

```python
from sportsdataverse.cfb import espn_cfb_play_participants
participants = espn_cfb_play_participants(game_id=401628334)
print(participants.shape)

# Skip the per-athlete fan-out for speed

participants_fast = espn_cfb_play_participants(
    game_id=401628334,
    resolve_missing=False,
)

# Pipeline next step (join onto play-by-play frame)

from sportsdataverse.cfb import CFBPlayProcess
pbp = CFBPlayProcess(gameId=401628334).espn_cfb_pbp()
plays = pbp["plays"]
joined = plays.join(participants, how="left", left_on="id", right_on="play_id")
```

### `espn_cfb_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'` {#espn_cfb_player_stats}

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
| `total` | logical | Total. |
| `athlete_id` | integer | ESPN athlete id. |
| `athlete_uid` | character | ESPN athlete UID (universal identifier). |
| `athlete_guid` | character | ESPN athlete GUID. |
| `athlete_type` | character | Athlete type / class. |
| `first_name` | character | Athlete first name. |
| `last_name` | character | Athlete last name. |
| `full_name` | character | Venue full name (e.g. `Tenney Stadium`). |
| `display_name` | character | Human-readable metric name. |
| `short_name` | character | Ranking source short name (e.g. `AP Poll`). |
| `weight` | double | Listed weight (lbs). |
| `display_weight` | character | Human-readable weight (e.g. `205 lbs`). |
| `height` | double | Listed height (inches). |
| `display_height` | character | Human-readable height (e.g. `6' 1"`). |
| `age` | integer | Player age (in years). |
| `date_of_birth` | character | Player date of birth (if published). |
| `jersey` | character | Jersey number. |
| `slug` | character | URL slug for the team. |
| `active` | logical | `TRUE` if the player was active for the game. |
| `position_id` | integer | ESPN position id. |
| `position_name` | character | Position name (e.g. `Quarterback`); `position_detail = TRUE` only. |
| `position_display_name` | character | Human-readable position name; `position_detail = TRUE` only. |
| `position_abbreviation` | character | Position abbreviation (e.g. `QB`); `position_detail = TRUE` only. |
| `college_name` | character | College name. |
| `status_id` | integer | ESPN commitment status id. |
| `status_name` | character | Status-type key (e.g. `STATUS_FINAL`). |
| `general_fumbles` | double | Total number of fumbles committed by the player across all offensive and special-teams plays. |
| `general_fumbles_lost` | double | Number of fumbles the player committed that were recovered by the opposing team. |
| `general_fumbles_touchdowns` | double | Total touchdowns scored by the player as a result of fumble recoveries, combining offensive and defensive occurrences. |
| `general_games_played` | double | Games Played. |
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
from sportsdataverse.cfb import espn_cfb_player_stats
df = espn_cfb_player_stats(athlete_id=4426338, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### `espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_cfb_schedule}

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
| `time_valid` | logical | Whether the start time is confirmed. |
| `date_valid` | logical | Boolean flag indicating whether the game's scheduled date is confirmed and valid. |
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
| `venue_address_country` | character | Country in which the game venue is located, as provided by ESPN's venue data. |
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
| `home_conference_id` | character | Unique identifier for home conference. |
| `home_score` | character | Home-team score after the play. |
| `home_current_rank` | integer | AP or Coaches Poll ranking of the home team at the time of the game (null if unranked). |
| `home_linescores` | list | Per-period point totals for the home team, stored as an array of quarter/overtime scores. |
| `home_records` | character | Win-loss record of the home team at the time of the game, as reported by ESPN (e.g., overall or conference record). |
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
| `away_conference_id` | character | Unique identifier for away conference. |
| `away_score` | character | Away-team score after the play. |
| `away_current_rank` | integer | AP or Coaches Poll ranking of the away team at the time of the game (null if unranked). |
| `away_linescores` | list | Per-period point totals for the away team, stored as an array of quarter/overtime scores. |
| `away_records` | character | Win-loss record of the away team at the time of the game, as reported by ESPN (e.g., overall or conference record). |
| `game_id` | integer | ESPN game identifier. |
| `season` | integer | Season (4-digit year). |
| `season_type` | integer | ESPN season type (2 = regular, 3 = postseason). |
| `week` | integer | Game week of the season. |
| `venue_address_state` | character | Venue address state / region. |
| `groups_id` | character | Unique identifier for groups. |
| `groups_name` | character | Groups name. |
| `groups_short_name` | character | Groups short name. |
| `groups_is_conference` | logical | Groups is conference. |

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

## Dataset loaders

### `load_cfb_betting_lines(return_as_pandas=False) -> 'pl.DataFrame'` {#load_cfb_betting_lines}

Load college football betting lines information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing betting lines available for the available seasons.

| col_name | type | description |
|---|---|---|
| `id` | double | 247Sports referencing id for the recruit. |
| `game_id` | integer | ESPN game identifier. |
| `season` | double | Season (4-digit year). |
| `game_desc` | character | Human-readable description of the game, typically including team names and context. |
| `date_time` | character | Date and time of the game to which the betting line applies, as a string. |
| `market_type` | character | Geographic market type (e.g. `National`). |
| `abbr` | character | Selection/side this odds row applies to — a team abbreviation for spread and moneyline markets, or 'over'/'under' for total markets (the data is long-format, one row per book per selection per market_type). |
| `lines` | double | Numeric line for this row's market — the per-side point spread for spread markets or the over/under total points for total markets; null for moneyline rows. |
| `odds` | integer | American-odds price for this selection — the juice/vig on spread and total rows, or the moneyline price itself on moneyline rows. |
| `opening_lines` | double | Opening numeric line for this row's market (per-side spread or over/under total points) before line movement; null for moneyline rows. |
| `opening_odds` | integer | Opening American-odds price for this selection before line movement (vig on spread/total rows, moneyline price on moneyline rows). |
| `book` | character | Name of the sportsbook or oddsmaker that provided the betting line. |
| `season_type` | character | ESPN season type (2 = regular, 3 = postseason). |
| `week` | integer | Game week of the season. |

**Example**

```python
from sportsdataverse.cfb import load_cfb_betting_lines
lines = load_cfb_betting_lines()
print(lines.shape)

# Pandas round-trip

lines_pd = load_cfb_betting_lines(return_as_pandas=True)
lines_pd.head()

# Pipeline next step (filter to one provider in 2023)

import polars as pl
consensus_2023 = load_cfb_betting_lines().filter(
    (pl.col("season") == 2023) & (pl.col("provider") == "consensus")
)
```

### `load_cfb_rosters_crosswalk(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'` {#load_cfb_rosters_crosswalk}

Load the current ESPN x Fox CFB rosters crosswalk (single snapshot).

Unlike the per-season `load_cfb_teams_crosswalk` / `load_cfb_schedule_crosswalk`
loaders, this one is **season-less**: ESPN's and Fox's team-roster endpoints
only expose the *current* roster, so the published artifact is a single
snapshot rather than a historical per-season series. It is built by
`cfbfastR-cfb-data`'s `scripts/build_cfb_crosswalk.py` (which fans the
per-team `sportsdataverse.cfb.cfb_rosters_crosswalk` builder out over
the current season's ESPN<->Fox team-id pairs) and refreshed on that repo's
cadence.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

one row per matched player, carrying `espn_team_id` / `fox_team_id` provenance plus each provider's athlete id, name, jersey, position, and the `match_method` / `matched_sources` flags.

**Example**

```python
from sportsdataverse.cfb import load_cfb_rosters_crosswalk
xwalk = load_cfb_rosters_crosswalk()
print(xwalk.shape)

# Pandas round-trip

xwalk_pd = load_cfb_rosters_crosswalk(return_as_pandas=True)

# Pipeline next step (one team's ESPN<->Fox athlete map)

import polars as pl
osu = load_cfb_rosters_crosswalk().filter(pl.col("espn_team_id") == 194)
```

## Utilities & helpers

### `CFBPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, odds_override=None, game_roster=None, participants=None, join_participants=True, **kwargs)` {#CFBPlayProcess}

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

#### `CFBPlayProcess.add_2pt_probs()`

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

#### `CFBPlayProcess.add_fourth_down_probs()`

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

#### `CFBPlayProcess.cfb_pbp_disk()`

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

#### `CFBPlayProcess.cfb_pbp_json(**kwargs)`

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

#### `CFBPlayProcess.corrupt_pbp_check()`

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

#### `CFBPlayProcess.create_box_score(play_df)`

Build a per-team and per-player advanced box score from a processed

plays frame.

Triggers `run_processing_pipeline` first if it hasn't already run,
so the input `play_df` is expected to be the post-pipeline plays frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | The plays frame produced by `run_processing_pipeline` (with EPA, WPA and play-type flags already populated). |

**Returns**

Box-score sections, each a list of records — `"pass"` / `"rush"` / `"receiver"` (per-player advanced + EPA lines), `"team"` and `"situational"` (per-team), `"defensive"` and `"defensive_players"` (team- and player-level havoc), `"specialists"` (kicking / punting / return players), `"turnover"`, `"drives"`, and the ESPN-sourced `"espn_team"` / `"espn_players"` totals.

**Example**

```python
from sportsdataverse.cfb import CFBPlayProcess
game = CFBPlayProcess(gameId=401628334)
game.espn_cfb_pbp()
processed = game.run_processing_pipeline()
box = game.create_box_score(game.plays_json)
print(list(box.keys()))
```

#### `CFBPlayProcess.espn_cfb_pbp(**kwargs)`

espn_cfb_pbp() - Pull the game by id. Data from API endpoints: `college-football/playbyplay`,

`college-football/summary`

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

#### `CFBPlayProcess.run_cleaning_pipeline()`

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

#### `CFBPlayProcess.run_processing_pipeline(fourth_down_probs: 'bool' = True, two_pt_probs: 'bool' = True)`

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

### `most_recent_cfb_season()` {#most_recent_cfb_season}

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

## Other

### `cfb_adjusted_epa(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float' = 325.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#cfb_adjusted_epa}

Season opponent-adjusted per-team EPA from a season's play-by-play.

Fits one ridge of per-play `EPA` on offense-team, defense-team, and
home-field indicators over the competitive (`0.1 <= wp_before <= 0.9`) pass
and rush plays, nets each team's per-game raw EPA against the opponent's
fitted strength, and averages to a season figure. In-sample/descriptive (the
fit uses the whole season); for leak-free per-game values use
`cfb_adjusted_epa_by_game`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the columns listed in the module docstring. One season at a time. |
| `ridge_lambda` | `float` | `325.0` | Ridge penalty (glmnet-scale; default 325). |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per team (>= 2 valid games): `team_id`, `pos_team`, `valid_games`, `adj_off_epa`, `adj_def_epa`, `off_strength_faced`, `def_strength_faced`, `net_adj_epa` and their `*_rank` columns.

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
cfb.cfb_adjusted_epa(pbp).sort("net_adj_epa_rank").head()
```

### `cfb_adjusted_epa_by_game(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float' = 325.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#cfb_adjusted_epa_by_game}

Walk-forward (point-in-time) opponent-adjusted EPA, one row per team-game.

For each week `w` the opponent-strength ridge is fit on competitive plays
from **weeks before `w` only**, then that week's games are adjusted with
those as-of strengths -- so the value uses no future information and is valid
as an in-season power-rating / model feature. Week 1 (no prior) yields null
adjustments; not-yet-seen opponents fall back to the league baseline (with the
heavy ridge penalty this is the intended early-season shrinkage to average).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the module-docstring columns **plus** `week`. One season at a time. |
| `ridge_lambda` | `float` | `325.0` | Ridge penalty (glmnet-scale; default 325). |
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

### `cfb_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'int', *, rng: 'Optional[np.random.Generator]' = None, elo: 'Optional[Dict[str, float]]' = None, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame]'` {#cfb_compute_results}

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

..., "games": ...}`` — updated frames, mirroring nflseedR's returned list.

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

### `cfb_games_from_schedule(schedule: 'FrameLike', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, Any]'` {#cfb_games_from_schedule}

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

### `cfb_odds_events_crosswalk(season: 'Optional[int]' = None, week: 'Optional[int]' = None, *, sport: 'str' = 'americanfootball_ncaaf', api_key: 'Optional[str]' = None, season_type: 'int' = 2, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'` {#cfb_odds_events_crosswalk}

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

**Example**

```python
from sportsdataverse.cfb import cfb_odds_events_crosswalk
xwalk = cfb_odds_events_crosswalk(season=2024, week=5)
matched = xwalk.filter(pl.col("espn_game_id").is_not_null())
```

### `cfb_playoff_seeds(standings: 'FrameLike', rankings: 'Optional[FrameLike]' = None, playoff_seeds: 'int' = 12, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, Any]'` {#cfb_playoff_seeds}

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

### `cfb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#cfb_predict_games}

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

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import cfb_predict_games
from sportsdataverse.cfb import cfb_ratings
from sportsdataverse.cfb.cfb_schedule import cfb_schedule  # schedule loader
ratings = cfb_ratings(2023)
preds = cfb_predict_games(schedule_2023, ratings)
```

### `cfb_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#cfb_ratings}

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
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

A DataFrame with one row per `team_id`, columns in this order: `season` (Int64 -- the single passed season for the common single-season call; `null` for a pooled multi-season call, since no single season applies to every row), `team_id` (Utf8), `adj_off_epa`, `adj_def_epa` (Float64, from `efficiency_ratings`), `adj_st_epa` (Float64, from `special_teams_ratings`), `adj_net` (Float64 -- offense minus defense only; special teams is a separate column, not folded in), `fei_off`, `fei_def`, `fei_net` (Float64, from `fei_ratings`), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model uses), `off_rank` (Int64, dense rank on `adj_off_epa` descending), `def_rank` (Int64, dense rank on `adj_def_epa` **ascending** -- fewer EPA allowed ranks better), `net_rank` (Int64, dense rank on `adj_net` descending), `net_z` (Float64, z-score of `adj_net`). Zero-row (correctly-typed) when the requested season(s) have no published pbp/schedule asset, or when `as_of_date` filters out every play.

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

### `cfb_rosters_crosswalk(espn_team_id: 'Union[int, str]', fox_team_id: 'Union[int, str]', *, season: 'Optional[int]' = None, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'` {#cfb_rosters_crosswalk}

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

### `cfb_schedule_crosswalk(season: 'int', week: 'Optional[int]' = None, *, season_type: 'int' = 2, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'` {#cfb_schedule_crosswalk}

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

### `cfb_simulations(games: 'FrameLike', teams: 'FrameLike', compute_results: 'Optional[ComputeResultsFn]' = None, *, simulations: 'int' = 10000, playoff_seeds: 'int' = 12, tiebreaker_depth: 'str' = 'SOS', sim_include: 'str' = 'POST', rankings: 'Optional[FrameLike]' = None, seed: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Dict[str, Union[pl.DataFrame, Any]]'` {#cfb_simulations}

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

### `cfb_standings(games: 'FrameLike', teams: 'FrameLike', *, tiebreaker_depth: 'str' = 'SOS', playoff_seeds: 'Optional[int]' = None, rankings: 'Optional[FrameLike]' = None, tiebreaker_data: 'Optional[Dict[str, FrameLike]]' = None, return_as_pandas: 'bool' = False, rng: 'Optional[np.random.Generator]' = None) -> 'Union[pl.DataFrame, Any]'` {#cfb_standings}

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

### `cfb_teams_crosswalk(*, season: 'Optional[int]' = None, week: 'int' = 1, providers: 'Optional[Sequence[str]]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'DataFrameT'` {#cfb_teams_crosswalk}

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

**Example**

```python
from sportsdataverse.cfb import cfb_teams_crosswalk
xwalk = cfb_teams_crosswalk(season=2024)
row = xwalk.filter(pl.col("espn_team_id") == 194)  # Ohio State

# Pairwise — just ESPN vs Fox

espn_fox = cfb_teams_crosswalk(providers=("espn", "fox"))
```

### `efficiency_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'` {#efficiency_ratings}

One row per team: opponent-adjusted offensive/defensive efficiency.

Fits the offense/defense ridge from `cfb_adjusted_epa` on the
competitive plays in `plays` (`min_competitive_wp <= wp_before <=
max_competitive_wp`) and reshapes the result to one row per team,
including the reference team the ridge's `model.matrix`-style
parameterization drops (its rating is the fitted intercept, i.e. the
league baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying every column in `cfb_adjusted_epa._REQUIRED_COLUMNS` (`game_id`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `wp_before`). Callers pass an already as-of-date-filtered frame; this function is pure. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs. Only `ridge_lambda` is consulted here; defaults to `RatingsConfig` when omitted. |

**Returns**

A `polars.DataFrame` with one row per `team_id`: `team_id` (Utf8), `adj_off_epa` / `adj_def_epa` / `adj_net` (Float64), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model consumes). Empty (zero-row, correctly-typed) when `plays` has no competitive plays.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()

# Custom ridge penalty

from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig
ratings = efficiency_ratings(pbp, config=RatingsConfig(ridge_lambda=100.0))
```

### `espn_cfb_teams(groups=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'` {#espn_cfb_teams}

espn_cfb_teams - look up the college football teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `groups` | `int` | `None` | Used to define different divisions. 80 is FBS, 81 is FCS. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.cfb.espn_cfb_teams.clear_cache().

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
from sportsdataverse.cfb import espn_cfb_teams
teams = espn_cfb_teams()
print(teams.shape)

# Pull FCS teams (group 81)

fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
fcs.head()

# Pipeline next step (build an abbreviation lookup)

teams = espn_cfb_teams()
abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))
```

### `fei_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'` {#fei_ratings}

One row per team: opponent-adjusted per-drive efficiency (FEI-style).

The Fremeau Efficiency Index rates teams on drive value above expectation
given starting field position. The cfbfastR-schema `plays` frame this
package works with carries no starting-field-position column, so this
function uses the documented fallback: per-play EPA summed within each
`(game_id, drive_id)` group stands in for drive value, and that
aggregate is fit through the same opponent-adjustment ridge as
`efficiency_ratings` / `special_teams_ratings` -- no forked
solver. Offline validation against the Fremeau FEI oracle put this
fallback's team ranking at Spearman 0.967.

`cfb_adjusted_epa._prepare` filters to individual pass/rush plays and
is not reused here (drive value should reflect every play on the drive,
special-teams snaps included); the `hfa` treatment is reproduced
directly, matching `special_teams_ratings`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying every column in `cfb_adjusted_epa._REQUIRED_COLUMNS` (`game_id`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `wp_before`) plus `drive_id`. Not pre-aggregated to drives -- this function does that grouping itself. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs. Only `ridge_lambda` is consulted here; defaults to `RatingsConfig` when omitted. |

**Returns**

A `polars.DataFrame` with one row per `team_id` appearing as `pos_team_id` on at least one drive: `team_id` (Utf8), `fei_off` / `fei_def` / `fei_net` (Float64). The ridge's dropped reference team is re-added at the shared intercept (`fei_net == 0.0`). Zero-row (correctly-typed) when `plays` has no rows with a non-null `EPA`.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import fei_ratings
fei = fei_ratings(pbp)
fei.sort("fei_net", descending=True).head()
```

### `fox_cfb_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_boxscore}

Fox Sports CFB boxscore (long: one row per player-stat).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/data`
(the `boxscore` block).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the per-team stat tables to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_boxscore
df = fox_cfb_boxscore("41616")
```

### `fox_cfb_league_leaders(category: 'str' = 'passing', who: 'str' = 'player', page: 'int' = 0, group_id: 'Union[int, str]' = '2', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_league_leaders}

Fox Sports CFB statistical leaders (one row per player/team).

Endpoint: `GET .../bifrost/v1/cfb/league/stats-con/{who}/{category}/{page}`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | `str` | `'passing'` | Stat category -- passing, rushing, receiving, defense, kicking, returning, scoring, yardage (team adds downs, turnovers). Defaults to `"passing"`. |
| `who` | `str` | `'player'` | `"player"` or `"team"`. Defaults to `"player"`. |
| `page` | `int` | `0` | 0-based result page. Defaults to `0`. |
| `group_id` | `Union[int, str]` | `'2'` | Conference/group filter. Defaults to `"2"`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_league_leaders
df = fox_cfb_league_leaders("passing")
```

### `fox_cfb_odds(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_odds}

Fox Sports CFB game odds six-pack (spread / to win / total per team).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/odds`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the six-pack market to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default; empty when no market is posted), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_odds
df = fox_cfb_odds("41616")
```

### `fox_cfb_pbp(game_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_pbp}

Fox Sports CFB play-by-play (one row per play).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/data`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Fox Bifrost event id (e.g. `"41616"`) -- not the ESPN id. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the pbp layout to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_pbp
df = fox_cfb_pbp("41616")
```

### `fox_cfb_play_process(event_id, odds_override: 'Optional[Dict[str, Any]]' = None, process: 'bool' = True, raw: 'bool' = False, **kwargs) -> 'Dict[str, Any]'` {#fox_cfb_play_process}

Build a *processed* CFB play-by-play game from FoxSports as a backup to ESPN.

Where `~sportsdataverse.cfb.cfb_fox_ext.fox_cfb_pbp` returns the raw Fox
play-by-play rows, this runs Fox data through the full ESPN play processor:
it fetches FoxSports Bifrost `cfb/event/{event_id}/data`, adapts it into the
ESPN-`summary` shape via `fox_to_espn_summary`, and runs the same
`~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess` pipeline ESPN games use
-- producing EPA / WPA / advanced box score. The result carries
`source="fox"` so downstream consumers know the provenance (and that
text-derived columns are lower fidelity than the ESPN path).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event_id` |  |  | FoxSports CFB event id (e.g. `41616`). |
| `odds_override` | `Optional[Dict[str, Any]]` | `None` | Optional `{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}` dict. Fox does not expose a clean pre-game spread, so when omitted a neutral pick'em line is used (EPA is unaffected; only the WP model's spread term is neutralized). |
| `process` | `bool` | `True` | If `True` (default) run the full `~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline` (EPA/WPA/box). If `False` run the lighter `~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_cleaning_pipeline`. |
| `raw` | `bool` | `False` | If `True` skip the processor entirely and return the adapted ESPN-summary dict (the input the processor would consume). |

**Returns**

The processed game payload (same keys as `CFBPlayProcess.run_processing_pipeline`) with an added `source="fox"` key. When `raw=True`, the adapted summary dict.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_play_process
game = fox_cfb_play_process(41616)
print(len(game["plays"]), game["source"])
```

### `fox_cfb_schedule(season: 'Optional[int]' = None, *, segment_id: 'Optional[str]' = None, group_id: 'Union[int, str]' = '2', return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_schedule}

Fox Sports CFB full-season schedule (one row per game).

Fox lists games behind a two-step *selector -> segment* flow: `scoreboard/main`
enumerates the season's segments (its `selectionGroupList`), and
`league/scores-segment/{segmentId}` returns the games for one segment.
Pass a `season` to scrape the **whole season** -- every regular week plus
conference championships, bowls, and every College Football Playoff round --
enumerated from the live selector and unioned, deduplicated by `game_id`.

Segment ids encode the phase, not an ESPN-style integer week:
`"{season}-{week}-1"` for a regular-season week, `"{season}-bowls-2"` for
the bowls, `"{season}-cfp-2"` for the CFP (conference championships fall in
the final regular-season week). Pass `segment_id` to fetch just one of them.

The numeric `game_id` is the Fox Bifrost event id that `fox_cfb_pbp` /
`fox_cfb_odds` accept; `week_label` is the section title.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year -> scrape the full season. Ignored when `segment_id` is given; if both are `None` the current segment is returned. |
| `segment_id` | `Optional[str]` | `None` | Explicit Fox segment id (e.g. `"2025-5-1"`, `"2025-cfp-2"`) -> fetch just that segment. |
| `group_id` | `Union[int, str]` | `'2'` | Conference/division group filter. Defaults to `"2"` (FBS). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON (a single segment's `dict`, or a `{segment_id: dict}` map in full-season mode). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with columns `game_id`, `date`, `status`, `week_label`, `home_team`, `home_team_id`, `away_team`, `away_team_id`, `segment_id`; a pandas DataFrame when `return_as_pandas=True`; or raw JSON when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_schedule
season = fox_cfb_schedule(2025)

# Fetch just one segment (a week, or the playoff)

wk5 = fox_cfb_schedule(segment_id="2025-5-1")
cfp = fox_cfb_schedule(segment_id="2025-cfp-2")
```

### `fox_cfb_standings(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_standings}

Fox Sports CFB conference standings for a team's conference.

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/standings`
(the league-wide `league/standings` endpoint returns header-only tables, so
standings are keyed by team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the standings tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_standings
df = fox_cfb_standings("11")
```

### `fox_cfb_team_gamelog(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_team_gamelog}

Fox Sports CFB team game log -- tidy long: one row per (game, stat).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/gamelog`
The endpoint groups team per-game stats by category (passing, rushing,
defense, ...) and season-type split; this flattens to columns
`team_id, season_type, category, game_id, game_date, opponent, stat, value`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to long form; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_gamelog
df = fox_cfb_team_gamelog("11")
```

### `fox_cfb_team_roster(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_team_roster}

Fox Sports CFB team roster (one row per player).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/roster`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)); discover via the league team directory (`cfb/league/teamnav`). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the position-group tables to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_roster
df = fox_cfb_team_roster("11")
```

### `fox_cfb_team_stats(team_id: 'Union[int, str]', *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_team_stats}

Fox Sports CFB team stat leaders (one row per category leader).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/stats`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_id` | `Union[int, str]` |  | Fox Bifrost team id (e.g. `"11"` = Miami (FL)). |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the leader sections to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_team_stats
df = fox_cfb_team_stats("11")
```

### `fox_cfb_teams(*, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#fox_cfb_teams}

Fox Sports CFB team directory (one row per team).

Endpoint: `GET https://api.foxsports.com/bifrost/v1/cfb/league/teamnav`

The team-nav payload is the canonical Fox directory: it maps every team's
Bifrost id to its abbreviation, full name, and web slug. This is the lookup
you need to translate a human team name into the numeric `team_id` the
other `fox_cfb_*` wrappers expect, and it is the Fox side of
`sportsdataverse.cfb.cfb_teams_crosswalk`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the nav items to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with columns `fox_team_id`, `abbreviation`, `name`, `slug`, `color`, `logo_url`; a pandas DataFrame when `return_as_pandas=True`; or the raw JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import fox_cfb_teams
teams = fox_cfb_teams()
fox_id = dict(zip(teams["abbreviation"], teams["fox_team_id"]))
```

### `fox_to_espn_summary(fox_data: 'Dict[str, Any]') -> 'Dict[str, Any]'` {#fox_to_espn_summary}

Adapt a Fox `cfb/event/{id}/data` payload into the ESPN-summary shape.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fox_data` | `Dict[str, Any]` |  | Parsed JSON from `api.foxsports.com/bifrost/v1/cfb/event/{id}/data`. |

**Returns**

A dict shaped like ESPN's `college-football/summary` response (`header` + `drives` + stub `pickcenter`/`boxscore`/...), ready to assign onto `CFBPlayProcess(...).json`.

### `get_2pt_probs(pbp_df: 'Any') -> 'pd.DataFrame'` {#get_2pt_probs}

Two-point-conversion decision surface (cfb4th `get_2pt_wp`).

Treats each row as "the scoring team just made a touchdown; decide between
the extra point and going for two". Enumerates the three point outcomes
(`0` / `1` / `2`) of the try, scores the opponent's ensuing-drive WP for
each from the scoring team's perspective, and combines them with the
two-point conversion probability (bundled CFB model) and the empirical CFB
extra-point make rate (XP_MAKE_PROB`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Any` |  | Play-by-play frame (polars or pandas) carrying the `start.*` state columns in `sportsdataverse.cfb.cfb_fourth_down._PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` plus: * `two_pt_wp` -- `prob_2pt * wp(pts=2) + (1 - prob_2pt) * wp(pts=0)`. * `xp_wp` -- `prob_xp * wp(pts=1) + (1 - prob_xp) * wp(pts=0)` with `prob_xp = _XP_MAKE_PROB`. * `prob_2pt` -- the bundled-model two-point conversion probability. * `two_pt_recommendation` -- `"go_for_2"` iff `two_pt_wp > xp_wp` else `"kick_xp"` (None where the inputs are NaN). * `two_pt_wp_diff` -- `two_pt_wp - xp_wp` (positive => go for 2). When the two-point model isn't bundled (`TWO_PT_MODEL_AVAILABLE` is False) or the required state columns are missing, all decision columns are null -- probabilities are never fabricated.

**Example**

```python
from sportsdataverse.cfb.cfb_two_point import get_2pt_probs
out = get_2pt_probs(touchdown_rows)
print(out[["two_pt_wp", "xp_wp", "two_pt_recommendation"]].head())
```

### `get_4th_down_probs(pbp_df) -> 'pd.DataFrame'` {#get_4th_down_probs}

Full 4th-down decision surface (cfb4th `add_4th_probs`) + recommendation.

Runs `get_go_wp`, `get_fg_wp`, `get_punt_wp` on the
fourth-down rows and adds the combined option columns plus:

* `fourth_down_recommendation` -- the max-WP choice among `{go, punt,
  field_goal}` (NaN options are excluded; when the FG model isn't bundled,
  `field_goal` is excluded from the comparison).
* `go_wp_diff` / `punt_wp_diff` / `fg_wp_diff` -- each option's WP minus
  the recommended option's WP (the recommended option's diff is 0, the others
  <= 0). NaN where the option WP is NaN.
* `go_boost` -- cfb4th's headline number: `100 * (go_wp - max(fg_wp,
  punt_wp))` in percentage points.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the `start.*` state columns in PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` with the decision columns added. Empty input returns the input plus empty decision columns.

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_4th_down_probs
out = get_4th_down_probs(fourth_down_rows)
print(out[["go_wp", "punt_wp", "fg_wp", "fourth_down_recommendation"]].head())
```

### `get_cfb_teams(return_as_pandas=False) -> 'pl.DataFrame'` {#get_cfb_teams}

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
| `logo` | character | Team or league logo URL. |
| `logo_dark` | character | Dark-mode logo URL. |

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

### `get_fg_wp(pbp_df) -> 'pd.DataFrame'` {#get_fg_wp}

Expected win probability of attempting a field goal (cfb4th `get_fg_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `fg_make_prob`, `make_fg_wp`, `miss_fg_wp` and `fg_wp` (= make_prob*make_wp + (1-make_prob)*miss_wp, from the kicking team's perspective). All four are NaN when the FG model is not bundled (`FG_MODEL_AVAILABLE` is False) -- probabilities are never fabricated.

### `get_go_wp(pbp_df) -> 'pd.DataFrame'` {#get_go_wp}

Expected win probability of going for it on 4th down (cfb4th `get_go_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the `start.*` state columns in PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` plus `go_wp` (prob-weighted WP of going for it), `first_down_prob` (P(conversion)), `wp_succeed` (mean WP over conversion outcomes) and `wp_fail` (mean WP over failure outcomes). `go_wp` is always in [0, 1]; the conditional columns are in [0, 1] but can be NaN for degenerate goal-line plays where one outcome bucket is empty (matches the R reference `pivot_wider` NA behavior).

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_go_wp
out = get_go_wp(fourth_down_rows)
print(out[["go_wp", "first_down_prob"]].head())
```

### `get_punt_wp(pbp_df) -> 'pd.DataFrame'` {#get_punt_wp}

Expected win probability of punting on 4th down (cfb4th `get_punt_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `punt_wp` (prob-weighted WP of punting, from the punting team's perspective). `punt_wp` is NaN where the punt end-yardline distribution has no support for the play's `yards_to_goal` (e.g. inside the 31, where punting is dominated and the cfb4th table is empty -- matching the R reference's left-join NA behavior).

### `predict_margin(home_adj_net: 'float', away_adj_net: 'float', neutral: 'bool', *, era: 'str' = 'modern') -> 'float'` {#predict_margin}

Expected home scoring margin from the two net ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_net` | `float` |  | Home team's opponent-adjusted net rating (`adj_net` from `cfb_ratings.efficiency_ratings`). |
| `away_adj_net` | `float` |  | Away team's opponent-adjusted net rating. |
| `neutral` | `bool` |  | Whether the game is at a neutral site (no home-field advantage). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying the fitted `net_points_scale` and `hfa_epa`. |

**Returns**

The expected margin (home minus away), in points: `net_points_scale * (home_adj_net - away_adj_net + 2 * hfa_epa)` on a home field, or without the `2 * hfa_epa` term on a neutral one. `net_points_scale` converts the EPA-per-play rating differential into points; the HFA is the ratings ridge's native home coefficient applied component-wise (home_off +hfa_epa, home_def -hfa_epa => net +2*hfa_epa), an EPA-scale additive that lands in the margin (~1.27 pt) and leaves totals untouched. See `predict_total`.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import predict_margin
predict_margin(0.30, 0.10, neutral=False)
```

### `predict_total(home_adj_off: 'float', home_adj_def: 'float', away_adj_off: 'float', away_adj_def: 'float', game_pace: 'float', *, era: 'str' = 'modern') -> 'float'` {#predict_total}

Expected combined point total from the four efficiency ratings + tempo.

Fitted linear model `total_intercept + total_scale * sum4 + total_pace_scale *
game_pace`, where `sum4 = home_adj_off + away_adj_def + away_adj_off +
home_adj_def`. The four ratings are summed because each side's scoring rises
with its own offense and with the opponent's EPA-*allowed* (`adj_def` is
lower = better defense). `game_pace` (the matchup's expected scrimmage plays,
`home_off_pace * away_off_pace / league_avg_pace`) enters because a total is a
*sum* -- tempo scales both sides' points the same way, so it compounds into the
total (whereas in the margin, a differential, pace cancels). All three
coefficients are fitted on 2023 actual totals.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_off` | `float` |  | Home offense adjusted EPA/play (`adj_off_epa`). |
| `home_adj_def` | `float` |  | Home defense adjusted EPA/play allowed (`adj_def_epa`). |
| `away_adj_off` | `float` |  | Away offense adjusted EPA/play. |
| `away_adj_def` | `float` |  | Away defense adjusted EPA/play allowed. |
| `game_pace` | `float` |  | Expected scrimmage plays for the matchup, i.e. `home_off_pace * away_off_pace / league_avg_pace` from the ratings' `off_pace` column (`cfb_predict_games` computes this for you). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying the fitted `total_intercept` / `total_scale` / `total_pace_scale`. |

**Returns**

The expected combined total points.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import predict_total
predict_total(0.20, -0.05, 0.10, 0.02, game_pace=66.0)
```

### `scoreboard_event_parsing(event)` {#scoreboard_event_parsing}

Internal helper that flattens an ESPN scoreboard event dict into a shape

suitable for `pd.json_normalize`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `dict` |  | A single scoreboard `events[*]` entry from the ESPN college-football scoreboard API. |

**Returns**

The same event dict, mutated in place with `home`/`away` copies of the competitors and trimmed of unused link/odds keys.

**Example**

```python
from sportsdataverse.cfb import espn_cfb_schedule
sched = espn_cfb_schedule(dates=2023, week=5)
```

### `special_teams_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'` {#special_teams_ratings}

One row per team: a per-unit special-teams EPA composite.

Special teams was empirically found NOT to obey the offense-minus-defense
symmetry `efficiency_ratings` / `fei_ratings` rely on, and not
to benefit from opponent adjustment, when validated against the 2023 SP+
special-teams oracle (`tests/fixtures/cfb_prediction/sp_plus_2023.parquet`
`sp_special`):

* The executing `pos_team` owns the EPA on a kickoff / punt / field
  goal. The `def_pos_team` "coverage" side reflects the opposing
  returner's skill, not the coverage team's, and is not recoverable from
  EPA -- adding any coverage unit *lowers* SP+ agreement (0.77 -> 0.58),
  so coverage/defense units are excluded entirely (see
  ST_UNIT_PATTERNS`).
* The opponent-adjustment ridge (`cfb_adjusted_epa._fit_opponent_ridge`)
  *hurts* agreement (0.72 vs 0.77) -- special teams is only weakly
  opponent-dependent, so this function does not fit a ridge at all.
* Splitting the offense-side plays into per-phase units (field goal, punt,
  kick return) and standardizing each separately, then summing the
  z-scores, is what helps: it reached Spearman 0.768 against SP+, versus
  0.703 for a single-unit offense-minus-intercept ridge fit.

`adj_st_epa` is therefore the sum, over the three units in
ST_UNIT_PATTERNS`, of each unit's z-scored per-team mean EPA. A
team with no plays in a given unit contributes 0 for that unit (not a
penalty). `config` is accepted for signature parity with
`efficiency_ratings` / `fei_ratings` but is unused -- there is
no ridge (and therefore no `ridge_lambda`) in this recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying `game_id`, `pos_team_id`, `EPA`, and `play_type`. Not pre-filtered to special-teams plays -- this function does that filtering itself. |
| `config` | `RatingsConfig \| None` | `None` | Unused (kept for signature parity across the three rating functions). See the note above. |

**Returns**

A `polars.DataFrame` with one row per `team_id` appearing anywhere in `plays`: `team_id` (Utf8), `adj_st_epa` (Float64, the sum of per-unit z-scored executing-team mean EPA). Teams with no special-teams plays get `adj_st_epa == 0.0`. Zero-row (correctly-typed) when `plays` has no special-teams plays.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import special_teams_ratings
st = special_teams_ratings(pbp)
st.sort("adj_st_epa", descending=True).head()
```

### `win_prob_from_margin(exp_margin: 'float', *, era: 'str' = 'modern') -> 'float'` {#win_prob_from_margin}

Home win probability from an expected margin via the Gaussian CDF.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home margin in points (e.g. from `predict_margin`). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying `margin_sd`. |

**Returns**

`Phi(exp_margin / margin_sd)` -- the probability the home team wins under a `Normal(exp_margin, margin_sd**2)` margin model. `0.5` at a zero expected margin.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import win_prob_from_margin
win_prob_from_margin(7.0)
```

### `yahoo_cfb_boxscore(game_id: 'Union[int, str]', *, return_parsed: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Dict[str, Any]'` {#yahoo_cfb_boxscore}

Yahoo CFB boxscore — raw JSON passthrough (parsing not yet implemented).

Wraps the editorial `boxscore/{game_id}` resource. The payload uses a
normalized decoder-dictionary schema
(`player_stats[playerId][variation][stat_type]=value` joined against the
`stat_types`/`stat_categories` dictionaries). Flattening that into
tidy frames is a follow-up; until then this returns the raw JSON `dict`
and **fails fast** if a parsed frame is requested rather than silently
ignoring `return_parsed`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[int, str]` |  | Dotted Yahoo game id (e.g. `"ncaaf.g.202509200023"`). |
| `return_parsed` | `bool` | `False` | Must be `False` (the default). Passing `True` raises `NotImplementedError` because parsing is not implemented. |
| `return_as_pandas` | `bool` | `False` | Accepted for signature parity with the sibling wrappers; has no effect while only raw output is supported. |

**Returns**

The raw editorial boxscore JSON as a `dict` (`service.boxscore`).

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_boxscore
raw = yahoo_cfb_boxscore("ncaaf.g.202509200023")
```

### `yahoo_cfb_player_season_stats(season: 'int' = 2024, *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, qualified: 'bool' = False, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_player_season_stats}

Yahoo CFB player season stats (modern; one wide row per player).

Wraps the shangrila `leagueStatsIndividual` query, which returns every
stat group (passing/rushing/receiving/...) in one call, pivoted wide with
one column per `statId`. NCAAF data is available 2013-present.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of players to request. Defaults to `200`. |
| `qualified` | `bool` | `False` | Restrict to qualified leaders only. Defaults to `False`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes a self-describing `season` column.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_player_season_stats
df = yahoo_cfb_player_season_stats(season=2024)
```

### `yahoo_cfb_player_season_stats_legacy(season: 'int' = 2024, category: 'str' = 'Passing', sort_stat: 'str' = 'PASSING_YARDS', *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_player_season_stats_legacy}

Yahoo CFB legacy per-category player leaders (one wide row per player).

Wraps the legacy `seasonStatsFootball{Category}Ncaaf` query (one stat
category per call), pivoted wide with one column per `statId`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `category` | `str` | `'Passing'` | Stat category, one of `{"Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns"}`. Defaults to `"Passing"`. |
| `sort_stat` | `str` | `'PASSING_YARDS'` | Required `FootballStatId` to sort by (see the catalog vocab). Defaults to `"PASSING_YARDS"`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of players to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `category` columns.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_player_season_stats_legacy
df = yahoo_cfb_player_season_stats_legacy(
    season=2024, category="Rushing", sort_stat="RUSHING_YARDS"
)
```

### `yahoo_cfb_scoreboard(season: 'int', week: 'int' = 1, *, count: 'int' = 500, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_scoreboard}

Yahoo CFB scoreboard (one row per game).

Wraps the editorial `scoreboard` resource and flattens the `games` map.
`season` is required — there is no meaningful default for a weekly
scoreboard and the API has no concept of "current season". The full raw
payload also carries teams/leagues/odds maps (use `return_parsed=False`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (required). |
| `week` | `int` | `1` | Schedule week number. Defaults to `1`. |
| `count` | `int` | `500` | Maximum number of games to request. Defaults to `500`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the games map to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with one row per game, a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `week` columns.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_scoreboard
df = yahoo_cfb_scoreboard(season=2024, week=1)
```

### `yahoo_cfb_team_season_stats(season: 'int' = 2024, *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_team_season_stats}

Yahoo CFB team season stats (modern; one wide row per team).

Wraps the shangrila `leagueStatsByTeam` query (all stat groups in one
call, pivoted wide with one column per `statId`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of teams to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes a self-describing `season` column.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_team_season_stats
df = yahoo_cfb_team_season_stats(season=2024)
```

### `yahoo_cfb_team_season_stats_legacy(season: 'int' = 2024, category: 'str' = 'Passing', sort_stat: 'str' = 'PASSING_YARDS', *, league_structure: 'str' = 'ncaaf.struct.div.1', count: 'int' = 200, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_team_season_stats_legacy}

Yahoo CFB legacy per-category team stats (one wide row per team).

Wraps the legacy `seasonTeamStatsFootball{Category}` query (one stat
category per call), pivoted wide with one column per `statId`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | Season year (2013-present). Defaults to `2024`. |
| `category` | `str` | `'Passing'` | Stat category, one of `{"Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns", "Kickoffs", "Offense"}`. Defaults to `"Passing"`. |
| `sort_stat` | `str` | `'PASSING_YARDS'` | Required `FootballStatId` to sort by. Defaults to `"PASSING_YARDS"`. |
| `league_structure` | `str` | `'ncaaf.struct.div.1'` | Yahoo league-structure id (division filter). Defaults to `"ncaaf.struct.div.1"` (FBS). |
| `count` | `int` | `200` | Maximum number of teams to request. Defaults to `200`. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten to a DataFrame; if `False` return the raw JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A wide polars DataFrame (default), a pandas DataFrame when `return_as_pandas=True`, or the raw JSON `dict` when `return_parsed=False`. Includes self-describing `season` and `category` columns.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_team_season_stats_legacy
df = yahoo_cfb_team_season_stats_legacy(
    season=2024, category="Rushing", sort_stat="RUSHING_YARDS"
)
```

### `yahoo_cfb_teams(season: 'int', week: 'int' = 1, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame', Dict[str, Any]]"` {#yahoo_cfb_teams}

Yahoo CFB team directory (one row per team).

Yahoo has no standalone teams resource (the documented
`sports.league.teams` resource 404s without auth). Instead the editorial
`scoreboard` payload is "fat": one call embeds the full ~186-team
directory under `service.scoreboard.teams` keyed by the dotted
`ncaaf.t.<id>` team id. This wrapper pulls that map for the requested
`(season, week)` and projects it to the directory columns -- it is the
Yahoo side of `sportsdataverse.cfb.cfb_teams_crosswalk`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (required; the scoreboard is fetched to obtain the embedded teams map). |
| `week` | `int` | `1` | Schedule week used to fetch the scoreboard. Defaults to `1`. The embedded directory is the full league list regardless of week. |
| `return_parsed` | `bool` | `True` | If `True` (default) flatten the teams map to a DataFrame; if `False` return the raw scoreboard JSON `dict`. |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; otherwise polars. Ignored when `return_parsed=False`. |

**Returns**

A polars DataFrame (default) with one row per team -- columns `team_id`, `abbreviation`, `display_name`, `full_name`, `location`, `nickname`, `conference`, `conference_abbreviation`, `conference_id`, `division`, `division_id`, `seatgeek_id` -- a pandas DataFrame when `return_as_pandas=True`, or the raw scoreboard JSON `dict` when `return_parsed=False`.

**Example**

```python
from sportsdataverse.cfb import yahoo_cfb_teams
teams = yahoo_cfb_teams(season=2024)
abbr = dict(zip(teams["team_id"], teams["abbreviation"]))
```
