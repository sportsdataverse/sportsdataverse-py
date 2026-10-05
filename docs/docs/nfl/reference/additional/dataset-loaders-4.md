---
title: "NFL — additional Python functions — Dataset loaders: players–trades"
sidebar_label: "Dataset loaders: players–trades"
sidebar_position: 6
description: "NFL — additional Python functions — Dataset loaders: players–trades — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Dataset loaders: players–trades

### load_players {#load_players}

`load_players(return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

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
| `source` | `str` | `'nflverse'` | Which player-master release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse seven-system `players.parquet` identity master described above. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_players` release built by `sportsdataverse.nfl.build_nfl_players` from the **public NFL Shield / ESPN-athletes** surface, with `gsis_id` and the other cross-system IDs enriched by a best-effort join against the nflverse player master. The SDV tier is a partial build: its columns are a subset of nflverse's and cross-system IDs are sparser (notably pre-2016), though `espn_id` is populated. The default stays `"nflverse"`. Any other value raises `ValueError`. |

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

### load_rosters {#load_rosters}

`load_rosters(seasons: 'List[int]', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load NFL season roster data for the requested seasons.

Reads nflverse's published season-roster parquet (one row per player per
season). nflverse's roster product is the union of three upstream tiers --
NFL Next Gen Stats (2016+), the credentialed NFL Data Exchange (2002-2015),
and the public NFL Shield endpoint (all seasons) -- so it carries densely
populated cross-system identifier columns (`espn_id`, `sportradar_id`,
`yahoo_id`, `pff_id`, `pfr_id`, ...) alongside biographical and
depth-chart fields. This is the richest roster surface; prefer it whenever a
network round trip to nflverse is acceptable.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Seasons to load (e.g. `[2024]` or `range(2020, 2025)`). A single `int` is accepted and wrapped. 1920 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe (default). |
| `source` | `str` | `'nflverse'` | Which roster release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse season-roster releases described above -- the full multi-source product (1920+, densely populated cross-system IDs). `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_rosters` release built by `sportsdataverse.nfl.build_nfl_rosters` from the **public NFL Shield / ESPN** surface, with cross-system IDs and `college` enriched by a best-effort join against the nflverse player master (`sportsdataverse.nfl.load_nfl_players`, on `gsis_id`; skipped if that load fails). The SDV tier is a partial build: its 30 columns are a subset of nflverse's 36, and cross-system IDs are sparser pre-2016. It covers only the published seasons (rosters 2022+). The default stays `"nflverse"`. Any other value raises `ValueError`. |

**Returns**

Polars dataframe of season rosters for the requested seasons (`pandas.DataFrame` when `return_as_pandas=True`).

**Example**

```python
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])

# Multi-season range

rosters = load_nfl_rosters(seasons=range(2020, 2025))

# Filter to a single team

import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")

# SDV-native rosters (public Shield/ESPN build; published seasons 2022+; 30-column subset of nflverse, sparser cross-IDs pre-2016)

rosters_sdv = load_nfl_rosters(seasons=[2023], source="sdv")
rosters_sdv.select(["season", "team", "full_name", "gsis_id"]).head()
```

### load_rosters_weekly {#load_rosters_weekly}

`load_rosters_weekly(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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

### load_schedules {#load_schedules}

`load_schedules(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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

### load_snap_counts {#load_snap_counts}

`load_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

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

### load_team_stats {#load_team_stats}

`load_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

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
| `game_id` | character | Ten digit identifier for NFL game. |
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
| `passing_10` | integer | Number of the team's completed passes that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_16` | integer | Number of the team's completed passes that gained 16 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_20` | integer | Number of the team's completed passes that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `passing_40` | integer | Number of the team's completed passes that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | integer | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | integer | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | integer | The number of rushes with a lost fumble. |
| `rushing_first_downs` | integer | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `rushing_10` | integer | Number of the team's runs that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_12` | integer | Number of the team's runs that gained 12 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_20` | integer | Number of the team's runs that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `rushing_40` | integer | Number of the team's runs that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
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
| `receiving_10` | integer | Number of the team's receptions that gained 10 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_16` | integer | Number of the team's receptions that gained 16 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_20` | integer | Number of the team's receptions that gained 20 or more yards (one of nflfastR's 'explosive' play thresholds). |
| `receiving_40` | integer | Number of the team's receptions that gained 40 or more yards (one of nflfastR's 'explosive' play thresholds). |
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
| `def_punt_blocks` | integer | Number of opponent punts blocked by the team's defense. |
| `def_pat_blocks` | integer | Number of opponent extra point attempts blocked by the team's defense. |
| `def_fg_blocks` | integer | Number of opponent field goal attempts blocked by the team's defense. |
| `def_2pt_atts` | integer | Number of defensive two-point conversion returns attempted by the team (nflfastR stat id 403). |
| `def_2pt_made` | integer | Number of successful defensive two-point conversion returns by the team (nflfastR stat id 404). |
| `misc_yards` | integer | Yards gained by the team through miscellaneous means not captured in standard rushing, passing, or return categories. |
| `fumble_recovery_own` | integer | Number of the team's own fumbles that were recovered by the team itself. |
| `fumble_recovery_yards_own` | integer | Total yards gained after recovering their own fumbles. |
| `fumble_recovery_opp` | integer | Number of fumbles recovered by the team from the opposing offense (defensive fumble recoveries). |
| `fumble_recovery_yards_opp` | integer | Total yards gained by the team on returns of opponent fumble recoveries. |
| `fumble_recovery_tds` | integer | Number of touchdowns scored by the team on fumble recoveries (own or opponent). |
| `penalties` | integer | Total number of penalties. |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer | Number of timeouts remaining or used by the team during the game or period covered. |
| `fumbles_forced_by_opp` | integer | Fumbles by the team's players that were forced by the opponent, counted across all units (offense, defense and special teams). |
| `fumbles_not_forced` | integer | Fumbles by the team's players that were not forced by the opponent, counted across all units. |
| `fumbles_out_of_bounds` | integer | Fumbles by the team's players where the ball went out of bounds, forced or not; each is also counted in fumbles_forced_by_opp or fumbles_not_forced. |
| `fumbles_total` | integer | Total fumbles by the team's players across all units; equals fumbles_forced_by_opp + fumbles_not_forced. |
| `fumbles_lost_total` | integer | Total fumbles lost by the team's players, counted across all units. |
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
| `pt_att` | integer | Number of punts kicked by the team; blocked punts are counted separately in pt_blocked. |
| `pt_blocked` | integer | Number of the team's punts that were blocked. |
| `pt_long` | integer | Length in yards of the team's longest punt; null when the team had no kicked punt (never 0 in the 2024 sample). |
| `pt_yards` | integer | Total gross yards of the team's punts. |
| `pt_inside_20` | integer | Number of the team's punts credited as ending inside the opponent's 20-yard line (nflfastR defines the spot as where the return ended). |
| `pt_out_of_bounds` | integer | Number of the team's punts that went out of bounds without a return. |
| `pt_downed` | integer | Number of the team's punts that were downed without a return. |
| `pt_touchback` | integer | Number of the team's punts that resulted in a touchback. |
| `pt_fair_caught` | integer | Number of the team's punts that were fair caught by the opponent. |
| `pt_returned` | integer | Number of the team's punts that were returned by the opponent. |
| `pt_return_yards` | integer | Punt return yards gained by the opponent on the team's punts; can be negative (minimum -4 in the 2024 sample). |
| `pt_return_tds` | integer | Number of the team's punts that the opponent returned for a touchdown. |
| `pt_net_yards` | integer | Net punting yards: pt_yards minus pt_return_yards minus 20 yards per touchback. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])

# Regular-season-only team stats

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

# SDV-native team stats (built from SDV play-by-play)

sdv = load_nfl_team_stats(seasons=[2024], source="sdv")
```

### load_teams {#load_teams}

`load_teams(return_as_pandas=False) -> 'pl.DataFrame'`

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
| `team_name` | character | Team nickname. |
| `team_id` | integer | ESPN team id. |
| `team_nick` | character | Team nickname or mascot name (e.g., 'Chiefs', 'Patriots'). |
| `team_conf` | character | Conference the team belongs to (e.g., 'AFC', 'NFC'). |
| `team_division` | character | Division within the conference the team belongs to (e.g., 'AFC East'). |
| `team_color` | character | Primary team color. |
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

### load_trades {#load_trades}

`load_trades(return_as_pandas=False) -> 'pl.DataFrame'`

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
