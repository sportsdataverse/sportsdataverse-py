---
title: "NFL — additional Python functions — NFL Pro: nfl_ngs"
sidebar_label: "NFL Pro: nfl_ngs"
sidebar_position: 3
description: "NFL — additional Python functions — NFL Pro: nfl_ngs — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — NFL Pro: nfl_ngs

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

### nfl_ngs_play_is_highlight {#nfl_ngs_play_is_highlight}

`nfl_ngs_play_is_highlight(game_id, play_id, return_as_pandas: 'bool' = False)`

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

### nfl_ngs_ryoe {#nfl_ngs_ryoe}

`nfl_ngs_ryoe(seasons: 'Union[int, Sequence[int]]', *, min_attempts: 'int' = 20, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

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

### nfl_ngs_separation_oe {#nfl_ngs_separation_oe}

`nfl_ngs_separation_oe(seasons: 'Union[int, Sequence[int]]', *, min_targets: 'int' = 20, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

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

### nfl_ngs_statboard {#nfl_ngs_statboard}

`nfl_ngs_statboard(stat_type: 'str' = 'passing', season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)`

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
