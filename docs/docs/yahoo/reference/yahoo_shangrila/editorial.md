---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: boxscore"
sidebar_label: "Editorial: boxscore"
sidebar_position: 1
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: boxscore — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: boxscore

## yahoo_editorial_boxscore

Full game box score + play-by-play (normalized stat dictionaries)

**Endpoint URL:** `GET https://api-secure.sports.yahoo.com/v1/editorial/s/boxscore/{game_id}`

**Valid URL:** [https://api-secure.sports.yahoo.com/v1/editorial/s/boxscore/ncaaf.g.202509200023](https://api-secure.sports.yahoo.com/v1/editorial/s/boxscore/ncaaf.g.202509200023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |
| `v` | `v` |  |  | `Y` | v query parameter. |
| `polling` | `polling` |  |  | `Y` | polling query parameter. |

### Returns {#yahoo_editorial_boxscore-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the feed's id-keyed collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**player_stats**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `ncaaf_stat_type_102` | character | Value recorded for the "Completions" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.102, abbreviated "Comp"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_103` | character | Value recorded for the "Attempts" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.103, abbreviated "Att"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_105` | character | Value recorded for the "Yards" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.105, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_104` | character | Value recorded for the "Completion Percentage" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.104, abbreviated "Pct"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_106` | character | Value recorded for the "Yards per Attempt" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.106, abbreviated "Y/A"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_111` | character | Value recorded for the "Sacks" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.111, abbreviated "Sack"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_112` | character | Value recorded for the "Yards Lost" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.112, abbreviated "YdsL"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_108` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.108, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_109` | character | Value recorded for the "Interceptions" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.109, abbreviated "Int"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_113` | character | Value recorded for the "QB Rating" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.113, abbreviated "QBRat"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_202` | character | Value recorded for the "Rushes" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.202, abbreviated "Rush"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_203` | character | Value recorded for the "Yards" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.203, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_205` | character | Value recorded for the "Average" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.205, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_206` | character | Value recorded for the "Longest" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.206, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_207` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.207, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_302` | character | Value recorded for the "Receptions" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.302, abbreviated "Rec"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_303` | character | Value recorded for the "Yards" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.303, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_305` | character | Value recorded for the "Average" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.305, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_306` | character | Value recorded for the "Longest" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.306, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_309` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.309, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_502` | character | Value recorded for the "Kickoff Returns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.502, abbreviated "KR"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_503` | character | Value recorded for the "Yards" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.503, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_505` | character | Value recorded for the "Average" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.505, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_506` | character | Value recorded for the "Longest" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.506, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_507` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.507, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_508` | character | Value recorded for the "Punt Returns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.508, abbreviated "PR"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_509` | character | Value recorded for the "Yards" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.509, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_511` | character | Value recorded for the "Average" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.511, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_512` | character | Value recorded for the "Longest" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.512, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_513` | character | Value recorded for the "Touchdowns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.513, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_411` | character | Value recorded for the "Extra Points Made" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.411, abbreviated "XPM"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_412` | character | Value recorded for the "Extra Points Attempted" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.412, abbreviated "XPA"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_407` | character | Value recorded for the "Total Made" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.407, abbreviated "FGM"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_408` | character | Value recorded for the "Total Attempted" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.408, abbreviated "FGA"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_410` | character | Value recorded for the "Long" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.410, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_409` | character | Value recorded for the "Percent" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.409, abbreviated "Pct"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_602` | character | Value recorded for the "Punts" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.602, abbreviated "Punt"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_604` | character | Value recorded for the "Average" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.604, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_608` | character | Value recorded for the "Longest" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.608, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_702` | character | Value recorded for the "Solo Tackles" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.702, abbreviated "Solo"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_703` | character | Value recorded for the "Tackle Assists" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.703, abbreviated "Ast"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_705` | character | Value recorded for the "Sacks" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.705, abbreviated "Sack"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_706` | character | Value recorded for the "Yards Lost" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.706, abbreviated "YdsL"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_710` | character | Value recorded for the "Passes Defended" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.710, abbreviated "PD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_707` | character | Value recorded for the "Interceptions" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.707, abbreviated "Int"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_708` | character | Value recorded for the "Yards" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.708, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_709` | character | Value recorded for the "Interception Touchdowns" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.709, abbreviated "IntTD"), for the player or team on this boxscore row. |

**team_stats**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `ncaaf_stat_type_919` | character | Value recorded for the "First Downs" statistic in Yahoo's Team category (stat type ncaaf.stat_type.919, abbreviated "Firsts"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_945` | character | Value recorded for the "Total Yards" statistic in Yahoo's Team category (stat type ncaaf.stat_type.945, abbreviated "TOTYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_950` | character | Value recorded for the "Turnovers" statistic in Yahoo's Team category (stat type ncaaf.stat_type.950, abbreviated "TO"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_937` | character | Value recorded for the "Passes for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.937, abbreviated "PASSF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_936` | character | Value recorded for the "Rushes for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.936, abbreviated "RUSHF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_938` | character | Value recorded for the "Penalties for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.938, abbreviated "PENF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_941` | character | Value recorded for the "Third Down Efficiency" statistic in Yahoo's Team category (stat type ncaaf.stat_type.941, abbreviated "3DE"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_944` | character | Value recorded for the "Fourth Down Efficiency" statistic in Yahoo's Team category (stat type ncaaf.stat_type.944, abbreviated "4DE"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_946` | character | Value recorded for the "Total Plays" statistic in Yahoo's Team category (stat type ncaaf.stat_type.946, abbreviated "TOTPLAYS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_952` | character | Value recorded for the "Avg Gain Per Play" statistic in Yahoo's Team category (stat type ncaaf.stat_type.952, abbreviated "AVGPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_921` | character | Value recorded for the "Net Yards Rushing" statistic in Yahoo's Team category (stat type ncaaf.stat_type.921, abbreviated "RYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_920` | character | Value recorded for the "Rushes" statistic in Yahoo's Team category (stat type ncaaf.stat_type.920, abbreviated "Rushes"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_949` | character | Value recorded for the "Yards Per Rush" statistic in Yahoo's Team category (stat type ncaaf.stat_type.949, abbreviated "AVGRYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_947` | character | Value recorded for the "Net Yards Passing" statistic in Yahoo's Team category (stat type ncaaf.stat_type.947, abbreviated "NETPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_951` | character | Value recorded for the "Comp-Att" statistic in Yahoo's Team category (stat type ncaaf.stat_type.951, abbreviated "PASSEFF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_948` | character | Value recorded for the "Yards Per Pass" statistic in Yahoo's Team category (stat type ncaaf.stat_type.948, abbreviated "AVGPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_927` | character | Value recorded for the "Times Sacked" statistic in Yahoo's Team category (stat type ncaaf.stat_type.927, abbreviated "SACKS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_928` | character | Value recorded for the "Yds Lost To Sacks" statistic in Yahoo's Team category (stat type ncaaf.stat_type.928, abbreviated "SACKYD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_926` | character | Value recorded for the "Interceptions" statistic in Yahoo's Team category (stat type ncaaf.stat_type.926, abbreviated "INTS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_929` | character | Value recorded for the "Punts" statistic in Yahoo's Team category (stat type ncaaf.stat_type.929, abbreviated "PUNTS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_930` | character | Value recorded for the "Punt Average" statistic in Yahoo's Team category (stat type ncaaf.stat_type.930, abbreviated "PUNTAVG"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_933` | character | Value recorded for the "Penalties" statistic in Yahoo's Team category (stat type ncaaf.stat_type.933, abbreviated "PEN"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_934` | character | Value recorded for the "Penalty Yards" statistic in Yahoo's Team category (stat type ncaaf.stat_type.934, abbreviated "PENYD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_931` | character | Value recorded for the "Fumbles" statistic in Yahoo's Team category (stat type ncaaf.stat_type.931, abbreviated "FUMB"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_932` | character | Value recorded for the "Fumbles Lost" statistic in Yahoo's Team category (stat type ncaaf.stat_type.932, abbreviated "FUMBLOST"), for the player or team on this boxscore row. |

**aliases**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `stats` | character | Stats. |

**stat_categories**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | character | Display name. |
| `sort` | character | Yahoo composite stat-type id the category sorts on by default (e.g., "ncaaf.stat_type.105"). |
| `stats` | character | Stats. |

**stat_variations**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | character | Display name. |

**stat_types**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | character | Display name. |
| `short_name` | character | Short display name. |

**stat_cut_types**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | character | Display name. |

**games**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `gameid` | character | Date-encoded Yahoo composite game id for this row (e.g., "ncaaf.g.202509200023"). |
| `global_gameid` | character | Yahoo cross-provider game id, distinct from the date-encoded gameid (e.g., "ncaaf.g.13556882"). |
| `start_time` | character | Kickoff time in eastern time zone. |
| `is_time_tba` | logical | Flag indicating that the scheduled start time has not yet been announced. |
| `season_phase_id` | character | Identifier of the season phase the game falls in (e.g., "season.phase.season"). |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `winning_team_id` | character | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `is_rank_upset` | character | Flag indicating that the lower-ranked side won, judged against the teams' poll rankings. |
| `is_spread_upset` | logical | Flag indicating that the winning side was the betting underdog against the closing spread. |
| `outcome_type` | character | Outcome classification for a completed game (e.g., "outcome.type.won", "outcome.type.tied"). |
| `home_team_id` | character | Unique identifier for the home team. |
| `away_team_id` | character | Unique identifier for the away team. |
| `week_number` | character | Week number. |
| `sportacular_url` | character | Deep link into the Yahoo Sportacular mobile app for this game (a "ysportacular://" URL). |
| `status_display_name` | character | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `status_description` | character | Roster status description (e.g. 'Active'). |
| `status_type` | character | Status type. |
| `total_away_points` | character | Points scored by the away team in the game. |
| `current_period_id` | character | Ordinal number of the period currently in progress, counting from 1. |
| `total_home_points` | character | Points scored by the home team in the game. |
| `total_away_shootout_points` | character | Shootout goals converted by the away team, populated only for sports that break ties by shootout. |
| `total_home_shootout_points` | character | Shootout goals converted by the home team, populated only for sports that break ties by shootout. |
| `home_team_stats` | character | JSON-encoded team-stat block for the home team, populated once the game is under way. |
| `away_team_stats` | character | JSON-encoded team-stat block for the away team, populated once the game is under way. |
| `game_period_balls` | character | Balls in the count for the at-bat in progress; baseball only. |
| `game_period_strikes` | character | Strikes in the count for the at-bat in progress; baseball only. |
| `game_period_outs` | character | Outs recorded so far in the current half-inning; baseball only. |
| `yards_to_endzone` | character | Distance from the current ball spot to the opponent's goal line, in yards. |
| `start_yardline` | character | Yard line at the drive start. |
| `distance` | character | Distance value (in feet for shot data; otherwise context-dependent). |
| `down` | character | The down for the given play. |
| `team_in_possession` | character | Yahoo team id of the side currently in possession of the ball. |
| `power_play_strength_home` | character | Number of skaters the home team has on the ice during special-teams play; hockey only. |
| `power_play_strength_away` | character | Number of skaters the away team has on the ice during special-teams play; hockey only. |
| `game_time_elapsed` | character | Playing time elapsed in the game, in seconds. |
| `game_time_elapsed_display` | character | Playing time elapsed formatted for display (e.g., "67:12"), used by sports whose clock counts up. |
| `inning_status` | character | Half-inning indicator for a game in progress (e.g., "Top", "Bottom"); baseball only. |
| `away_timeouts` | character | Away-team timeouts remaining. |
| `home_timeouts` | character | Home-team timeouts remaining. |
| `is_halftime` | character | Flag indicating that the game is currently stopped at halftime. |
| `minimum_periods` | integer | Number of periods a game of this sport runs before overtime is required (4 for football, 9 for baseball). |
| `game_periods` | character | JSON-encoded list of the game's period nodes, each carrying a period number and its display names. |
| `baserunners` | character | JSON-encoded baserunner occupancy for the game in progress; baseball only. |
| `season` | character | Season year. |
| `subleague` | character | Sub-league the game belongs to, for leagues split into constituent circuits. |
| `subleague_display_name` | character | Display name of the sub-league the game belongs to. |
| `agg_score` | character | Aggregate score across the legs of a two-leg tie, populated only for competitions decided on aggregate. |
| `leg_number` | character | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `seatgeek_id` | character | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `last_updated` | character | Last-updated timestamp. |
| `teams` | character | Nested list of member-team membership spans. |
| `play_by_play` | character | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `pitches` | character | JSON-encoded data-island pointer to the game's pitch-level feed; baseball only. |
| `at_bat` | character | JSON-encoded data-island pointer to the game's current at-bat feed; baseball only. |
| `penalty_summary` | character | Whether penalty summary data is available. |
| `scoring_summary` | character | Whether scoring summary data is available. |
| `stat_categories` | character | JSON-encoded pointer to the stat-category dictionary that groups this feed's statistics. |
| `stadium` | character | Name of the stadium |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `stadium_image` | character | JSON-encoded data-island pointer to the venue photograph used on the game page. |
| `attendance` | character | Reported attendance. |
| `lineups` | character | JSON-encoded data-island pointer to the game's lineup collection. |
| `top_performer` | character | JSON-encoded data-island pointer to the game's top-performing players. |
| `players` | character | Nested list of per-player box scores. |
| `byline` | character | News article byline / author. |
| `highlight` | character | JSON-encoded data-island pointer to the game's highlight video. |
| `highlights` | character | Game highlight urls. |
| `live_video` | character | JSON-encoded data-island pointer to the live video stream for the game. |
| `odds` | character | JSON-encoded data-island pointer to the game's odds collection. |
| `current_players` | character | JSON-encoded data-island pointer to the players currently on the field, ice or court. |
| `last_play` | character | Free-text description of the most recent play. |
| `series_type` | character | JSON-encoded data-island pointer to the kind of series the game belongs to. |
| `series_status` | character | JSON-encoded data-island pointer to the current state of the series the game belongs to. |
| `games` | character | Games played. |
| `series_games` | character | JSON-encoded data-island pointer to the games making up the series. |
| `game_details` | character | JSON-encoded data-island pointer to supplementary detail notes for the game. |
| `section_notes` | character | JSON-encoded data-island pointer to editorial section notes attached to the game page. |
| `articles` | character | JSON-encoded data-island pointer to the editorial articles attached to the game. |
| `tweets` | character | JSON-encoded data-island pointer to the social posts attached to the game page. |
| `playoff_round` | character | Playoff round identifier. |
| `media_stream` | character | JSON-encoded data-island pointer to the game's media-stream collection. |
| `playoff_series_status` | character | JSON-encoded data-island pointer to the current state of the playoff series the game belongs to. |
| `playoff_series_details` | character | JSON-encoded data-island pointer to detail about the playoff series the game belongs to. |
| `drives` | character | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `user_teams_game` | character | JSON-encoded data-island pointer to the viewer's followed-team context for the game. |
| `page_metadata` | character | JSON-encoded data-island pointer to the SEO and page metadata for the entity. |
| `penalty_box` | character | JSON-encoded data-island pointer to the game's penalty-box feed; hockey only. |
| `starting_pitchers` | character | JSON-encoded data-island pointer to the game's announced starting pitchers; baseball only. |
| `unrestricted_streams` | character | JSON-encoded data-island pointer to the streams viewable without a subscription. |
| `tv_details` | character | JSON-encoded list of broadcast entries for the game, each carrying a network abbreviation and full channel name (e.g., [{"abbr": "NBC", "name": "NBC/Peacock"}]). |
| `away_seed` | character | Away team's seed. |
| `home_seed` | character | Home team's seed. |
| `navigation_links_tickets_url` | character | Affiliate ticket-purchase URL for the game, pointing at the SeatGeek marketplace. |
| `navigation_links_boxscore_url` | character | Site-relative URL of the game's boxscore page on sports.yahoo.com. |
| `navigation_links_match_page_url` | character | Site-relative URL of the game's match page on sports.yahoo.com. |
| `navigation_links_recap_url` | character | Site-relative URL of the editorial recap article written for the game. |
| `navigation_links_league_home_url` | character | Site-relative URL of the league's home page on sports.yahoo.com. |
| `navigation_links_league_scores_url` | character | Site-relative URL of the league's scoreboard page on sports.yahoo.com. |
| `provider_coverage_score_update_frequency_in_minutes` | character | How often, in minutes, the data provider refreshes the score for this game. |
| `provider_coverage_has_plays` | character | Flag indicating that the data provider supplies play-by-play for this game. |
| `provider_coverage_has_stats` | character | Flag indicating that the data provider supplies box-score statistics for this game. |
| `provider_coverage_has_extended_stats` | character | Flag indicating that the data provider supplies extended statistics beyond the standard box score. |
| `provider_coverage_has_final_stats` | character | Flag indicating that the data provider has published final, official statistics for the game. |

**gameplayoff_round**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gameplayoff_series_status**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gameplayoff_series_details**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gamescore**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gamecurrent_players**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `current_batter` | character | Yahoo composite player id of the batter at the plate; baseball only. |
| `current_pitcher` | character | Yahoo composite player id of the pitcher on the mound; baseball only. |
| `base_runners` | character | JSON-encoded list of runners currently occupying bases; baseball only. |
| `due_ups` | character | JSON-encoded list of the batters due up next; baseball only. |

**gamelast_play**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `play_id` | character | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `play_text` | character | Free-form text description of the play from the CFBD feed. |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | character | Game clock value. |
| `team` | character | Team-side label or team identifier. |
| `is_scoring_play` | integer | Flag indicating that the play put points on the board (1 = scoring play, 0 = not). |

**gamepenalty_box**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gameplay_by_play**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `play_id` | character | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | character | Game clock value. |
| `down` | character | The down for the given play. |
| `distance` | character | Distance value (in feet for shot data; otherwise context-dependent). |
| `team` | character | Team-side label or team identifier. |
| `yardline` | character | Ball spot at the snap as rendered on the scoreboard (e.g., "MICH 35"). |
| `yards_to_endzone` | character | Distance from the current ball spot to the opponent's goal line, in yards. |
| `type` | character | Record type / category. |
| `yards` | character | Total yards gained on the drive. |
| `text` | character | Text description of the play / record. |
| `play_time` | character | Wall-clock instant the play was recorded, as a Unix epoch timestamp in seconds. |

**gameat_bat**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gamescoring_summary**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `play_id` | character | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | character | Game clock value. |
| `away_score` | character | Away team score at the time of the play. |
| `home_score` | character | Home team score at the time of the play. |
| `team` | character | Team-side label or team identifier. |
| `score_type` | character | Kind of score produced by the scoring play (e.g., "TD" touchdown, "FG" field goal, "SF" safety). |
| `xp_type` | character | Conversion attempted after the touchdown ("EP" for an extra point, "2PT" for a two-point try, "0" when none was attempted). |
| `players` | character | Nested list of per-player box scores. |
| `text` | character | Text description of the play / record. |

**gamemedia_stream**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `media_type` | character | Kind of item carried in the media stream (e.g., "play", "video"). |
| `media_source` | character | Feed the media item was produced from (e.g., "play_by_play"). |
| `sequence_id` | integer | Monotonic sequence number that orders items within the game's media stream. |
| `external_id` | character | Provider-side identifier for the media item, matching the play id it accompanies. |
| `timestamp` | character | Response timestamp (ISO 8601). |
| `official` | logical | Flag indicating that the media item comes from the official league feed rather than an editorial source. |

**gamedrives**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `id` | character | ID of the player in the 'name' column. |
| `team` | character | Team-side label or team identifier. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `num_plays` | character | Number of plays the offense ran on the drive. |
| `yards_covered` | character | Net yards the offense gained over the course of the drive. |
| `start_yardline` | character | Yard line at the drive start. |
| `yardline_text` | character | Ball spot where the drive started, rendered as it appears on the scoreboard (e.g., "NEB 25"). |
| `plays` | character | Total qualifying passing plays included in the WEPA calculation. |
| `result` | character | Result. |
| `start_time_clock` | character | Game clock reading when the drive began, as MM:SS remaining in its period. |
| `start_time_period` | character | Period number in which the drive began. |
| `end_time_clock` | character | Game clock reading when the drive ended, as MM:SS remaining in its period. |
| `end_time_period` | integer | Period number in which the drive ended. |

**gamelineups**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `home_lineup_order` | character | JSON-encoded batting or lineup order for the home team, empty for sports without a fixed order. |
| `away_lineup_order` | character | JSON-encoded batting or lineup order for the away team, empty for sports without a fixed order. |
| `home_lineup_all_ncaaf_p_457863_player_id` | character | Yahoo composite player id ncaaf.p.457863, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333433_player_id` | character | Yahoo composite player id ncaaf.p.333433, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_461405_player_id` | character | Yahoo composite player id ncaaf.p.461405, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457834_player_id` | character | Yahoo composite player id ncaaf.p.457834, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_469568_player_id` | character | Yahoo composite player id ncaaf.p.469568, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333434_player_id` | character | Yahoo composite player id ncaaf.p.333434, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333286_player_id` | character | Yahoo composite player id ncaaf.p.333286, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_404124_player_id` | character | Yahoo composite player id ncaaf.p.404124, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322801_player_id` | character | Yahoo composite player id ncaaf.p.322801, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457875_player_id` | character | Yahoo composite player id ncaaf.p.457875, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_469559_player_id` | character | Yahoo composite player id ncaaf.p.469559, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322827_player_id` | character | Yahoo composite player id ncaaf.p.322827, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_451229_player_id` | character | Yahoo composite player id ncaaf.p.451229, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_471801_player_id` | character | Yahoo composite player id ncaaf.p.471801, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_299700_player_id` | character | Yahoo composite player id ncaaf.p.299700, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322308_player_id` | character | Yahoo composite player id ncaaf.p.322308, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322802_player_id` | character | Yahoo composite player id ncaaf.p.322802, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333347_player_id` | character | Yahoo composite player id ncaaf.p.333347, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_334637_player_id` | character | Yahoo composite player id ncaaf.p.334637, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_404052_player_id` | character | Yahoo composite player id ncaaf.p.404052, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_406379_player_id` | character | Yahoo composite player id ncaaf.p.406379, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457838_player_id` | character | Yahoo composite player id ncaaf.p.457838, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457853_player_id` | character | Yahoo composite player id ncaaf.p.457853, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457880_player_id` | character | Yahoo composite player id ncaaf.p.457880, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_461399_player_id` | character | Yahoo composite player id ncaaf.p.461399, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_469436_player_id` | character | Yahoo composite player id ncaaf.p.469436, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403962_player_id` | character | Yahoo composite player id ncaaf.p.403962, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404392_player_id` | character | Yahoo composite player id ncaaf.p.404392, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457591_player_id` | character | Yahoo composite player id ncaaf.p.457591, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_323602_player_id` | character | Yahoo composite player id ncaaf.p.323602, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_340496_player_id` | character | Yahoo composite player id ncaaf.p.340496, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403924_player_id` | character | Yahoo composite player id ncaaf.p.403924, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457613_player_id` | character | Yahoo composite player id ncaaf.p.457613, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333922_player_id` | character | Yahoo composite player id ncaaf.p.333922, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404008_player_id` | character | Yahoo composite player id ncaaf.p.404008, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_338368_player_id` | character | Yahoo composite player id ncaaf.p.338368, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457602_player_id` | character | Yahoo composite player id ncaaf.p.457602, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327406_player_id` | character | Yahoo composite player id ncaaf.p.327406, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327412_player_id` | character | Yahoo composite player id ncaaf.p.327412, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327421_player_id` | character | Yahoo composite player id ncaaf.p.327421, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333312_player_id` | character | Yahoo composite player id ncaaf.p.333312, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333324_player_id` | character | Yahoo composite player id ncaaf.p.333324, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333423_player_id` | character | Yahoo composite player id ncaaf.p.333423, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_340505_player_id` | character | Yahoo composite player id ncaaf.p.340505, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403634_player_id` | character | Yahoo composite player id ncaaf.p.403634, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403958_player_id` | character | Yahoo composite player id ncaaf.p.403958, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404304_player_id` | character | Yahoo composite player id ncaaf.p.404304, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_405415_player_id` | character | Yahoo composite player id ncaaf.p.405415, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_405652_player_id` | character | Yahoo composite player id ncaaf.p.405652, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_451303_player_id` | character | Yahoo composite player id ncaaf.p.451303, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457606_player_id` | character | Yahoo composite player id ncaaf.p.457606, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_474363_player_id` | character | Yahoo composite player id ncaaf.p.474363, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_474366_player_id` | character | Yahoo composite player id ncaaf.p.474366, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_editorial_boxscore-example}

```python
yahoo_editorial_boxscore(game_id='ncaaf.g.202509200023')
```

_Last validated n/a._
