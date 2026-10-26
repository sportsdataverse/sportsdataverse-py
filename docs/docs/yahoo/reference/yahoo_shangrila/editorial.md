---
title: "Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: boxscore"
sidebar_label: "Editorial: boxscore"
sidebar_position: 1
description: "Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: boxscore — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Yahoo Sports — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: boxscore

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
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | String | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `ncaaf_stat_type_102` | String | Value recorded for the "Completions" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.102, abbreviated "Comp"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_103` | String | Value recorded for the "Attempts" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.103, abbreviated "Att"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_105` | String | Value recorded for the "Yards" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.105, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_104` | String | Value recorded for the "Completion Percentage" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.104, abbreviated "Pct"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_106` | String | Value recorded for the "Yards per Attempt" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.106, abbreviated "Y/A"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_111` | String | Value recorded for the "Sacks" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.111, abbreviated "Sack"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_112` | String | Value recorded for the "Yards Lost" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.112, abbreviated "YdsL"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_108` | String | Value recorded for the "Touchdowns" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.108, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_109` | String | Value recorded for the "Interceptions" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.109, abbreviated "Int"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_113` | String | Value recorded for the "QB Rating" statistic in Yahoo's Passing category (stat type ncaaf.stat_type.113, abbreviated "QBRat"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_202` | String | Value recorded for the "Rushes" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.202, abbreviated "Rush"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_203` | String | Value recorded for the "Yards" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.203, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_205` | String | Value recorded for the "Average" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.205, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_206` | String | Value recorded for the "Longest" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.206, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_207` | String | Value recorded for the "Touchdowns" statistic in Yahoo's Rushing category (stat type ncaaf.stat_type.207, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_302` | String | Value recorded for the "Receptions" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.302, abbreviated "Rec"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_303` | String | Value recorded for the "Yards" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.303, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_305` | String | Value recorded for the "Average" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.305, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_306` | String | Value recorded for the "Longest" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.306, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_309` | String | Value recorded for the "Touchdowns" statistic in Yahoo's Receiving category (stat type ncaaf.stat_type.309, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_502` | String | Value recorded for the "Kickoff Returns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.502, abbreviated "KR"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_503` | String | Value recorded for the "Yards" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.503, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_505` | String | Value recorded for the "Average" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.505, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_506` | String | Value recorded for the "Longest" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.506, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_507` | String | Value recorded for the "Touchdowns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.507, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_508` | String | Value recorded for the "Punt Returns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.508, abbreviated "PR"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_509` | String | Value recorded for the "Yards" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.509, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_511` | String | Value recorded for the "Average" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.511, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_512` | String | Value recorded for the "Longest" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.512, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_513` | String | Value recorded for the "Touchdowns" statistic in Yahoo's Returns category (stat type ncaaf.stat_type.513, abbreviated "TD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_411` | String | Value recorded for the "Extra Points Made" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.411, abbreviated "XPM"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_412` | String | Value recorded for the "Extra Points Attempted" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.412, abbreviated "XPA"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_407` | String | Value recorded for the "Total Made" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.407, abbreviated "FGM"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_408` | String | Value recorded for the "Total Attempted" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.408, abbreviated "FGA"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_410` | String | Value recorded for the "Long" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.410, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_409` | String | Value recorded for the "Percent" statistic in Yahoo's Kicking category (stat type ncaaf.stat_type.409, abbreviated "Pct"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_602` | String | Value recorded for the "Punts" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.602, abbreviated "Punt"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_604` | String | Value recorded for the "Average" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.604, abbreviated "Avg"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_608` | String | Value recorded for the "Longest" statistic in Yahoo's Punting category (stat type ncaaf.stat_type.608, abbreviated "Long"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_702` | String | Value recorded for the "Solo Tackles" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.702, abbreviated "Solo"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_703` | String | Value recorded for the "Tackle Assists" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.703, abbreviated "Ast"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_705` | String | Value recorded for the "Sacks" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.705, abbreviated "Sack"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_706` | String | Value recorded for the "Yards Lost" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.706, abbreviated "YdsL"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_710` | String | Value recorded for the "Passes Defended" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.710, abbreviated "PD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_707` | String | Value recorded for the "Interceptions" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.707, abbreviated "Int"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_708` | String | Value recorded for the "Yards" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.708, abbreviated "Yds"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_709` | String | Value recorded for the "Interception Touchdowns" statistic in Yahoo's Defense category (stat type ncaaf.stat_type.709, abbreviated "IntTD"), for the player or team on this boxscore row. |

**team_stats**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | String | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `ncaaf_stat_type_919` | String | Value recorded for the "First Downs" statistic in Yahoo's Team category (stat type ncaaf.stat_type.919, abbreviated "Firsts"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_945` | String | Value recorded for the "Total Yards" statistic in Yahoo's Team category (stat type ncaaf.stat_type.945, abbreviated "TOTYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_950` | String | Value recorded for the "Turnovers" statistic in Yahoo's Team category (stat type ncaaf.stat_type.950, abbreviated "TO"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_937` | String | Value recorded for the "Passes for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.937, abbreviated "PASSF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_936` | String | Value recorded for the "Rushes for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.936, abbreviated "RUSHF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_938` | String | Value recorded for the "Penalties for First" statistic in Yahoo's Team category (stat type ncaaf.stat_type.938, abbreviated "PENF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_941` | String | Value recorded for the "Third Down Efficiency" statistic in Yahoo's Team category (stat type ncaaf.stat_type.941, abbreviated "3DE"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_944` | String | Value recorded for the "Fourth Down Efficiency" statistic in Yahoo's Team category (stat type ncaaf.stat_type.944, abbreviated "4DE"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_946` | String | Value recorded for the "Total Plays" statistic in Yahoo's Team category (stat type ncaaf.stat_type.946, abbreviated "TOTPLAYS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_952` | String | Value recorded for the "Avg Gain Per Play" statistic in Yahoo's Team category (stat type ncaaf.stat_type.952, abbreviated "AVGPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_921` | String | Value recorded for the "Net Yards Rushing" statistic in Yahoo's Team category (stat type ncaaf.stat_type.921, abbreviated "RYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_920` | String | Value recorded for the "Rushes" statistic in Yahoo's Team category (stat type ncaaf.stat_type.920, abbreviated "Rushes"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_949` | String | Value recorded for the "Yards Per Rush" statistic in Yahoo's Team category (stat type ncaaf.stat_type.949, abbreviated "AVGRYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_947` | String | Value recorded for the "Net Yards Passing" statistic in Yahoo's Team category (stat type ncaaf.stat_type.947, abbreviated "NETPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_951` | String | Value recorded for the "Comp-Att" statistic in Yahoo's Team category (stat type ncaaf.stat_type.951, abbreviated "PASSEFF"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_948` | String | Value recorded for the "Yards Per Pass" statistic in Yahoo's Team category (stat type ncaaf.stat_type.948, abbreviated "AVGPYDS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_927` | String | Value recorded for the "Times Sacked" statistic in Yahoo's Team category (stat type ncaaf.stat_type.927, abbreviated "SACKS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_928` | String | Value recorded for the "Yds Lost To Sacks" statistic in Yahoo's Team category (stat type ncaaf.stat_type.928, abbreviated "SACKYD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_926` | String | Value recorded for the "Interceptions" statistic in Yahoo's Team category (stat type ncaaf.stat_type.926, abbreviated "INTS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_929` | String | Value recorded for the "Punts" statistic in Yahoo's Team category (stat type ncaaf.stat_type.929, abbreviated "PUNTS"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_930` | String | Value recorded for the "Punt Average" statistic in Yahoo's Team category (stat type ncaaf.stat_type.930, abbreviated "PUNTAVG"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_933` | String | Value recorded for the "Penalties" statistic in Yahoo's Team category (stat type ncaaf.stat_type.933, abbreviated "PEN"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_934` | String | Value recorded for the "Penalty Yards" statistic in Yahoo's Team category (stat type ncaaf.stat_type.934, abbreviated "PENYD"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_931` | String | Value recorded for the "Fumbles" statistic in Yahoo's Team category (stat type ncaaf.stat_type.931, abbreviated "FUMB"), for the player or team on this boxscore row. |
| `ncaaf_stat_type_932` | String | Value recorded for the "Fumbles Lost" statistic in Yahoo's Team category (stat type ncaaf.stat_type.932, abbreviated "FUMBLOST"), for the player or team on this boxscore row. |

**aliases**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `stats` | String |  |

**stat_categories**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | String |  |
| `sort` | String | Yahoo composite stat-type id the category sorts on by default (e.g., "ncaaf.stat_type.105"). |
| `stats` | String |  |

**stat_variations**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | String |  |

**stat_types**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | String |  |
| `short_name` | String |  |

**stat_cut_types**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `name` | String |  |

**games**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `gameid` | String | Date-encoded Yahoo composite game id for this row (e.g., "ncaaf.g.202509200023"). |
| `global_gameid` | String | Yahoo cross-provider game id, distinct from the date-encoded gameid (e.g., "ncaaf.g.13556882"). |
| `start_time` | String |  |
| `is_time_tba` | Boolean | Flag indicating that the scheduled start time has not yet been announced. |
| `season_phase_id` | String | Identifier of the season phase the game falls in (e.g., "season.phase.season"). |
| `game_type` | String |  |
| `winning_team_id` | String | Composite Yahoo team id of the side that won the game (e.g., "ncaaf.t.29"). |
| `is_rank_upset` | String | Flag indicating that the lower-ranked side won, judged against the teams' poll rankings. |
| `is_spread_upset` | Boolean | Flag indicating that the winning side was the betting underdog against the closing spread. |
| `outcome_type` | String | Outcome classification for a completed game (e.g., "outcome.type.won", "outcome.type.tied"). |
| `home_team_id` | String |  |
| `away_team_id` | String |  |
| `week_number` | String |  |
| `sportacular_url` | String | Deep link into the Yahoo Sportacular mobile app for this game (a "ysportacular://" URL). |
| `status_display_name` | String | Short game or event status as shown on the scoreboard (e.g., "Final", "12:00 pm ET"). |
| `status_description` | String |  |
| `status_type` | String |  |
| `total_away_points` | String | Points scored by the away team in the game. |
| `current_period_id` | String | Ordinal number of the period currently in progress, counting from 1. |
| `total_home_points` | String | Points scored by the home team in the game. |
| `total_away_shootout_points` | String | Shootout goals converted by the away team, populated only for sports that break ties by shootout. |
| `total_home_shootout_points` | String | Shootout goals converted by the home team, populated only for sports that break ties by shootout. |
| `home_team_stats` | String | JSON-encoded team-stat block for the home team, populated once the game is under way. |
| `away_team_stats` | String | JSON-encoded team-stat block for the away team, populated once the game is under way. |
| `game_period_balls` | String | Balls in the count for the at-bat in progress; baseball only. |
| `game_period_strikes` | String | Strikes in the count for the at-bat in progress; baseball only. |
| `game_period_outs` | String | Outs recorded so far in the current half-inning; baseball only. |
| `yards_to_endzone` | String | Distance from the current ball spot to the opponent's goal line, in yards. |
| `start_yardline` | String |  |
| `distance` | String |  |
| `down` | String |  |
| `team_in_possession` | String | Yahoo team id of the side currently in possession of the ball. |
| `power_play_strength_home` | String | Number of skaters the home team has on the ice during special-teams play; hockey only. |
| `power_play_strength_away` | String | Number of skaters the away team has on the ice during special-teams play; hockey only. |
| `game_time_elapsed` | String | Playing time elapsed in the game, in seconds. |
| `game_time_elapsed_display` | String | Playing time elapsed formatted for display (e.g., "67:12"), used by sports whose clock counts up. |
| `inning_status` | String | Half-inning indicator for a game in progress (e.g., "Top", "Bottom"); baseball only. |
| `away_timeouts` | String |  |
| `home_timeouts` | String |  |
| `is_halftime` | String | Flag indicating that the game is currently stopped at halftime. |
| `minimum_periods` | Int64 | Number of periods a game of this sport runs before overtime is required (4 for football, 9 for baseball). |
| `game_periods` | String | JSON-encoded list of the game's period nodes, each carrying a period number and its display names. |
| `baserunners` | String | JSON-encoded baserunner occupancy for the game in progress; baseball only. |
| `season` | String |  |
| `subleague` | String | Sub-league the game belongs to, for leagues split into constituent circuits. |
| `subleague_display_name` | String | Display name of the sub-league the game belongs to. |
| `agg_score` | String | Aggregate score across the legs of a two-leg tie, populated only for competitions decided on aggregate. |
| `leg_number` | String | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `tv_coverage` | String | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `seatgeek_id` | String | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `last_updated` | String |  |
| `teams` | String |  |
| `play_by_play` | String | JSON-encoded data-island pointer to the game's play-by-play collection in the same editorial payload. |
| `pitches` | String | JSON-encoded data-island pointer to the game's pitch-level feed; baseball only. |
| `at_bat` | String | JSON-encoded data-island pointer to the game's current at-bat feed; baseball only. |
| `penalty_summary` | String |  |
| `scoring_summary` | String |  |
| `stat_categories` | String | JSON-encoded pointer to the stat-category dictionary that groups this feed's statistics. |
| `stadium` | String |  |
| `stadium_id` | String |  |
| `stadium_image` | String | JSON-encoded data-island pointer to the venue photograph used on the game page. |
| `attendance` | String |  |
| `lineups` | String | JSON-encoded data-island pointer to the game's lineup collection. |
| `top_performer` | String | JSON-encoded data-island pointer to the game's top-performing players. |
| `players` | String |  |
| `byline` | String |  |
| `highlight` | String | JSON-encoded data-island pointer to the game's highlight video. |
| `highlights` | String |  |
| `live_video` | String | JSON-encoded data-island pointer to the live video stream for the game. |
| `odds` | String | JSON-encoded data-island pointer to the game's odds collection. |
| `current_players` | String | JSON-encoded data-island pointer to the players currently on the field, ice or court. |
| `last_play` | String |  |
| `series_type` | String | JSON-encoded data-island pointer to the kind of series the game belongs to. |
| `series_status` | String | JSON-encoded data-island pointer to the current state of the series the game belongs to. |
| `games` | String |  |
| `series_games` | String | JSON-encoded data-island pointer to the games making up the series. |
| `game_details` | String | JSON-encoded data-island pointer to supplementary detail notes for the game. |
| `section_notes` | String | JSON-encoded data-island pointer to editorial section notes attached to the game page. |
| `articles` | String | JSON-encoded data-island pointer to the editorial articles attached to the game. |
| `tweets` | String | JSON-encoded data-island pointer to the social posts attached to the game page. |
| `playoff_round` | String |  |
| `media_stream` | String | JSON-encoded data-island pointer to the game's media-stream collection. |
| `playoff_series_status` | String | JSON-encoded data-island pointer to the current state of the playoff series the game belongs to. |
| `playoff_series_details` | String | JSON-encoded data-island pointer to detail about the playoff series the game belongs to. |
| `drives` | String | JSON-encoded data-island pointer to the game's drive collection; football only. |
| `user_teams_game` | String | JSON-encoded data-island pointer to the viewer's followed-team context for the game. |
| `page_metadata` | String | JSON-encoded data-island pointer to the SEO and page metadata for the entity. |
| `penalty_box` | String | JSON-encoded data-island pointer to the game's penalty-box feed; hockey only. |
| `starting_pitchers` | String | JSON-encoded data-island pointer to the game's announced starting pitchers; baseball only. |
| `unrestricted_streams` | String | JSON-encoded data-island pointer to the streams viewable without a subscription. |
| `tv_details` | String | JSON-encoded list of broadcast entries for the game, each carrying a network abbreviation and full channel name (e.g., [{"abbr": "NBC", "name": "NBC/Peacock"}]). |
| `away_seed` | String |  |
| `home_seed` | String |  |
| `navigation_links_tickets_url` | String | Affiliate ticket-purchase URL for the game, pointing at the SeatGeek marketplace. |
| `navigation_links_boxscore_url` | String | Site-relative URL of the game's boxscore page on sports.yahoo.com. |
| `navigation_links_match_page_url` | String | Site-relative URL of the game's match page on sports.yahoo.com. |
| `navigation_links_recap_url` | String | Site-relative URL of the editorial recap article written for the game. |
| `navigation_links_league_home_url` | String | Site-relative URL of the league's home page on sports.yahoo.com. |
| `navigation_links_league_scores_url` | String | Site-relative URL of the league's scoreboard page on sports.yahoo.com. |
| `provider_coverage_score_update_frequency_in_minutes` | String | How often, in minutes, the data provider refreshes the score for this game. |
| `provider_coverage_has_plays` | String | Flag indicating that the data provider supplies play-by-play for this game. |
| `provider_coverage_has_stats` | String | Flag indicating that the data provider supplies box-score statistics for this game. |
| `provider_coverage_has_extended_stats` | String | Flag indicating that the data provider supplies extended statistics beyond the standard box score. |
| `provider_coverage_has_final_stats` | String | Flag indicating that the data provider has published final, official statistics for the game. |

**gameplayoff_round**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | String |  |

**gameplayoff_series_status**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | String |  |

**gameplayoff_series_details**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | String |  |

**gamescore**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | String |  |

**gamecurrent_players**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `current_batter` | String | Yahoo composite player id of the batter at the plate; baseball only. |
| `current_pitcher` | String | Yahoo composite player id of the pitcher on the mound; baseball only. |
| `base_runners` | String | JSON-encoded list of runners currently occupying bases; baseball only. |
| `due_ups` | String | JSON-encoded list of the batters due up next; baseball only. |

**gamelast_play**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `play_id` | String |  |
| `play_type` | String |  |
| `play_text` | String |  |
| `period` | String |  |
| `clock` | String |  |
| `team` | String |  |
| `is_scoring_play` | Int64 | Flag indicating that the play put points on the board (1 = scoring play, 0 = not). |

**gamepenalty_box**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | String |  |

**gameplay_by_play**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | String | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `play_id` | String |  |
| `period` | String |  |
| `clock` | String |  |
| `down` | String |  |
| `distance` | String |  |
| `team` | String |  |
| `yardline` | String | Ball spot at the snap as rendered on the scoreboard (e.g., "MICH 35"). |
| `yards_to_endzone` | String | Distance from the current ball spot to the opponent's goal line, in yards. |
| `type` | String |  |
| `yards` | String |  |
| `text` | String |  |
| `play_time` | String | Wall-clock instant the play was recorded, as a Unix epoch timestamp in seconds. |

**gameat_bat**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | String |  |

**gamescoring_summary**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | String | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `play_id` | String |  |
| `period` | String |  |
| `clock` | String |  |
| `away_score` | String |  |
| `home_score` | String |  |
| `team` | String |  |
| `score_type` | String | Kind of score produced by the scoring play (e.g., "TD" touchdown, "FG" field goal, "SF" safety). |
| `xp_type` | String | Conversion attempted after the touchdown ("EP" for an extra point, "2PT" for a two-point try, "0" when none was attempted). |
| `players` | String |  |
| `text` | String |  |

**gamemedia_stream**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `media_type` | String | Kind of item carried in the media stream (e.g., "play", "video"). |
| `media_source` | String | Feed the media item was produced from (e.g., "play_by_play"). |
| `sequence_id` | String | Monotonic sequence number that orders items within the game's media stream. |
| `external_id` | String | Provider-side identifier for the media item, matching the play id it accompanies. |
| `timestamp` | String |  |
| `official` | Boolean | Flag indicating that the media item comes from the official league feed rather than an editorial source. |

**gamedrives**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `id` | String |  |
| `team` | String |  |
| `time` | String |  |
| `num_plays` | String | Number of plays the offense ran on the drive. |
| `yards_covered` | String | Net yards the offense gained over the course of the drive. |
| `start_yardline` | String |  |
| `yardline_text` | String | Ball spot where the drive started, rendered as it appears on the scoreboard (e.g., "NEB 25"). |
| `plays` | String |  |
| `result` | String |  |
| `start_time_clock` | String | Game clock reading when the drive began, as MM:SS remaining in its period. |
| `start_time_period` | String | Period number in which the drive began. |
| `end_time_clock` | String | Game clock reading when the drive ended, as MM:SS remaining in its period. |
| `end_time_period` | Int64 | Period number in which the drive ended. |

**gamelineups**

| col_name | type | description |
|---|---|---|
| `entity_id` | String | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `home_lineup_order` | String | JSON-encoded batting or lineup order for the home team, empty for sports without a fixed order. |
| `away_lineup_order` | String | JSON-encoded batting or lineup order for the away team, empty for sports without a fixed order. |
| `home_lineup_all_ncaaf_p_457863_player_id` | String | Yahoo composite player id ncaaf.p.457863, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333433_player_id` | String | Yahoo composite player id ncaaf.p.333433, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_461405_player_id` | String | Yahoo composite player id ncaaf.p.461405, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457834_player_id` | String | Yahoo composite player id ncaaf.p.457834, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_469568_player_id` | String | Yahoo composite player id ncaaf.p.469568, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333434_player_id` | String | Yahoo composite player id ncaaf.p.333434, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333286_player_id` | String | Yahoo composite player id ncaaf.p.333286, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_404124_player_id` | String | Yahoo composite player id ncaaf.p.404124, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322801_player_id` | String | Yahoo composite player id ncaaf.p.322801, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457875_player_id` | String | Yahoo composite player id ncaaf.p.457875, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_469559_player_id` | String | Yahoo composite player id ncaaf.p.469559, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322827_player_id` | String | Yahoo composite player id ncaaf.p.322827, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_451229_player_id` | String | Yahoo composite player id ncaaf.p.451229, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_471801_player_id` | String | Yahoo composite player id ncaaf.p.471801, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_299700_player_id` | String | Yahoo composite player id ncaaf.p.299700, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322308_player_id` | String | Yahoo composite player id ncaaf.p.322308, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_322802_player_id` | String | Yahoo composite player id ncaaf.p.322802, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_333347_player_id` | String | Yahoo composite player id ncaaf.p.333347, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_334637_player_id` | String | Yahoo composite player id ncaaf.p.334637, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_404052_player_id` | String | Yahoo composite player id ncaaf.p.404052, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_406379_player_id` | String | Yahoo composite player id ncaaf.p.406379, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457838_player_id` | String | Yahoo composite player id ncaaf.p.457838, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457853_player_id` | String | Yahoo composite player id ncaaf.p.457853, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_457880_player_id` | String | Yahoo composite player id ncaaf.p.457880, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `home_lineup_all_ncaaf_p_461399_player_id` | String | Yahoo composite player id ncaaf.p.461399, present when that player is listed in the home team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_469436_player_id` | String | Yahoo composite player id ncaaf.p.469436, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403962_player_id` | String | Yahoo composite player id ncaaf.p.403962, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404392_player_id` | String | Yahoo composite player id ncaaf.p.404392, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457591_player_id` | String | Yahoo composite player id ncaaf.p.457591, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_323602_player_id` | String | Yahoo composite player id ncaaf.p.323602, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_340496_player_id` | String | Yahoo composite player id ncaaf.p.340496, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403924_player_id` | String | Yahoo composite player id ncaaf.p.403924, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457613_player_id` | String | Yahoo composite player id ncaaf.p.457613, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333922_player_id` | String | Yahoo composite player id ncaaf.p.333922, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404008_player_id` | String | Yahoo composite player id ncaaf.p.404008, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_338368_player_id` | String | Yahoo composite player id ncaaf.p.338368, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457602_player_id` | String | Yahoo composite player id ncaaf.p.457602, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327406_player_id` | String | Yahoo composite player id ncaaf.p.327406, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327412_player_id` | String | Yahoo composite player id ncaaf.p.327412, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_327421_player_id` | String | Yahoo composite player id ncaaf.p.327421, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333312_player_id` | String | Yahoo composite player id ncaaf.p.333312, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333324_player_id` | String | Yahoo composite player id ncaaf.p.333324, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_333423_player_id` | String | Yahoo composite player id ncaaf.p.333423, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_340505_player_id` | String | Yahoo composite player id ncaaf.p.340505, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403634_player_id` | String | Yahoo composite player id ncaaf.p.403634, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_403958_player_id` | String | Yahoo composite player id ncaaf.p.403958, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_404304_player_id` | String | Yahoo composite player id ncaaf.p.404304, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_405415_player_id` | String | Yahoo composite player id ncaaf.p.405415, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_405652_player_id` | String | Yahoo composite player id ncaaf.p.405652, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_451303_player_id` | String | Yahoo composite player id ncaaf.p.451303, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_457606_player_id` | String | Yahoo composite player id ncaaf.p.457606, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_474363_player_id` | String | Yahoo composite player id ncaaf.p.474363, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |
| `away_lineup_all_ncaaf_p_474366_player_id` | String | Yahoo composite player id ncaaf.p.474366, present when that player is listed in the away team's full lineup; the lineup map keys become one column per player, so the column exists only for games in which this player dressed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#yahoo_editorial_boxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#yahoo_editorial_boxscore-example}

```python
yahoo_editorial_boxscore(game_id='ncaaf.g.202509200023')
```
