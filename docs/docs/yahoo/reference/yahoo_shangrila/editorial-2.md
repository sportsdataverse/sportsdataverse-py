---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: scoreboard"
sidebar_label: "Editorial: scoreboard"
sidebar_position: 2
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: scoreboard — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Editorial: scoreboard

## yahoo_editorial_scoreboard

Scoreboard: games + teams + leagues + odds (fat payload)

**Endpoint URL:** `GET https://api-secure.sports.yahoo.com/v1/editorial/s/scoreboard`

**Valid URL:** [https://api-secure.sports.yahoo.com/v1/editorial/s/scoreboard](https://api-secure.sports.yahoo.com/v1/editorial/s/scoreboard)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `conferences` | `conferences` |  |  | `Y` | conferences query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `v` | `v` |  |  | `Y` | v query parameter. |

### Returns {#yahoo_editorial_scoreboard-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the feed's id-keyed collections (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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
| `is_spread_upset` | character | Flag indicating that the winning side was the betting underdog against the closing spread. |
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
| `minimum_periods` | character | Number of periods a game of this sport runs before overtime is required (4 for football, 9 for baseball). |
| `game_periods` | character | JSON-encoded list of the game's period nodes, each carrying a period number and its display names. |
| `baserunners` | character | JSON-encoded baserunner occupancy for the game in progress; baseball only. |
| `season` | character | Season year. |
| `subleague` | character | Sub-league the game belongs to, for leagues split into constituent circuits. |
| `subleague_display_name` | character | Display name of the sub-league the game belongs to. |
| `agg_score` | character | Aggregate score across the legs of a two-leg tie, populated only for competitions decided on aggregate. |
| `leg_number` | character | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `tv_coverage` | character | Network carrying the game, as a short broadcast abbreviation (e.g., "CBS", "ESPN"). |
| `seatgeek_id` | character | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `last_updated` | logical | Last-updated timestamp. |
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

**teams**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `team_id` | character | Unique team identifier. |
| `display_name` | character | Display name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `full_name` | character | Player's full name. |
| `abbr` | character | Short team abbreviation used on the scoreboard (e.g., "TCU", "UNC"). |
| `division_id` | character | Division MLBAM ID. |
| `division` | character | Team division. |
| `division_abbr` | character | Division abbreviation. |
| `subdivision_id` | character | Yahoo numeric identifier of the subdivision the team competes in. |
| `subdivision` | character | Name of the subdivision the team competes in, typically a conference division (e.g., "East Division"). |
| `conference_id` | character | Conference identifier. |
| `conference_abbr` | character | Conference abbreviation. |
| `conference_seed` | character | Seed the team holds within its conference for playoff purposes. |
| `conference` | character | Conference name. |
| `seatgeek_id` | character | SeatGeek performer or event identifier used to build the ticket-purchase link. |
| `sportacular_logo` | character | Data-island pointer to the team's Sportacular-app logo, JSON-encoded as ["teamsportacularLogo", <team id>]. |
| `sportacular_logo_dark` | character | Data-island pointer to the team's dark-mode Sportacular-app logo, JSON-encoded as ["teamsportacularLogoDark", <team id>]. |
| `logo` | character | Team or league logo URL. |
| `logo_dark` | character | Dark-mode logo URL. |
| `color_primary` | character | Data-island pointer to the team's primary brand color, JSON-encoded as ["teamcolorPrimary", <team id>]. |
| `color_secondary` | character | Data-island pointer to the team's secondary brand color, JSON-encoded as ["teamColorSecondary", <team id>]. |
| `record` | character | Team win-loss record for the season. |
| `players` | character | Nested list of per-player box scores. |
| `rankings` | character | Data-island pointer to the team's poll rankings, JSON-encoded as ["teamrankings", <team id>]. |
| `stat_categories` | character | JSON-encoded pointer to the stat-category dictionary that groups this feed's statistics. |
| `page_metadata` | character | JSON-encoded data-island pointer to the SEO and page metadata for the entity. |
| `team_home_link` | character | Site-relative URL of the team's home page (e.g., "/ncaaf/teams/tcu"). |
| `team_schedule_link` | character | Site-relative URL of the team's schedule page (e.g., "/ncaaf/teams/tcu/schedule"). |
| `conference_position` | character | Rank of the team within its conference standings; empty when the league does not order by conference. |
| `group_position` | character | Rank of the team within its scoreboard grouping; an empty string for leagues that do not group. |
| `playoff_seed` | character | Current playoff seed. |

**teamsportacular_logo**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teamsportacular_logo_dark**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**team_logo**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**team_logo_dark**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teamrecord**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teamrankings**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `date` | character | Date in YYYY-MM-DD format. |
| `poll_key` | character | Yahoo composite poll id the ranking row was taken from (e.g., "ncaaf.poll.9"). |
| `rank` | character | Position of the school within the poll for the given week (1 = top-ranked). |
| `previous_rank` | character | Team's rank in the prior release of this poll. |
| `points` | character | Points scored. |
| `source` | character | News source. |
| `primary` | character | Flag indicating that this poll is the one shown as the team's headline ranking. |
| `relevant` | character | Flag indicating that the poll ranking is currently relevant enough to display. |
| `teams` | character | Nested list of member-team membership spans. |

**leagues**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `name` | character | Display name. |
| `display_name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `season_display_year` | character | Season year shown in the league's scoreboard header, as a four-digit year. |
| `season_week_number` | integer | Week number the league feed is currently anchored on. |
| `season_season_current_date` | character | Date the league feed treats as "today", in YYYY-MM-DD form. |
| `season_season_next_real_game_date` | character | Date of the next non-exhibition game on the league calendar, in YYYY-MM-DD form. |
| `season_season_next_game_date` | character | Date of the next scheduled game on the league calendar, in YYYY-MM-DD form. |
| `season_season_month` | character | Two-digit calendar month the league feed is currently anchored on. |
| `season_display_schedule_period` | character | Season phase the scoreboard is currently displaying (e.g., "season.phase.offseason"). |
| `season_display_schedule_period_id` | character | Numeric identifier of the season phase the scoreboard is currently displaying. |
| `season_current_phase` | character | Phase the league season is currently in, as Yahoo's season-phase key. |
| `season_current_sched_state` | character | Numeric scheduling state matching the current phase (2 regular season, 3 postseason, 4 offseason). |
| `season_suspended` | character | Flag indicating that league play is currently suspended (1 = suspended, 0 = normal). |
| `season_current_stat_state_season` | integer | Season the stats graph is currently serving statistics for, as a four-digit year. |
| `season_current_stat_state_week` | character | Week the stats graph is currently serving statistics for. |
| `season_current_stat_state_graphite_phase` | character | Phase key the stats graph is currently serving statistics for (e.g., "REGULAR_SEASON"). |
| `season_phases_2_phase_id` | character | Season-phase key Yahoo assigns the league's regular season phase (e.g., "season.phase.season"). |
| `season_phases_2_name` | character | Display label Yahoo gives the league's regular season phase. |
| `season_phases_2_sched_state` | character | Numeric scheduling state Yahoo assigns the league's regular season phase (2). |
| `season_phases_2_phase_start` | character | Start of the league's regular season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_2_phase_end` | character | End of the league's regular season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_2_phase_start_week` | character | Week number the league's regular season phase begins on, or false when the phase is not organised into weeks. |
| `season_phases_2_phase_end_week` | character | Week number the league's regular season phase ends on, or false when the phase is not organised into weeks. |
| `season_phases_2_structures` | character | JSON-encoded pointer to the league structures (divisions and conferences) that apply during the regular season phase. |
| `season_phases_3_phase_id` | character | Season-phase key Yahoo assigns the league's post season phase (e.g., "season.phase.postseason"). |
| `season_phases_3_name` | character | Display label Yahoo gives the league's post season phase. |
| `season_phases_3_sched_state` | character | Numeric scheduling state Yahoo assigns the league's post season phase (3). |
| `season_phases_3_phase_start` | character | Start of the league's post season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_3_phase_end` | character | End of the league's post season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_3_phase_start_week` | logical | Week number the league's post season phase begins on, or false when the phase is not organised into weeks. |
| `season_phases_3_phase_end_week` | logical | Week number the league's post season phase ends on, or false when the phase is not organised into weeks. |
| `season_phases_3_structures` | character | JSON-encoded pointer to the league structures (divisions and conferences) that apply during the post season phase. |
| `season_phases_4_phase_id` | character | Season-phase key Yahoo assigns the league's off season phase (e.g., "season.phase.offseason"). |
| `season_phases_4_name` | character | Display label Yahoo gives the league's off season phase. |
| `season_phases_4_sched_state` | character | Numeric scheduling state Yahoo assigns the league's off season phase (4). |
| `season_phases_4_phase_start` | character | Start of the league's off season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_4_phase_end` | character | End of the league's off season phase as an RFC 1123 timestamp, or "TBD" when Yahoo has not fixed it yet. |
| `season_phases_4_phase_start_week` | logical | Week number the league's off season phase begins on, or false when the phase is not organised into weeks. |
| `season_phases_4_phase_end_week` | logical | Week number the league's off season phase ends on, or false when the phase is not organised into weeks. |
| `season_phases_4_structures` | character | JSON-encoded pointer to the league structures (divisions and conferences) that apply during the off season phase. |

**divisions**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `sub_id` | character | Second-level key of an id-keyed editorial collection, present when one entity holds many sub-records — a play id, a scoring-play id, or a stat variation such as "ncaaf.stat_variation.2". |
| `id` | integer | ID of the player in the 'name' column. |
| `name` | character | Display name. |
| `type` | character | Record type / category. |
| `conferences_1_id` | numeric | Yahoo numeric conference id carried for the Atlantic Coast conference in the league's division structure. |
| `conferences_1_name` | character | Conference name carried under Yahoo conference key 1, the Atlantic Coast conference. |
| `conferences_1_type` | character | Structure level of Yahoo conference key 1 (Atlantic Coast); "conference" for a full conference rather than a division within one. |
| `conferences_4_id` | numeric | Yahoo numeric conference id carried for the Big Ten conference in the league's division structure. |
| `conferences_4_name` | character | Conference name carried under Yahoo conference key 4, the Big Ten conference. |
| `conferences_4_type` | character | Structure level of Yahoo conference key 4 (Big Ten); "conference" for a full conference rather than a division within one. |
| `conferences_6_id` | numeric | Yahoo numeric conference id carried for the Mid-American conference in the league's division structure. |
| `conferences_6_name` | character | Conference name carried under Yahoo conference key 6, the Mid-American conference. |
| `conferences_6_type` | character | Structure level of Yahoo conference key 6 (Mid-American); "conference" for a full conference rather than a division within one. |
| `conferences_7_id` | numeric | Yahoo numeric conference id carried for the Pac-12 conference in the league's division structure. |
| `conferences_7_name` | character | Conference name carried under Yahoo conference key 7, the Pac-12 conference. |
| `conferences_7_type` | character | Structure level of Yahoo conference key 7 (Pac-12); "conference" for a full conference rather than a division within one. |
| `conferences_8_id` | numeric | Yahoo numeric conference id carried for the SEC conference in the league's division structure. |
| `conferences_8_name` | character | Conference name carried under Yahoo conference key 8, the SEC conference. |
| `conferences_8_type` | character | Structure level of Yahoo conference key 8 (SEC); "conference" for a full conference rather than a division within one. |
| `conferences_11_id` | numeric | Yahoo numeric conference id carried for the Independents (FBS) conference in the league's division structure. |
| `conferences_11_name` | character | Conference name carried under Yahoo conference key 11, the Independents (FBS) conference. |
| `conferences_11_type` | character | Structure level of Yahoo conference key 11 (Independents (FBS)); "conference" for a full conference rather than a division within one. |
| `conferences_71_id` | numeric | Yahoo numeric conference id carried for the Big 12 conference in the league's division structure. |
| `conferences_71_name` | character | Conference name carried under Yahoo conference key 71, the Big 12 conference. |
| `conferences_71_type` | character | Structure level of Yahoo conference key 71 (Big 12); "conference" for a full conference rather than a division within one. |
| `conferences_72_id` | numeric | Yahoo numeric conference id carried for the Conference USA conference in the league's division structure. |
| `conferences_72_name` | character | Conference name carried under Yahoo conference key 72, the Conference USA conference. |
| `conferences_72_type` | character | Structure level of Yahoo conference key 72 (Conference USA); "conference" for a full conference rather than a division within one. |
| `conferences_87_id` | numeric | Yahoo numeric conference id carried for the Mountain West conference in the league's division structure. |
| `conferences_87_name` | character | Conference name carried under Yahoo conference key 87, the Mountain West conference. |
| `conferences_87_type` | character | Structure level of Yahoo conference key 87 (Mountain West); "conference" for a full conference rather than a division within one. |
| `conferences_90_id` | numeric | Yahoo numeric conference id carried for the Sun Belt conference in the league's division structure. |
| `conferences_90_name` | character | Conference name carried under Yahoo conference key 90, the Sun Belt conference. |
| `conferences_90_type` | character | Structure level of Yahoo conference key 90 (Sun Belt); "conference" for a full conference rather than a division within one. |
| `conferences_90_subdivisions_1_id` | numeric | Yahoo numeric subdivision id for the East Division of the Sun Belt conference. |
| `conferences_90_subdivisions_1_name` | character | Name of subdivision 1 within the Sun Belt conference, the East Division. |
| `conferences_90_subdivisions_1_type` | character | Structure level of subdivision 1 within the Sun Belt conference; "subdivision" for a division inside a conference. |
| `conferences_90_subdivisions_2_id` | numeric | Yahoo numeric subdivision id for the West Division of the Sun Belt conference. |
| `conferences_90_subdivisions_2_name` | character | Name of subdivision 2 within the Sun Belt conference, the West Division. |
| `conferences_90_subdivisions_2_type` | character | Structure level of subdivision 2 within the Sun Belt conference; "subdivision" for a division inside a conference. |
| `conferences_122_id` | numeric | Yahoo numeric conference id carried for the American Athletic conference in the league's division structure. |
| `conferences_122_name` | character | Conference name carried under Yahoo conference key 122, the American Athletic conference. |
| `conferences_122_type` | character | Structure level of Yahoo conference key 122 (American Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_13_id` | numeric | Yahoo numeric conference id carried for the Big Sky conference in the league's division structure. |
| `conferences_13_name` | character | Conference name carried under Yahoo conference key 13, the Big Sky conference. |
| `conferences_13_type` | character | Structure level of Yahoo conference key 13 (Big Sky); "conference" for a full conference rather than a division within one. |
| `conferences_14_id` | numeric | Yahoo numeric conference id carried for the Missouri Valley conference in the league's division structure. |
| `conferences_14_name` | character | Conference name carried under Yahoo conference key 14, the Missouri Valley conference. |
| `conferences_14_type` | character | Structure level of Yahoo conference key 14 (Missouri Valley); "conference" for a full conference rather than a division within one. |
| `conferences_15_id` | numeric | Yahoo numeric conference id carried for the Ivy League conference in the league's division structure. |
| `conferences_15_name` | character | Conference name carried under Yahoo conference key 15, the Ivy League conference. |
| `conferences_15_type` | character | Structure level of Yahoo conference key 15 (Ivy League); "conference" for a full conference rather than a division within one. |
| `conferences_17_id` | numeric | Yahoo numeric conference id carried for the Mid-Eastern Athletic conference in the league's division structure. |
| `conferences_17_name` | character | Conference name carried under Yahoo conference key 17, the Mid-Eastern Athletic conference. |
| `conferences_17_type` | character | Structure level of Yahoo conference key 17 (Mid-Eastern Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_19_id` | numeric | Yahoo numeric conference id carried for the Patriot League conference in the league's division structure. |
| `conferences_19_name` | character | Conference name carried under Yahoo conference key 19, the Patriot League conference. |
| `conferences_19_type` | character | Structure level of Yahoo conference key 19 (Patriot League); "conference" for a full conference rather than a division within one. |
| `conferences_20_id` | numeric | Yahoo numeric conference id carried for the Pioneer League conference in the league's division structure. |
| `conferences_20_name` | character | Conference name carried under Yahoo conference key 20, the Pioneer League conference. |
| `conferences_20_type` | character | Structure level of Yahoo conference key 20 (Pioneer League); "conference" for a full conference rather than a division within one. |
| `conferences_21_id` | numeric | Yahoo numeric conference id carried for the Southern conference in the league's division structure. |
| `conferences_21_name` | character | Conference name carried under Yahoo conference key 21, the Southern conference. |
| `conferences_21_type` | character | Structure level of Yahoo conference key 21 (Southern); "conference" for a full conference rather than a division within one. |
| `conferences_22_id` | numeric | Yahoo numeric conference id carried for the Southland conference in the league's division structure. |
| `conferences_22_name` | character | Conference name carried under Yahoo conference key 22, the Southland conference. |
| `conferences_22_type` | character | Structure level of Yahoo conference key 22 (Southland); "conference" for a full conference rather than a division within one. |
| `conferences_23_id` | numeric | Yahoo numeric conference id carried for the Southwestern Athletic conference in the league's division structure. |
| `conferences_23_name` | character | Conference name carried under Yahoo conference key 23, the Southwestern Athletic conference. |
| `conferences_23_type` | character | Structure level of Yahoo conference key 23 (Southwestern Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_23_subdivisions_1_id` | numeric | Yahoo numeric subdivision id for the East Division of the Southwestern Athletic conference. |
| `conferences_23_subdivisions_1_name` | character | Name of subdivision 1 within the Southwestern Athletic conference, the East Division. |
| `conferences_23_subdivisions_1_type` | character | Structure level of subdivision 1 within the Southwestern Athletic conference; "subdivision" for a division inside a conference. |
| `conferences_23_subdivisions_2_id` | numeric | Yahoo numeric subdivision id for the West Division of the Southwestern Athletic conference. |
| `conferences_23_subdivisions_2_name` | character | Name of subdivision 2 within the Southwestern Athletic conference, the West Division. |
| `conferences_23_subdivisions_2_type` | character | Structure level of subdivision 2 within the Southwestern Athletic conference; "subdivision" for a division inside a conference. |
| `conferences_73_id` | numeric | Yahoo numeric conference id carried for the Northeast conference in the league's division structure. |
| `conferences_73_name` | character | Conference name carried under Yahoo conference key 73, the Northeast conference. |
| `conferences_73_type` | character | Structure level of Yahoo conference key 73 (Northeast); "conference" for a full conference rather than a division within one. |
| `conferences_74_id` | numeric | Yahoo numeric conference id carried for the Independents (FCS) conference in the league's division structure. |
| `conferences_74_name` | character | Conference name carried under Yahoo conference key 74, the Independents (FCS) conference. |
| `conferences_74_type` | character | Structure level of Yahoo conference key 74 (Independents (FCS)); "conference" for a full conference rather than a division within one. |
| `conferences_98_id` | numeric | Yahoo numeric conference id carried for the Coastal Athletic conference in the league's division structure. |
| `conferences_98_name` | character | Conference name carried under Yahoo conference key 98, the Coastal Athletic conference. |
| `conferences_98_type` | character | Structure level of Yahoo conference key 98 (Coastal Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_282_id` | numeric | Yahoo numeric conference id carried for the United Athletic Conference conference in the league's division structure. |
| `conferences_282_name` | character | Conference name carried under Yahoo conference key 282, the United Athletic Conference conference. |
| `conferences_282_type` | character | Structure level of Yahoo conference key 282 (United Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_283_id` | numeric | Yahoo numeric conference id carried for the Big South-OVC conference in the league's division structure. |
| `conferences_283_name` | character | Conference name carried under Yahoo conference key 283, the Big South-OVC conference. |
| `conferences_283_type` | character | Structure level of Yahoo conference key 283 (Big South-OVC); "conference" for a full conference rather than a division within one. |
| `conferences_25_id` | numeric | Yahoo numeric conference id carried for the CIAA conference in the league's division structure. |
| `conferences_25_name` | character | Conference name carried under Yahoo conference key 25, the CIAA conference. |
| `conferences_25_type` | character | Structure level of Yahoo conference key 25 (CIAA); "conference" for a full conference rather than a division within one. |
| `conferences_27_id` | numeric | Yahoo numeric conference id carried for the Gulf South conference in the league's division structure. |
| `conferences_27_name` | character | Conference name carried under Yahoo conference key 27, the Gulf South conference. |
| `conferences_27_type` | character | Structure level of Yahoo conference key 27 (Gulf South); "conference" for a full conference rather than a division within one. |
| `conferences_28_id` | numeric | Yahoo numeric conference id carried for the Lone Star conference in the league's division structure. |
| `conferences_28_name` | character | Conference name carried under Yahoo conference key 28, the Lone Star conference. |
| `conferences_28_type` | character | Structure level of Yahoo conference key 28 (Lone Star); "conference" for a full conference rather than a division within one. |
| `conferences_29_id` | numeric | Yahoo numeric conference id carried for the Mid-America Intercollegiate Athletics Association conference in the league's division structure. |
| `conferences_29_name` | character | Conference name carried under Yahoo conference key 29, the Mid-America Intercollegiate Athletics Association conference. |
| `conferences_29_type` | character | Structure level of Yahoo conference key 29 (Mid-America Intercollegiate Athletics Association); "conference" for a full conference rather than a division within one. |
| `conferences_33_id` | numeric | Yahoo numeric conference id carried for the Northern Sun Intercollegiate conference in the league's division structure. |
| `conferences_33_name` | character | Conference name carried under Yahoo conference key 33, the Northern Sun Intercollegiate conference. |
| `conferences_33_type` | character | Structure level of Yahoo conference key 33 (Northern Sun Intercollegiate); "conference" for a full conference rather than a division within one. |
| `conferences_34_id` | numeric | Yahoo numeric conference id carried for the Pennsylvania State Athletic Conference conference in the league's division structure. |
| `conferences_34_name` | character | Conference name carried under Yahoo conference key 34, the Pennsylvania State Athletic Conference conference. |
| `conferences_34_type` | character | Structure level of Yahoo conference key 34 (Pennsylvania State Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_35_id` | numeric | Yahoo numeric conference id carried for the Rocky Mountain Athletic conference in the league's division structure. |
| `conferences_35_name` | character | Conference name carried under Yahoo conference key 35, the Rocky Mountain Athletic conference. |
| `conferences_35_type` | character | Structure level of Yahoo conference key 35 (Rocky Mountain Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_36_id` | numeric | Yahoo numeric conference id carried for the South Atlantic conference in the league's division structure. |
| `conferences_36_name` | character | Conference name carried under Yahoo conference key 36, the South Atlantic conference. |
| `conferences_36_type` | character | Structure level of Yahoo conference key 36 (South Atlantic); "conference" for a full conference rather than a division within one. |
| `conferences_37_id` | numeric | Yahoo numeric conference id carried for the Southern Intercollegiate Athletic conference in the league's division structure. |
| `conferences_37_name` | character | Conference name carried under Yahoo conference key 37, the Southern Intercollegiate Athletic conference. |
| `conferences_37_type` | character | Structure level of Yahoo conference key 37 (Southern Intercollegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_97_id` | numeric | Yahoo numeric conference id carried for the Great Lakes Intercollegiate Athletic conference in the league's division structure. |
| `conferences_97_name` | character | Conference name carried under Yahoo conference key 97, the Great Lakes Intercollegiate Athletic conference. |
| `conferences_97_type` | character | Structure level of Yahoo conference key 97 (Great Lakes Intercollegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_99_id` | numeric | Yahoo numeric conference id carried for the Northeast 10 conference in the league's division structure. |
| `conferences_99_name` | character | Conference name carried under Yahoo conference key 99, the Northeast 10 conference. |
| `conferences_99_type` | character | Structure level of Yahoo conference key 99 (Northeast 10); "conference" for a full conference rather than a division within one. |
| `conferences_102_id` | numeric | Yahoo numeric conference id carried for the Atlantic Central Conference conference in the league's division structure. |
| `conferences_102_name` | character | Conference name carried under Yahoo conference key 102, the Atlantic Central Conference conference. |
| `conferences_102_type` | character | Structure level of Yahoo conference key 102 (Atlantic Central Conference); "conference" for a full conference rather than a division within one. |
| `conferences_107_id` | numeric | Yahoo numeric conference id carried for the Great Northwest Athletic Conference conference in the league's division structure. |
| `conferences_107_name` | character | Conference name carried under Yahoo conference key 107, the Great Northwest Athletic Conference conference. |
| `conferences_107_type` | character | Structure level of Yahoo conference key 107 (Great Northwest Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_121_id` | numeric | Yahoo numeric conference id carried for the Great American Conference conference in the league's division structure. |
| `conferences_121_name` | character | Conference name carried under Yahoo conference key 121, the Great American Conference conference. |
| `conferences_121_type` | character | Structure level of Yahoo conference key 121 (Great American Conference); "conference" for a full conference rather than a division within one. |
| `conferences_123_id` | numeric | Yahoo numeric conference id carried for the Great Midwest Athletic Conference conference in the league's division structure. |
| `conferences_123_name` | character | Conference name carried under Yahoo conference key 123, the Great Midwest Athletic Conference conference. |
| `conferences_123_type` | character | Structure level of Yahoo conference key 123 (Great Midwest Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_127_id` | numeric | Yahoo numeric conference id carried for the Great Lakes Valley Conference conference in the league's division structure. |
| `conferences_127_name` | character | Conference name carried under Yahoo conference key 127, the Great Lakes Valley Conference conference. |
| `conferences_127_type` | character | Structure level of Yahoo conference key 127 (Great Lakes Valley Conference); "conference" for a full conference rather than a division within one. |
| `conferences_128_id` | numeric | Yahoo numeric conference id carried for the Mountain East Conference conference in the league's division structure. |
| `conferences_128_name` | character | Conference name carried under Yahoo conference key 128, the Mountain East Conference conference. |
| `conferences_128_type` | character | Structure level of Yahoo conference key 128 (Mountain East Conference); "conference" for a full conference rather than a division within one. |
| `conferences_285_id` | numeric | Yahoo numeric conference id carried for the Conference Carolinas conference in the league's division structure. |
| `conferences_285_name` | character | Conference name carried under Yahoo conference key 285, the Conference Carolinas conference. |
| `conferences_285_type` | character | Structure level of Yahoo conference key 285 (Conference Carolinas); "conference" for a full conference rather than a division within one. |
| `conferences_26_id` | numeric | Yahoo numeric conference id carried for the Eastern Collegiate conference in the league's division structure. |
| `conferences_26_name` | character | Conference name carried under Yahoo conference key 26, the Eastern Collegiate conference. |
| `conferences_26_type` | character | Structure level of Yahoo conference key 26 (Eastern Collegiate); "conference" for a full conference rather than a division within one. |
| `conferences_30_id` | numeric | Yahoo numeric conference id carried for the Midwest Intercollegiate conference in the league's division structure. |
| `conferences_30_name` | character | Conference name carried under Yahoo conference key 30, the Midwest Intercollegiate conference. |
| `conferences_30_type` | character | Structure level of Yahoo conference key 30 (Midwest Intercollegiate); "conference" for a full conference rather than a division within one. |
| `conferences_41_id` | numeric | Yahoo numeric conference id carried for the Centennial Football conference in the league's division structure. |
| `conferences_41_name` | character | Conference name carried under Yahoo conference key 41, the Centennial Football conference. |
| `conferences_41_type` | character | Structure level of Yahoo conference key 41 (Centennial Football); "conference" for a full conference rather than a division within one. |
| `conferences_46_id` | numeric | Yahoo numeric conference id carried for the Michigan Intercollegiate Athletic Association conference in the league's division structure. |
| `conferences_46_name` | character | Conference name carried under Yahoo conference key 46, the Michigan Intercollegiate Athletic Association conference. |
| `conferences_46_type` | character | Structure level of Yahoo conference key 46 (Michigan Intercollegiate Athletic Association); "conference" for a full conference rather than a division within one. |
| `conferences_49_id` | numeric | Yahoo numeric conference id carried for the Minnesota Intercollegiate Athletic conference in the league's division structure. |
| `conferences_49_name` | character | Conference name carried under Yahoo conference key 49, the Minnesota Intercollegiate Athletic conference. |
| `conferences_49_type` | character | Structure level of Yahoo conference key 49 (Minnesota Intercollegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_51_id` | numeric | Yahoo numeric conference id carried for the New England Football conference in the league's division structure. |
| `conferences_51_name` | character | Conference name carried under Yahoo conference key 51, the New England Football conference. |
| `conferences_51_type` | character | Structure level of Yahoo conference key 51 (New England Football); "conference" for a full conference rather than a division within one. |
| `conferences_53_id` | numeric | Yahoo numeric conference id carried for the North Coast Athletic conference in the league's division structure. |
| `conferences_53_name` | character | Conference name carried under Yahoo conference key 53, the North Coast Athletic conference. |
| `conferences_53_type` | character | Structure level of Yahoo conference key 53 (North Coast Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_55_id` | numeric | Yahoo numeric conference id carried for the Old Dominion Athletic conference in the league's division structure. |
| `conferences_55_name` | character | Conference name carried under Yahoo conference key 55, the Old Dominion Athletic conference. |
| `conferences_55_type` | character | Structure level of Yahoo conference key 55 (Old Dominion Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_56_id` | numeric | Yahoo numeric conference id carried for the Presidents' Athletic conference in the league's division structure. |
| `conferences_56_name` | character | Conference name carried under Yahoo conference key 56, the Presidents' Athletic conference. |
| `conferences_56_type` | character | Structure level of Yahoo conference key 56 (Presidents' Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_57_id` | numeric | Yahoo numeric conference id carried for the Southern California Intercollegiate Athletic conference in the league's division structure. |
| `conferences_57_name` | character | Conference name carried under Yahoo conference key 57, the Southern California Intercollegiate Athletic conference. |
| `conferences_57_type` | character | Structure level of Yahoo conference key 57 (Southern California Intercollegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_76_id` | numeric | Yahoo numeric conference id carried for the Independents (III) conference in the league's division structure. |
| `conferences_76_name` | character | Conference name carried under Yahoo conference key 76, the Independents (III) conference. |
| `conferences_76_type` | character | Structure level of Yahoo conference key 76 (Independents (III)); "conference" for a full conference rather than a division within one. |
| `conferences_79_id` | numeric | Yahoo numeric conference id carried for the Liberty conference in the league's division structure. |
| `conferences_79_name` | character | Conference name carried under Yahoo conference key 79, the Liberty conference. |
| `conferences_79_type` | character | Structure level of Yahoo conference key 79 (Liberty); "conference" for a full conference rather than a division within one. |
| `conferences_93_id` | numeric | Yahoo numeric conference id carried for the American Southwest conference in the league's division structure. |
| `conferences_93_name` | character | Conference name carried under Yahoo conference key 93, the American Southwest conference. |
| `conferences_93_type` | character | Structure level of Yahoo conference key 93 (American Southwest); "conference" for a full conference rather than a division within one. |
| `conferences_105_id` | numeric | Yahoo numeric conference id carried for the Heartland Collegiate Athletic conference in the league's division structure. |
| `conferences_105_name` | character | Conference name carried under Yahoo conference key 105, the Heartland Collegiate Athletic conference. |
| `conferences_105_type` | character | Structure level of Yahoo conference key 105 (Heartland Collegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_106_id` | numeric | Yahoo numeric conference id carried for the Wisconsin Intercollegiate Athletic Conference conference in the league's division structure. |
| `conferences_106_name` | character | Conference name carried under Yahoo conference key 106, the Wisconsin Intercollegiate Athletic Conference conference. |
| `conferences_106_type` | character | Structure level of Yahoo conference key 106 (Wisconsin Intercollegiate Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_109_id` | numeric | Yahoo numeric conference id carried for the Empire Eight conference in the league's division structure. |
| `conferences_109_name` | character | Conference name carried under Yahoo conference key 109, the Empire Eight conference. |
| `conferences_109_type` | character | Structure level of Yahoo conference key 109 (Empire Eight); "conference" for a full conference rather than a division within one. |
| `conferences_113_id` | numeric | Yahoo numeric conference id carried for the Upper Midwest Athletic Conference conference in the league's division structure. |
| `conferences_113_name` | character | Conference name carried under Yahoo conference key 113, the Upper Midwest Athletic Conference conference. |
| `conferences_113_type` | character | Structure level of Yahoo conference key 113 (Upper Midwest Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_114_id` | numeric | Yahoo numeric conference id carried for the USA South Athletic Conference conference in the league's division structure. |
| `conferences_114_name` | character | Conference name carried under Yahoo conference key 114, the USA South Athletic Conference conference. |
| `conferences_114_type` | character | Structure level of Yahoo conference key 114 (USA South Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_129_id` | numeric | Yahoo numeric conference id carried for the Massachusetts State Collegiate Athletic Conference conference in the league's division structure. |
| `conferences_129_name` | character | Conference name carried under Yahoo conference key 129, the Massachusetts State Collegiate Athletic Conference conference. |
| `conferences_129_type` | character | Structure level of Yahoo conference key 129 (Massachusetts State Collegiate Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_130_id` | numeric | Yahoo numeric conference id carried for the Southern Athletic Association conference in the league's division structure. |
| `conferences_130_name` | character | Conference name carried under Yahoo conference key 130, the Southern Athletic Association conference. |
| `conferences_130_type` | character | Structure level of Yahoo conference key 130 (Southern Athletic Association); "conference" for a full conference rather than a division within one. |
| `conferences_132_id` | numeric | Yahoo numeric conference id carried for the Central Atlantic Collegiate Conference conference in the league's division structure. |
| `conferences_132_name` | character | Conference name carried under Yahoo conference key 132, the Central Atlantic Collegiate Conference conference. |
| `conferences_132_type` | character | Structure level of Yahoo conference key 132 (Central Atlantic Collegiate Conference); "conference" for a full conference rather than a division within one. |
| `conferences_48_id` | numeric | Yahoo numeric conference id carried for the Midwest Collegiate Athletic conference in the league's division structure. |
| `conferences_48_name` | character | Conference name carried under Yahoo conference key 48, the Midwest Collegiate Athletic conference. |
| `conferences_48_type` | character | Structure level of Yahoo conference key 48 (Midwest Collegiate Athletic); "conference" for a full conference rather than a division within one. |
| `conferences_62_id` | numeric | Yahoo numeric conference id carried for the Frontier conference in the league's division structure. |
| `conferences_62_name` | character | Conference name carried under Yahoo conference key 62, the Frontier conference. |
| `conferences_62_type` | character | Structure level of Yahoo conference key 62 (Frontier); "conference" for a full conference rather than a division within one. |
| `conferences_68_id` | numeric | Yahoo numeric conference id carried for the Mid-South conference in the league's division structure. |
| `conferences_68_name` | character | Conference name carried under Yahoo conference key 68, the Mid-South conference. |
| `conferences_68_type` | character | Structure level of Yahoo conference key 68 (Mid-South); "conference" for a full conference rather than a division within one. |
| `conferences_69_id` | numeric | Yahoo numeric conference id carried for the Chicagoland Collegiate conference in the league's division structure. |
| `conferences_69_name` | character | Conference name carried under Yahoo conference key 69, the Chicagoland Collegiate conference. |
| `conferences_69_type` | character | Structure level of Yahoo conference key 69 (Chicagoland Collegiate); "conference" for a full conference rather than a division within one. |
| `conferences_70_id` | numeric | Yahoo numeric conference id carried for the American Midwest conference in the league's division structure. |
| `conferences_70_name` | character | Conference name carried under Yahoo conference key 70, the American Midwest conference. |
| `conferences_70_type` | character | Structure level of Yahoo conference key 70 (American Midwest); "conference" for a full conference rather than a division within one. |
| `conferences_77_id` | numeric | Yahoo numeric conference id carried for the Independents (NAIA-I) conference in the league's division structure. |
| `conferences_77_name` | character | Conference name carried under Yahoo conference key 77, the Independents (NAIA-I) conference. |
| `conferences_77_type` | character | Structure level of Yahoo conference key 77 (Independents (NAIA-I)); "conference" for a full conference rather than a division within one. |
| `conferences_103_id` | numeric | Yahoo numeric conference id carried for the Cascade Collegiate Conference conference in the league's division structure. |
| `conferences_103_name` | character | Conference name carried under Yahoo conference key 103, the Cascade Collegiate Conference conference. |
| `conferences_103_type` | character | Structure level of Yahoo conference key 103 (Cascade Collegiate Conference); "conference" for a full conference rather than a division within one. |
| `conferences_104_id` | numeric | Yahoo numeric conference id carried for the Mid-States Football Association conference in the league's division structure. |
| `conferences_104_name` | character | Conference name carried under Yahoo conference key 104, the Mid-States Football Association conference. |
| `conferences_104_type` | character | Structure level of Yahoo conference key 104 (Mid-States Football Association); "conference" for a full conference rather than a division within one. |
| `conferences_115_id` | numeric | Yahoo numeric conference id carried for the Great Plains Athletic Conference conference in the league's division structure. |
| `conferences_115_name` | character | Conference name carried under Yahoo conference key 115, the Great Plains Athletic Conference conference. |
| `conferences_115_type` | character | Structure level of Yahoo conference key 115 (Great Plains Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_116_id` | numeric | Yahoo numeric conference id carried for the Heart of America Athletic Conference conference in the league's division structure. |
| `conferences_116_name` | character | Conference name carried under Yahoo conference key 116, the Heart of America Athletic Conference conference. |
| `conferences_116_type` | character | Structure level of Yahoo conference key 116 (Heart of America Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_117_id` | numeric | Yahoo numeric conference id carried for the Kansas Collegiate Athletic Conference conference in the league's division structure. |
| `conferences_117_name` | character | Conference name carried under Yahoo conference key 117, the Kansas Collegiate Athletic Conference conference. |
| `conferences_117_type` | character | Structure level of Yahoo conference key 117 (Kansas Collegiate Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_119_id` | numeric | Yahoo numeric conference id carried for the Red River Athletic Conference conference in the league's division structure. |
| `conferences_119_name` | character | Conference name carried under Yahoo conference key 119, the Red River Athletic Conference conference. |
| `conferences_119_type` | character | Structure level of Yahoo conference key 119 (Red River Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_133_id` | numeric | Yahoo numeric conference id carried for the New England Women's and Men's Athletic Conference conference in the league's division structure. |
| `conferences_133_name` | character | Conference name carried under Yahoo conference key 133, the New England Women's and Men's Athletic Conference conference. |
| `conferences_133_type` | character | Structure level of Yahoo conference key 133 (New England Women's and Men's Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_135_id` | numeric | Yahoo numeric conference id carried for the Wolverine-Hoosier Athletic Conference conference in the league's division structure. |
| `conferences_135_name` | character | Conference name carried under Yahoo conference key 135, the Wolverine-Hoosier Athletic Conference conference. |
| `conferences_135_type` | character | Structure level of Yahoo conference key 135 (Wolverine-Hoosier Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_136_id` | numeric | Yahoo numeric conference id carried for the Sooner Athletic Conference conference in the league's division structure. |
| `conferences_136_name` | character | Conference name carried under Yahoo conference key 136, the Sooner Athletic Conference conference. |
| `conferences_136_type` | character | Structure level of Yahoo conference key 136 (Sooner Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_137_id` | numeric | Yahoo numeric conference id carried for the Southern States Athletic Conference conference in the league's division structure. |
| `conferences_137_name` | character | Conference name carried under Yahoo conference key 137, the Southern States Athletic Conference conference. |
| `conferences_137_type` | character | Structure level of Yahoo conference key 137 (Southern States Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_139_id` | numeric | Yahoo numeric conference id carried for the Midlands Collegiate Athletic Conference conference in the league's division structure. |
| `conferences_139_name` | character | Conference name carried under Yahoo conference key 139, the Midlands Collegiate Athletic Conference conference. |
| `conferences_139_type` | character | Structure level of Yahoo conference key 139 (Midlands Collegiate Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_141_id` | numeric | Yahoo numeric conference id carried for the Crossroads League conference in the league's division structure. |
| `conferences_141_name` | character | Conference name carried under Yahoo conference key 141, the Crossroads League conference. |
| `conferences_141_type` | character | Structure level of Yahoo conference key 141 (Crossroads League); "conference" for a full conference rather than a division within one. |
| `conferences_281_id` | numeric | Yahoo numeric conference id carried for the The Sun Conference conference in the league's division structure. |
| `conferences_281_name` | character | Conference name carried under Yahoo conference key 281, the The Sun Conference conference. |
| `conferences_281_type` | character | Structure level of Yahoo conference key 281 (The Sun Conference); "conference" for a full conference rather than a division within one. |
| `conferences_284_id` | numeric | Yahoo numeric conference id carried for the New South Athletic Conference conference in the league's division structure. |
| `conferences_284_name` | character | Conference name carried under Yahoo conference key 284, the New South Athletic Conference conference. |
| `conferences_284_type` | character | Structure level of Yahoo conference key 284 (New South Athletic Conference); "conference" for a full conference rather than a division within one. |
| `conferences_125_id` | numeric | Yahoo numeric conference id carried for the Independents (ASCAA) conference in the league's division structure. |
| `conferences_125_name` | character | Conference name carried under Yahoo conference key 125, the Independents (ASCAA) conference. |
| `conferences_125_type` | character | Structure level of Yahoo conference key 125 (Independents (ASCAA)); "conference" for a full conference rather than a division within one. |

**teamcolor_primary**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**teamcolor_secondary**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gamehighlight**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `value` | character | Numeric or string value field. |

**gametv_details**

| col_name | type | description |
|---|---|---|
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `tv_details` | character | JSON-encoded list of broadcast entries for the game, each carrying a network abbreviation and full channel name (e.g., [{"abbr": "NBC", "name": "NBC/Peacock"}]). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_editorial_scoreboard-example}

```python
yahoo_editorial_scoreboard()
```

_Last validated n/a._
