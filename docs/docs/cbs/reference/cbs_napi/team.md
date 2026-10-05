---
title: "CBS — CBS Sports NAPI (api.cbssports.com/napi) — Team"
sidebar_label: "Team"
sidebar_position: 3
description: "CBS — CBS Sports NAPI (api.cbssports.com/napi) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CBS — CBS Sports NAPI (api.cbssports.com/napi) — Team

## cbs_team_futures

Get futures for a team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/futures/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/futures/404](https://api.cbssports.com/napi/resource/team/futures/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |

### Returns {#cbs_team_futures-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_futures-example}

```python
cbs_team_futures(team_id=404)
```

_Last validated n/a._

## cbs_team_metadata

Get a team metadata resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/metadata/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/metadata/404](https://api.cbssports.com/napi/resource/team/metadata/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: team. |

### Returns {#cbs_team_metadata-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_metadata-example}

```python
cbs_team_metadata(team_id=404)
```

_Last validated n/a._

## cbs_team_players

Get player resources on a team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/players/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/players/404](https://api.cbssports.com/napi/resource/team/players/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: playerTeamAssociations, injuries, transactions, depthCharts. |

### Returns {#cbs_team_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `full_first_name` | character | Player's full given name including any middle names CBS records (Kieran James Ricardo); null throughout the US leagues and populated mainly in the soccer leagues. |
| `last_name` | character | Player's last name. |
| `full_last_name` | character | Player's full family name as CBS records it, which can be longer than the display last_name; null throughout the US leagues and populated mainly in soccer. |
| `nick_name` | character | Player nickname. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | integer | Player weight in pounds. |
| `experience` | integer | Years of professional experience. |
| `school` | character | Team name. |
| `home_town` | character | Home town of the player. |
| `debut` | character | Date of the player's debut for the team as CBS records it; null for every player in the captured leagues, so the shipped format is unverified. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `birth_country` | character | Player birth country. |
| `birth_country_code` | character | Lowercase three-letter code for the player's country of birth, e.g. eng, wal, fra; populated in the soccer leagues and null in the US ones. |
| `nationality_country` | character | Country the player represents internationally, spelled out (England, Wales, France); can differ from birth_country for dual-eligible players. |
| `nationality_country_code` | character | Lowercase three-letter code matching nationality_country, e.g. eng. |
| `locked` | integer | Flag CBS sets on a player record it treats as locked (0 or 1); 0 for nearly every player in the captures. |
| `player_team_associations` | character | Nested list of the player's team associations, JSON-encoded when present; null unless the request named it in the endpoint's resources parameter, which defaults to none. |
| `injuries` | character | Nested injury records for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `transactions` | character | Nested transaction records for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `depth_charts` | character | Nested depth-chart entries for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `position_rankings` | character | Nested positional-ranking entries for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `player_stats` | character | Nested statistical lines for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `standings` | character | Nested standings sub-resource attached to the player's team, JSON-encoded when present; null unless requested through the resources parameter. |
| `rankings` | character | Nested ranking entries for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `player_outlook` | character | Nested fantasy-outlook copy for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `meta_data` | character | Nested metadata block for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `draft_info` | character | Draft information. |
| `game_stats` | character | Nested per-game statistical lines for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `combine_data` | character | Nested scouting-combine measurements for the player, JSON-encoded when present; null unless requested through the resources parameter. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_players-example}

```python
cbs_team_players(team_id=404)
```

_Last validated n/a._

## cbs_team_polls

Retrieve team rankings data from our various polls.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/polls/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/polls/404](https://api.cbssports.com/napi/resource/team/polls/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `polls` | `polls` |  |  | `Y` | View option.  Filter by a certain poll name. Allowed: coaches, ap, fcscoachespoll, statstsnfcspoll, rpi, playoffselectioncommitteepoll, net. |
| `seasonId` | `season_id` |  |  | `Y` | View option. Filter by seasonId. |

### Returns {#cbs_team_polls-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_polls-example}

```python
cbs_team_polls(team_id=404)
```

_Last validated n/a._

## cbs_team_rankings

Get rankings for a team (by season)

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/rankings/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/rankings/404](https://api.cbssports.com/napi/resource/team/rankings/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team id |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |

### Returns {#cbs_team_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_rankings-example}

```python
cbs_team_rankings(team_id=404)
```

_Last validated n/a._

## cbs_team_rankings_sportsline

Get sportsline rankings for a team

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/rankings/sportsline/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/rankings/sportsline/404](https://api.cbssports.com/napi/resource/team/rankings/sportsline/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team id |

### Returns {#cbs_team_rankings_sportsline-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_rankings_sportsline-example}

```python
cbs_team_rankings_sportsline(team_id=404)
```

_Last validated n/a._

## cbs_team_seasons

Get a list of Season resources associated to a Team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/seasons/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/seasons/404](https://api.cbssports.com/napi/resource/team/seasons/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: league. |

### Returns {#cbs_team_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_seasons-example}

```python
cbs_team_seasons(team_id=404)
```

_Last validated n/a._

## cbs_team_standings

Get standings for a particular team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/standings/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/standings/404](https://api.cbssports.com/napi/resource/team/standings/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `year` | `year` |  |  | `Y` | Optional year in YYYY format |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. v3 only! Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. v3 only! |

### Returns {#cbs_team_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `streak` | character | Current streak (e.g. 'W3' for three-game win streak). |
| `win_loss_record` | character | JSON-encoded array of the team's split records, one object per split carrying wins, losses, ties (plus shootout and overtime splits in the NHL) with a name and a type such as home, away, division, conference or last5. |
| `season_id` | integer | Unique season identifier. |
| `wins_number` | integer | Wins the team recorded over the season and season type of this row. |
| `goals_for_goals` | integer | Goals the team scored, on the soccer and hockey standings shapes. |
| `last_results_r2` | character | Second of the five form-guide slots CBS publishes for a soccer team, reported as Win, Loss or Draw. |
| `last_results_r3` | character | Third of the five form-guide slots CBS publishes for a soccer team, reported as Win, Loss or Draw. |
| `last_results_r4` | character | Fourth of the five form-guide slots CBS publishes for a soccer team, reported as Win, Loss or Draw. |
| `last_results_r5` | character | Fifth of the five form-guide slots CBS publishes for a soccer team, reported as Win, Loss or Draw. |
| `last_results_r1` | character | One of the team's five most recent results in the soccer standings form guide, reported as Win, Loss or Draw; CBS labels the five slots r1 through r5 without documenting which end is the most recent match. |
| `winning_percentage_percentage` | character | Winning percentage as CBS formats it, a leading-dot three-decimal string such as .706. |
| `goals_against_goals` | integer | Goals conceded by the team, on the soccer and hockey standings shapes. |
| `losses_number` | integer | Losses the team recorded over the season and season type of this row. |
| `points_penalty_points` | integer | Table points deducted from the team as a sanction, on the soccer standings shape; 0 for teams with no deduction. |
| `points_points_per_game` | character | Table points per game played, on the soccer standings shape, e.g. 2.33. |
| `points_points` | integer | League table points the team has earned, on the soccer standings shape (three per win, one per draw). |
| `ties_number` | character | Ties (draws) the team recorded; 0 in the leagues that no longer play to a tie, and typed as text because CBS emits an empty string where the concept does not apply. |
| `games_played_games` | integer | Games the team has played in the season and season type of this row. |
| `place_previous` | integer | Position the team held in the previously published version of the same table, so movement can be shown; equal to place_place when the team did not move. |
| `place_season_end_id` | character | Numeric code paired with place_season_end (7 for Europa League, 4 for Champions League, 9 for Relegation); an empty string when no end-of-season label applies. |
| `place_season_end` | character | End-of-season outcome CBS attaches to the team's finishing position on the soccer standings shape, e.g. Champions League, Europa League Qualifying, Relegation Playoffs; an empty string when the finish carries no label. |
| `place_place` | integer | Position the team occupies in the standings table it is ranked within, 1 being top. |
| `clinch_status_id` | integer | Numeric code for the qualification or clinch status CBS assigns the team on the soccer standings shape, paired with clinch_status_status. |
| `clinch_status_status` | character | Qualification or clinch status spelled out on the soccer standings shape, e.g. Advanced to Knockout Stage or Europa League. |
| `team_info_display_name` | character | Display name CBS uses for the team on the soccer standings shape, e.g. Arsenal. |
| `team_info_name` | character | Nickname portion of the team's name on the soccer standings shape; an empty string for the soccer clubs, which carry their identity in team_info_display_name. |
| `team_info_alias` | character | Short alias CBS uses for the team on the soccer standings shape, e.g. ARS. |
| `team_info_location` | character | Location portion of the team's name on the soccer standings shape, which for many clubs repeats the club name rather than a city. |
| `team_info_id` | integer | CBS team identifier carried on the soccer standings shape's team-info block; league-scoped rather than global, and paired with team_info_global_id. |
| `team_info_global_id` | integer | CBS global team identifier on the soccer standings shape, stable across leagues and seasons where team_info_id is not. |
| `season_season_id` | integer | CBS season identifier for the standings row, an eight-digit id on modern rows (29444245 for the 2025 NFL regular season) and a small legacy number on the oldest ones. |
| `season_sport_id` | integer | CBS sport identifier for the season (1 football, 2 baseball, 3 basketball, 4 hockey, 5 soccer). |
| `season_league_id` | integer | CBS league identifier for the season, e.g. 59 for the NFL, 60 for the NHL, 52 for MLB. |
| `season_league` | character | League record nested inside the season block; CBS returns it as null on every captured standings payload. |
| `season_teams` | character | Team list nested inside the season block; CBS returns it as an empty array on standings payloads, JSON-encoded to []. |
| `season_season_year` | integer | Calendar year CBS keys the season by, matching the year key the standings block was nested under. |
| `season_is_current` | integer | Flag marking the season as the one currently under way (1 current, 0 historical). |
| `season_season_type` | character | Portion of the season the row covers, one of pre, regular or post. |
| `season_season_type_desc` | character | Human-readable label for season_season_type, e.g. Regular season. |
| `season_season_start_date` | character | First day of the season formatted MM-DD-YYYY HH:MM:SS with a UTC offset, e.g. 09-04-2025 00:00:00 -0400. |
| `season_season_end_date` | character | Last day of the season in the same MM-DD-YYYY HH:MM:SS plus UTC-offset format, e.g. 01-04-2026 23:59:59 -0500. |
| `goal_differential_differential` | integer | Goals scored minus goals conceded, on the soccer standings shape. |
| `clinched_division_clinched_id` | integer | Numeric mirror of clinched_division_clinched (1 clinched, 0 not). |
| `clinched_division_clinched` | character | Whether the team has clinched its division. |
| `team_code_id` | integer | League-scoped team code on the US-league standings shape, a small per-league sequence (3 for the Angels, 34 for the Texans) rather than the global id. |
| `team_code_global_id` | integer | CBS global team identifier on the US-league standings shape, e.g. 227 for the Angels and 325 for the Texans. |
| `team_city_city` | character | City CBS attributes the team to on the US-league standings shape, e.g. Houston. |
| `clinched_playoff_spot_clinched_id` | integer | Numeric mirror of clinched_playoff_spot_clinched (1 clinched, 0 not). |
| `clinched_playoff_spot_clinched` | character | Whether the team has clinched a playoff berth, on the MLB standings shape. |
| `games_back_number` | character | Games behind the leader of the team's standings group; typed as text because CBS emits a dash on rows where it does not compute one. |
| `division_rank_tied` | character | Whether the team's division rank is shared with another team. |
| `division_rank_rank` | integer | Team's rank within its division, 1 being best. |
| `division_rank_tied_id` | integer | Numeric mirror of division_rank_tied (1 tied, 0 not). |
| `streak_kind` | character | Direction of the team's current run when CBS returns a single streak object, e.g. winning or losing (the NHL returns an array instead, kept in the streak column). |
| `streak_games` | integer | Length in games of the current run described by streak_kind. |
| `wild_card_rank_tied` | character | Whether the team's wild-card rank is shared with another team. |
| `wild_card_rank_rank` | integer | Team's rank in the wild-card race, on the MLB and NHL standings shapes. |
| `wild_card_rank_tied_id` | integer | Numeric mirror of wild_card_rank_tied (1 tied, 0 not). |
| `today_games_included_through` | character | Whether the standings figures already account for games played today. |
| `today_games_included_through_id` | integer | Numeric mirror of today_games_included_through (1 included, 0 not). |
| `eliminated_from_playoffs_eliminated` | character | Whether the team has been eliminated from playoff contention. |
| `wc_games_back_number` | character | Games behind the last wild-card position, on the MLB standings shape. |
| `team_name_name` | character | Nickname CBS uses for the team on the US-league standings shape, e.g. Texans or Angels. |
| `team_name_alias` | character | Short alias for the team on the US-league standings shape, e.g. Hou or LAA. |
| `elimination_number_number` | character | Elimination number for the team, the combined team losses and rival wins that would end its contention; 0 once the outcome is settled, and typed as text because CBS emits an empty string where it does not compute one. |
| `wc_elimination_number_number` | integer | Elimination number for the team's wild-card contention specifically, on the MLB standings shape. |
| `runs_allowed` | integer | Runs the team conceded, on the MLB standings shape. |
| `runs_scored` | integer | Runs the team scored, on the MLB standings shape. |
| `league_rank_tied` | character | Whether the team's league rank is shared with another team. |
| `league_rank_rank` | integer | Team's rank across the whole league, 1 being best. |
| `league_rank_tied_id` | integer | Numeric mirror of league_rank_tied (1 tied, 0 not). |
| `place_conference_rank` | integer | Rank the team holds within its conference, carried on the place block of the MLS standings shape. |
| `place_division_rank` | character | Rank the team holds within its division, carried on the place block of the MLS standings shape; an empty string for leagues or seasons without divisions. |
| `conference_conference_id` | integer | CBS conference identifier for the team's conference, paired with the conference name and abbreviation on the same block. |
| `conference_name` | character | Full conference name. |
| `conference_abbreviation` | character | Conference abbreviation. |
| `basketball_nba_playoffs_indicator` | character | JSON-encoded array of the NBA clinching markers CBS attaches to the team, each an object with a type such as clinched-playoffs, division-first or conference-first. |
| `points_for_per_game_points` | numeric | Points the team scored per game, on the NBA standings shape, e.g. 120.5. |
| `magic_number_number` | character | Magic number CBS publishes for the team's clinching scenario; 0 or negative once the scenario no longer applies to a clinched team, and an empty string on rows where CBS computes none (preseason blocks). |
| `conference_games_back_games` | integer | Games behind the conference leader, on the NBA standings shape. |
| `points_against_per_game_points` | numeric | Points the team conceded per game, on the NBA standings shape, e.g. 107.6. |
| `conference_seed_seed` | character | Team's current seeding within its conference bracket. |
| `conference_eos_seed_seed` | integer | Team's end-of-season conference seed, CBS's settled bracket position once the regular season is complete. |
| `points_for` | character | Goals/points scored. |
| `points_against` | character | Points allowed. |
| `won_conference_tournament_won` | logical | Whether the team won its conference tournament, on the NCAA standings shape. |
| `rpi_rank` | integer | Team's national rank by the rpi_rpi rating, 1 being best. |
| `rpi_rpi` | character | Ratings Percentage Index for the team on the NCAA standings shape, a leading-dot four-decimal string such as .4021. |
| `sequence_sequence` | integer | Sort position CBS assigns the team within its standings group; it tracks place_place closely but can sit one higher where teams are tied. |
| `clinched_conference_clinched` | logical | Whether the team has clinched its conference. |
| `college_code_id` | integer | CBS college identifier for the school on the NCAA standings shape, e.g. 2120 for USC Upstate. |
| `ineligible_ineligible` | logical | Whether the team is ineligible for postseason play, on the NCAA standings shape. |
| `sos_sos` | character | Strength-of-schedule rating on the NCAA standings shape, a leading-dot four-decimal string such as .4528. |
| `sos_rank` | integer | Team's rank by the sos_sos rating, 1 being the toughest schedule. |
| `college_name_name` | character | School name CBS uses on the NCAA standings shape, e.g. USC Upstate. |
| `ranking_ranking` | integer | Poll ranking CBS carries for the team on the NCAA standings shape; 0 across the captured rows, which is CBS's stand-in for unranked. |
| `net_ranking_rank` | integer | Team's NET ranking on the NCAA basketball standings shape, 1 being best. |
| `conference_games_back_number` | integer | Games behind the conference leader, on the NCAA standings shape, where CBS names the same quantity number rather than games. |
| `ranking_playoff_ranking` | integer | Playoff-committee ranking CBS carries for the team on the NCAA football standings shape; 0 across the captured rows, which is CBS's stand-in for unranked. |
| `clinched_playoffs_clinched` | logical | Whether the team has clinched a playoff berth, on the NFL standings shape. |
| `strength_of_schedule_rank` | character | Team's rank by strength of schedule on the NFL standings shape; typed as text because CBS emits a dash on rows where it publishes none. |
| `points_for_number` | integer | Points the team scored, on the NFL standings shape. |
| `points_against_number` | integer | Points the team conceded, on the NFL standings shape. |
| `clinched_home_field_clinched` | logical | Whether the team has clinched home-field advantage through the playoffs, on the NFL standings shape. |
| `clinched_first_round_bye_clinched` | character | Whether the team has clinched a first-round playoff bye, on the NFL standings shape. |
| `content` | character | Raw markup fragment CBS emits on some NFL standings blocks; the captured rows carry only the string /> and it holds no standings meaning. |
| `clinched_playoffs_date_date` | integer | Day of the month on which the team clinched a playoff berth. |
| `clinched_playoffs_date_month` | integer | Month of the date on which the team clinched a playoff berth, 1 through 12. |
| `clinched_playoffs_date_year` | integer | Year of the date on which the team clinched a playoff berth, on the NFL standings shape. |
| `clinched_playoffs_date_day` | integer | Day-of-week component CBS emits alongside the clinch date; the one captured NFL row pairs 6 with Saturday 12-27-2025, an ISO Monday-is-1 index. |
| `overtime_losses_number` | integer | Losses the team took in overtime, on the NHL standings shape. |
| `regulation_plus_overtime_wins_number` | integer | Wins the team earned in regulation or overtime, excluding shootout wins, on the NHL standings shape. |
| `shootout_losses_number` | integer | Losses the team took in a shootout, on the NHL standings shape. |
| `overtime_wins_number` | integer | Wins the team earned in overtime, on the NHL standings shape. |
| `team_points_number` | integer | Standings points the team has accumulated on the NHL shape (two per win, one per overtime or shootout loss). |
| `shootout_wins_number` | integer | Wins the team earned in a shootout, on the NHL standings shape. |
| `team_city_alternate` | character | Alternate city spelling CBS carries for the team on the NHL standings shape, which usually repeats team_city_city. |
| `hockey_nhl_conference_ranking_ranking` | character | Team's ranking within its NHL conference; typed as text because CBS emits both integers and zero-padded strings such as 05. |
| `hockey_nhl_playoffs_indicator_type` | character | Single NHL clinching marker for rows where CBS sends one object rather than an array, e.g. clinched-playoffs. |
| `regulation_wins_number` | integer | Wins the team earned in regulation time, on the NHL standings shape. |
| `hockey_nhl_playoffs_indicator` | character | JSON-encoded array of the NHL clinching markers CBS attaches to the team, each an object with a type such as clinched-playoffs, division-first, conference-first or presidents' trophy; rows where CBS sends a single object instead land in hockey_nhl_playoffs_indicator_type. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_standings-example}

```python
cbs_team_standings(team_id=404)
```

_Last validated n/a._

## cbs_team_standings_sportsline

Get SportsLine standings for a particular team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/standings/sportsline/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/standings/sportsline/404](https://api.cbssports.com/napi/resource/team/standings/sportsline/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |

### Returns {#cbs_team_standings_sportsline-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_standings_sportsline-example}

```python
cbs_team_standings_sportsline(team_id=404)
```

_Last validated n/a._

## cbs_team_stats

Get all statistics for a team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/stats/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/stats/404](https://api.cbssports.com/napi/resource/team/stats/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `isCurrent` | `is_current` |  |  | `Y` | View option.  Only show stats for seasons where isCurrent is true. Allowed: 1. |

### Returns {#cbs_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#cbs_team_stats-example}

```python
cbs_team_stats(team_id=404)
```

_Last validated n/a._
