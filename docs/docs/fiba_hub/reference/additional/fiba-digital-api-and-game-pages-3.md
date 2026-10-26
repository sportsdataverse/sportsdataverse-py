---
title: "FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: asiacup_player–bcl_player"
sidebar_label: "FIBA digital API and game pages: asiacup_player–bcl_player"
sidebar_position: 3
description: "FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: asiacup_player–bcl_player — function reference in sdv-py, the SportsDataverse Python package."
---
# FIBA (digital API and game pages) — additional Python functions — FIBA digital API and game pages: asiacup_player–bcl_player

### fiba_asiacup_player_box {#fiba_asiacup_player_box}

`fiba_asiacup_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Player box score of one FIBA Asia Cup game.

FIBA Asia Cup (men's); season rule: event year (2025 = 2025).

Convenience wrapper: a thin binding of `fiba_gamepage_box_players` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule). |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Required unless `comp` is given: a game function never guesses the edition. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. Checked to be an edition of this competition (one cached getGdapCompetitionById request). |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `player_id` | Int64 | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `first_name` | String | Given name(s) as registered with FIBA. |
| `last_name` | String | Last name as registered with FIBA. |
| `jersey` | String | Jersey number as a string. |
| `position` | String | Playing position code from the lineup card (PG, SG, SF, PF, C). |
| `is_captain` | Boolean | True for the team captain. |
| `is_starter` | Boolean | True for the starting five. |
| `has_played` | Boolean | True when the player got on the court. |
| `minutes` | String | Minutes played as an 'm:ss' string ('0:0' = did not play). |
| `seconds_played` | Int64 | Seconds played, derived from minutes. |
| `points` | Int64 | Points scored in the game. |
| `fg_made` | Int64 | Field goals made (2 + 3 points). |
| `fg_attempted` | Int64 | Field goals attempted (2 + 3 points). |
| `fg_pct` | Float64 | Field-goal percentage, 0-100. |
| `fg2_made` | Int64 | Two-point field goals made. |
| `fg2_attempted` | Int64 | Two-point field goals attempted. |
| `fg2_pct` | Float64 | Two-point field-goal percentage, 0-100. |
| `fg3_made` | Int64 | Three-point field goals made. |
| `fg3_attempted` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point field-goal percentage, 0-100. |
| `ft_made` | Int64 | Free throws made. |
| `ft_attempted` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100. |
| `reb_offensive` | Int64 | Offensive rebounds. |
| `reb_defensive` | Int64 | Defensive rebounds. |
| `reb_total` | Int64 | Total rebounds (a team row includes the team rebounds team_reb_total). |
| `assists` | Int64 | Assists credited in the game. |
| `steals` | Int64 | Steals credited in the game. |
| `blocks` | Int64 | Blocked shots (blocks made). |
| `blocks_received` | Int64 | Own shots blocked by the opponent (blocks received). |
| `turnovers` | Int64 | Turnovers (a team row includes the team turnovers team_turnovers). |
| `fouls` | Int64 | Personal fouls (a team row includes the team fouls team_fouls). |
| `fouls_drawn` | Int64 | Fouls drawn in the game. |
| `paint_fg_made` | Int64 | Field goals made in the paint (team points_in_paint = 2 x paint_fg_made on every sampled game). |
| `paint_fg_attempted` | Int64 | Field goals attempted in the paint. |
| `paint_fg_pct` | Float64 | Field-goal percentage in the paint, 0-100. |
| `efficiency` | Int64 | Efficiency (FIBA EFF: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers). |
| `plus_minus` | Int64 | Plus/minus: the team's point differential while the player was on the court. |
| `cb` | Boolean | Boolean flag, false on every sampled player; meaning unverified. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). FIBA lists no edition for the season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). `comp` is an edition of another competition family, or GDAP gives it no family to check. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. Neither `season` nor `comp` is given, or the season lists several editions (pass `comp`). |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_asiacup_schedule, fiba_asiacup_player_box
games = fiba_asiacup_schedule(season=2025)
fiba_asiacup_player_box(game_id=int(games['game_id'][0]), season=2025)
```

### fiba_asiacup_schedule {#fiba_asiacup_schedule}

`fiba_asiacup_schedule(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Schedule and results of one FIBA Asia Cup edition, one row per game.

FIBA Asia Cup (men's); season rule: event year (2025 = 2025).

Convenience wrapper: a thin binding of `fiba_gdap_schedule` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. Checked to be an edition of this competition (one cached getGdapCompetitionById request). |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id, global across competitions (GDAP key space): gdapGameId, the <gameId> of a game-page URL. |
| `game_name` | String | Technical game name '<roundId>-<groupPairingCode>-<gameNumber>'. |
| `game_number` | String | Game number within the round or pairing (a string: '1', 'A'). |
| `status_code` | String | Game status code: VALID (validated result), CLOS (closed, pre-2000 rows). |
| `team_a_team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_a_organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `team_a_code` | String | Team code (national: FIBA country code; club: a club abbreviation such as OOST). |
| `team_a_official_name` | String | Official name of team A: the country for a national team, the club for a club team. |
| `team_a_short_name` | String | Short name of team A: the country code for a national team, the club's short name for a club. |
| `team_b_team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_b_organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `team_b_code` | String | Team code (national: FIBA country code; club: a club abbreviation such as NANT). |
| `team_b_official_name` | String | Official name of team B: the country for a national team, the club for a club team. |
| `team_b_short_name` | String | Short name of team B: the country code for a national team, the club's short name for a club. |
| `team_a_score` | Int64 | Final score of team A. |
| `team_b_score` | Int64 | Final score of team B. |
| `is_live` | Boolean | True while the game is being played. |
| `current_period` | String | Last period reached ('Q4', 'OT1'). |
| `chrono` | String | Game clock of the current period, 'mm:ss' ('00:00' when finished). |
| `live_game_status` | Int64 | Live state code: 999 on every finished game; 5 seen on a finished 2024 youth game; mapping unverified. |
| `current_period_status` | String | State of the current period: E = ended (the only value sampled). |
| `host_city` | String | City of the venue. |
| `host_country` | String | Country of the venue (name; a FIBA code in getgdapgameswithleaderdetailsbyteamid). |
| `host_country_code` | String | FIBA code of the venue's country. |
| `venue_id` | Int64 | Venue id (GDAP key space; 0 = unknown). |
| `venue_name` | String | Name of the arena the game is played in. |
| `game_date_time` | String | Tip-off in venue-local wall-clock time, ISO 8601 without offset (T00:00:00 when hasTimeGameDateTime is false). |
| `game_date_time_utc` | String | Tip-off in UTC, ISO 8601 without the Z. |
| `has_time_game_date_time` | Boolean | False when only the date is known (the time part is then 00:00:00). |
| `iana_time_zone` | String | IANA time zone of the venue. |
| `utc_offset` | Float64 | Venue UTC offset at tip-off, in hours (Float64: a half-hour zone such as India +5.5 or Adelaide +9.5 is a fraction; whole hours in every sample). |
| `is_postponed` | Boolean | True when the game was postponed. |
| `is_played_behind_closed_doors` | Boolean | True when played without spectators. |
| `venue_capacity` | Int64 | Venue capacity (0 = unknown). |
| `spectators` | Int64 | Attendance at the game, as recorded by FIBA. |
| `statistic_system` | String | Live-stats system that scored the game: OVR (FIBA's Olympic/World Cup system), FLS (FIBA LiveStats), null before about 2006. |
| `group_id` | String | Group id (GDAP key space). |
| `group_pairing_code` | String | Group letter (group play) or pairing number (knockout bracket slot). |
| `round_round_id` | Int64 | Round id (GDAP key space, unique across competitions); the first segment of gameName. |
| `round_round_code` | String | Round code (GP group phase, QF, SF, F final, RS regular season, CL17-32 classification...). |
| `round_round_name` | String | Round label (Group Phase, Quarter-Finals, Final...). |
| `round_round_number` | Int64 | Round number within the competition (1-based). |
| `round_round_type` | String | Round type label: Group Round, Non Group Round, Super Round (getgdapgamesbycompetitionid uses the code G/NG here). |
| `round_round_status_code` | String | Round status: COMPL completed, PROGR in progress, CLOS closed. |
| `round_game_system` | String | Pairing system label (Single, Home and Away, Best of 3). |
| `round_game_system_code` | String | Pairing system code: S single game, HA home and away, BOF3 best of three. |
| `round_start_date` | String | Round start, local date-time. |
| `round_end_date` | String | Round end, local date-time (23:59:59.9999999 on the last day). |
| `round_round_kind` | String | Standard Round or Qualifiers. |
| `round_round_order` | Int64 | Display order of the round. |
| `competition_competition_id` | Int64 | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `competition_competition_code` | String | Competition code (WMM men's World Cup, WOLYM, BCL, ELW...). |
| `competition_season` | Int64 | Season key, three rules: the END year for club competitions (BCL 2024-25 = 2025) and the event year for national-team events (World Cup 2023 = 2023), both equal to the sdv season key; qualifiers carry the TARGET tournament year, not the year played (World Cup 2027 qualifiers played in 2025 = 2027, EuroBasket 2029 pre-qualifiers = 2029), whose sdv mapping is an open Phase B decision. |
| `competition_official_name` | String | Edition title (FIBA Basketball World Cup; Paris 2024 Men's Olympic Basketball Tournament). |
| `competition_start` | String | First day, YYYY-MM-DD. |
| `competition_end` | String | Last day, YYYY-MM-DD. |
| `competition_status` | String | Competition status: In Progress, Completed, Closed. |
| `competition_age_category` | String | Age category (Senior, Under 16...). |
| `competition_gender` | String | Men or Women: the gender of the competition. |
| `competition_fiba_zone` | String | FIBA zone the competition belongs to (World, Europe, Americas...). |
| `competition_zone_information_zone_id` | Int64 | FIBA zone id (1 = World, 23 = Europe; zone table key space). |
| `competition_zone_information_zone_official_name` | String | Official name of the competition's FIBA zone (FIBA Europe, FIBA Americas, World). |
| `competition_zone_information_zone_code` | String | Zone code (W, E, AF, AM, AS, OC). |
| `competition_zone_information_top_zone_id` | Int64 | Parent zone id; null for top zones. |
| `competition_zone_information_top_zone_official_name` | String | Parent zone name; always null in the samples, values unverified. |
| `competition_zone_information_top_zone_code` | String | Parent zone code; always null in the samples, values unverified. |
| `competition_competition_type` | String | National Teams or Clubs. |
| `competition_competition_category_is_league` | Boolean | True for league-format club competitions. |
| `competition_competition_category_age_type` | String | Age type code: S senior, U16. |
| `competition_competition_category_code` | String | Category code (WC World Cup, OLY Olympic Tournament, CCC-1 continental club tier 1). |
| `competition_competition_category_name` | String | Name of the competition's category (FIBA Basketball World Cup, Olympic Basketball Tournament...). |
| `competition_official_flag_item_code` | String | Official-status code: O = official. |
| `competition_official_flag_item_name` | String | Official-status label. |
| `competition_official` | String | Official-status label. |
| `competition_discipline` | String | Discipline (Basketball; 3x3 is a separate platform). |
| `competition_competition_marketing_name_competition_marketing_name_id` | Int64 | Competition family id (201 = World Cup, 112 = BCL tier 1, 211 = EuroLeague Women); the <histId> of /en/history/ pages. |
| `competition_competition_marketing_name_marketing_name` | String | Competition family name; slugified it is the <histSlug> of /en/history/ pages. |
| `competition_competition_marketing_name_code` | String | Family code; null in every sample. |
| `game_statistic_status_code` | String | Box-score validation status: VALID. |
| `game_play_by_play_status_code` | String | Play-by-play availability flag: VALID or EMPTY. UNRELIABLE: World Cup 2014 and EuroBasket 2017 say EMPTY but their game pages carry full play-by-play; always check the page. |
| `game_result_status_code` | String | Result validation status: VALID. |
| `game_result_additional_information` | String | Free-text result note (forfeit, overtime remark); null when none. |
| `window_code` | String | Qualifier window code; null otherwise. |
| `window_name` | String | Qualifier window name; null otherwise. |
| `team_a_from` | String | How team A qualified for this slot ('Winner of Game 87', '1st of group C'); null in group play. |
| `team_b_from` | String | How team B qualified for this slot ('Winner of Game 88', '2nd of group A'); null in group play. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). FIBA lists no edition for the season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). `comp` is an edition of another competition family, or GDAP gives it no family to check. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. The season lists several editions (pass `comp`). |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_asiacup_schedule
fiba_asiacup_schedule(season=2025)
```

### fiba_asiacup_shots {#fiba_asiacup_shots}

`fiba_asiacup_shots(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Shot chart of one FIBA Asia Cup game.

FIBA Asia Cup (men's); season rule: event year (2025 = 2025).

Convenience wrapper: a thin binding of `fiba_gamepage_shots` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule). |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Required unless `comp` is given: a game function never guesses the edition. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. Checked to be an edition of this competition (one cached getGdapCompetitionById request). |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | String | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `period` | String | Period name: Q1..Q4, then OT1, OT2... |
| `period_number` | Int64 | 1-based position of the period in the game. |
| `action_id` | String | Action id (String): '<n>:<seq>' in OVR games (3:0015), a sequence number in FLS games; unique within the game. |
| `order` | Int64 | Sort key within the period (OVR: 1, 2, ...; FLS: 1000, 2000, ...); null on some period events. |
| `clock` | String | Game clock: time REMAINING in the period, 'm:ss', counting down from 10:00 (5:00 in overtime). |
| `clock_seconds` | Int64 | Seconds remaining in the period, derived from clock. |
| `action_code` | String | Action code: P2 two-point shot, P3 three-point shot, FT free throw, REB rebound, TREB team rebound, ASS assist, ST steal, BS block, TO turnover, TTO team turnover, FOUL foul, RFOUL foul drawn, SUBST substitution, TIMO timeout, plus period events (STARTG, STARTP, ENDP, ENDG). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `player_id` | Int64 | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `shot_type` | String | Shot type derived from action_code: 2PT, 3PT or FT; null on other actions. |
| `points_attempted` | Int64 | Points the attempt was worth (2, 3 or 1), made or not; null on other actions. |
| `made` | Boolean | True when the shot went in (two-pointers, three-pointers and free throws only; null otherwise). |
| `shot_x` | Int64 | Shot x, integer pixels on a half-court image across the court width (observed 0-278, basket near x = 139). Present only on shots; free throws are 0. Which sideline is x = 0 and the scale are UNVERIFIED. |
| `shot_y` | Int64 | Shot y, integer pixels from the baseline (y = 0) towards midcourt (observed 0-224, basket near y = 25); both teams' shots are drawn on one basket. Free throws are 0. Scale UNVERIFIED. |
| `has_location` | Boolean | True when shot_x / shot_y locate the shot (two- and three-pointers); False for free throws (always 0, 0); null on other actions. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). FIBA lists no edition for the season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). `comp` is an edition of another competition family, or GDAP gives it no family to check. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. Neither `season` nor `comp` is given, or the season lists several editions (pass `comp`). |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_asiacup_schedule, fiba_asiacup_shots
games = fiba_asiacup_schedule(season=2025)
fiba_asiacup_shots(game_id=int(games['game_id'][0]), season=2025)
```

### fiba_asiacup_standings {#fiba_asiacup_standings}

`fiba_asiacup_standings(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Final standings of one FIBA Asia Cup edition.

FIBA Asia Cup (men's); season rule: event year (2025 = 2025).

Convenience wrapper: a thin binding of `fiba_gdap_final_standings` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. Checked to be an edition of this competition (one cached getGdapCompetitionById request). |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | FIBA competition id (Int64) the final ranking belongs to; the comp argument. |
| `final_rank` | Int64 | Final rank in the competition. |
| `team_team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_team_code` | String | Team abbreviation (FIBA country code or club abbreviation). |
| `team_short_name` | String | Short team name: the country code for a national team, the club's short name for a club. |
| `team_profile` | String | Team profile; null in this route. |
| `team_status_code` | String | Team entry status; always null in the samples, values unverified. |
| `team_status_name` | String | Team entry status label; always null in the samples, values unverified. |
| `team_organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). FIBA lists no edition for the season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). `comp` is an edition of another competition family, or GDAP gives it no family to check. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. The season lists several editions (pass `comp`). |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_asiacup_standings
fiba_asiacup_standings(season=2025)
```

### fiba_asiacup_team_box {#fiba_asiacup_team_box}

`fiba_asiacup_team_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Team box score of one FIBA Asia Cup game.

FIBA Asia Cup (men's); season rule: event year (2025 = 2025).

Convenience wrapper: a thin binding of `fiba_gamepage_box_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule). |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Required unless `comp` is given: a game function never guesses the edition. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. Checked to be an edition of this competition (one cached getGdapCompetitionById request). |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | String | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `score` | Int64 | Team final score. |
| `points` | Int64 | Points scored in the game. |
| `fg_made` | Int64 | Field goals made (2 + 3 points). |
| `fg_attempted` | Int64 | Field goals attempted (2 + 3 points). |
| `fg_pct` | Float64 | Field-goal percentage, 0-100. |
| `fg2_made` | Int64 | Two-point field goals made. |
| `fg2_attempted` | Int64 | Two-point field goals attempted. |
| `fg2_pct` | Float64 | Two-point field-goal percentage, 0-100. |
| `fg3_made` | Int64 | Three-point field goals made. |
| `fg3_attempted` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point field-goal percentage, 0-100. |
| `ft_made` | Int64 | Free throws made. |
| `ft_attempted` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100. |
| `reb_offensive` | Int64 | Offensive rebounds. |
| `reb_defensive` | Int64 | Defensive rebounds. |
| `reb_total` | Int64 | Total rebounds (a team row includes the team rebounds team_reb_total). |
| `assists` | Int64 | Assists credited in the game. |
| `steals` | Int64 | Steals credited in the game. |
| `blocks` | Int64 | Blocked shots (blocks made). |
| `blocks_received` | Int64 | Own shots blocked by the opponent (blocks received). |
| `turnovers` | Int64 | Turnovers (a team row includes the team turnovers team_turnovers). |
| `fouls` | Int64 | Personal fouls (a team row includes the team fouls team_fouls). |
| `fouls_drawn` | Int64 | Fouls drawn in the game. |
| `paint_fg_made` | Int64 | Field goals made in the paint (team points_in_paint = 2 x paint_fg_made on every sampled game). |
| `paint_fg_attempted` | Int64 | Field goals attempted in the paint. |
| `paint_fg_pct` | Float64 | Field-goal percentage in the paint, 0-100. |
| `efficiency` | Int64 | Efficiency (FIBA EFF: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers). |
| `biggest_lead` | Int64 | Biggest lead, points. |
| `biggest_lead_score` | String | Score when the biggest lead was reached, 'A-B' string (spacing varies; empty when the team never led). |
| `biggest_run` | Int64 | Biggest scoring run, points. |
| `biggest_run_score` | String | Score of the biggest scoring run, 'x-y' string; exact meaning unverified. |
| `fast_break_points` | Int64 | Fast-break points. |
| `points_off_turnovers` | Int64 | Points scored after turnovers. |
| `bench_points` | Int64 | Points from the bench (the sum of the non-starters' points). |
| `points_in_paint` | Int64 | Points in the paint (2 x paint_fg_made). |
| `second_chance_points` | Int64 | Second-chance points. |
| `a_act` | Int64 | Always 0 in the samples; meaning unverified (FIBA stat code A_ACT). |
| `a_foul` | Int64 | Always 0 in the samples; meaning unverified (FIBA stat code A_FOUL). |
| `a_tod` | Int64 | Always 0 in the samples; meaning unverified (FIBA stat code A_TOD). |
| `a_tot` | Int64 | Always 0 in the samples; meaning unverified (FIBA stat code A_TOT). |
| `team_reb_offensive` | Int64 | Team offensive rebounds, credited to no player. |
| `team_reb_defensive` | Int64 | Team defensive rebounds, credited to no player. |
| `team_reb_total` | Int64 | Team rebounds (total rebounds minus the players' sum). |
| `team_fouls` | Int64 | Team (bench/coach) fouls (personal fouls minus the players' sum). |
| `team_turnovers` | Int64 | Team turnovers (turnovers minus the players' sum). |
| `team_bench_points_flag` | Int64 | Bench points in FLS games (equal to bench_points); 0 in OVR games (FIBA stat code T_PFB). |
| `time_led` | String | Time the team led, 'mm:ss' string. |
| `lead_changes` | Int64 | Lead changes in the game (game-level; repeats on both team rows). |
| `times_tied` | Int64 | Times the score was tied (game-level; repeats on both team rows). |
| `duration` | String | Game duration as an 'h:mm' string (game-level; repeats on both team rows). |
| `attendance` | Int64 | Attendance (game-level; repeats on both team rows). |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). FIBA lists no edition for the season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). `comp` is an edition of another competition family, or GDAP gives it no family to check. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. Neither `season` nor `comp` is given, or the season lists several editions (pass `comp`). |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_asiacup_schedule, fiba_asiacup_team_box
games = fiba_asiacup_schedule(season=2025)
fiba_asiacup_team_box(game_id=int(games['game_id'][0]), season=2025)
```

### fiba_asiacup_teams {#fiba_asiacup_teams}

`fiba_asiacup_teams(season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Teams of one FIBA Asia Cup edition.

FIBA Asia Cup (men's); season rule: event year (2025 = 2025).

Convenience wrapper: a thin binding of `fiba_gdap_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Omitted = the newest edition that has started. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. Checked to be an edition of this competition (one cached getGdapCompetitionById request). |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `team_code` | String | Team abbreviation (FIBA country code or club abbreviation). |
| `short_name` | String | Short team name: the country code for a national team, the club's short name for a club. |
| `profile_team_id` | Int64 | Competition team id: one per team per competition (Germany = 279569 at the 2023 World Cup, 281545 at Paris 2024). Joins getgdapcompetitionteamrosterbyteamid / getgdapcompetitionteamstatisticsbyteamid gdapTeamId; not an organisationId. |
| `profile_organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |
| `profile_name` | String | Team name as registered for the competition. |
| `profile_code` | String | Team code (FIBA country code or club abbreviation). |
| `profile_country_name` | String | Country of the team, in English. |
| `profile_country_code` | String | Country FIBA code. |
| `profile_gender` | String | Men or Women: the gender of the team's competition. |
| `profile_facebook` | String | Facebook handle or URL; always null in the samples, values unverified. |
| `profile_twitter` | String | X/Twitter handle; always null in the samples, values unverified. |
| `profile_instagram` | String | Instagram handle; always null in the samples, values unverified. |
| `profile_website` | String | The team's website address. |
| `profile_you_tube` | String | YouTube handle; always null in the samples, values unverified. |
| `profile_final_ranking` | Int64 | Final rank in the competition. |
| `profile_points_per_game` | Float64 | The team's average points per game played. |
| `profile_rebounds_per_game` | Float64 | The team's average rebounds per game played. |
| `profile_assists_per_game` | Float64 | The team's average assists per game played. |
| `profile_rank_points_per_game` | Float64 | Rank in the competition by points per game (1 = highest). |
| `profile_rank_rebounds_per_game` | Float64 | Rank in the competition by rebounds per game (1 = highest). |
| `profile_rank_assists_per_game` | Float64 | Rank in the competition by assists per game (1 = highest). |
| `profile_total_games_won` | Int64 | The team's total games won over the games played. |
| `profile_total_games_lost` | Int64 | The team's total games lost over the games played. |
| `profile_social_networks` | String | Social-network handles. Stored as a JSON string. |
| `profile_competition_id` | Int64 | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `status_code` | String | Team entry status: APPR approved. |
| `status_name` | String | Team entry status label (Approved). |
| `organisation_id` | Int64 | Organisation id, stable across competitions and seasons: the national federation for a national team (64 = Germany), the club for a club team (396 = Filou Oostende). Same key space as nationalFederationId, clubId, play-by-play oId and the game page's T_<id>. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). FIBA lists no edition for the season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). `comp` is an edition of another competition family, or GDAP gives it no family to check. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. The season lists several editions (pass `comp`). |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_asiacup_teams
fiba_asiacup_teams(season=2025)
```

### fiba_bcl_editions {#fiba_bcl_editions}

`fiba_bcl_editions(season: 'Optional[int]' = None, to_season: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Every edition of Basketball Champions League (FIBA Men's European Club Competitions Tier 1).

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

Convenience wrapper: a thin binding of `fiba_gdap_editions` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | First season (inclusive); omitted = all editions. |
| `to_season` | `Optional[int]` | `None` | Last season, inclusive. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `hosts` | String | Host countries with their cities. Stored as a JSON string. |
| `competition_id` | Int64 | FIBA competition id (GDAP key space): one per edition (2023 World Cup = 208182, BCL 2024-25 = 208850); gdapCompetitionId. |
| `competition_code` | String | Competition code (WMM men's World Cup, WOLYM, BCL, ELW...). |
| `season` | Int64 | Season key, three rules: the END year for club competitions (BCL 2024-25 = 2025) and the event year for national-team events (World Cup 2023 = 2023), both equal to the sdv season key; qualifiers carry the TARGET tournament year, not the year played (World Cup 2027 qualifiers played in 2025 = 2027, EuroBasket 2029 pre-qualifiers = 2029), whose sdv mapping is an open Phase B decision. |
| `official_name` | String | Edition title (FIBA Basketball World Cup; Paris 2024 Men's Olympic Basketball Tournament). |
| `start` | String | First day, YYYY-MM-DD. |
| `end` | String | Last day, YYYY-MM-DD. |
| `status` | String | Competition status: In Progress, Completed, Closed. |
| `age_category` | String | Age category (Senior, Under 16...). |
| `gender` | String | Men or Women: the gender of the competition. |
| `fiba_zone` | String | FIBA zone the competition belongs to (World, Europe, Americas...). |
| `zone_information_zone_id` | Int64 | FIBA zone id (1 = World, 23 = Europe; zone table key space). |
| `zone_information_zone_official_name` | String | Official name of the competition's FIBA zone (FIBA Europe, FIBA Americas, World). |
| `zone_information_zone_code` | String | Zone code (W, E, AF, AM, AS, OC). |
| `zone_information_top_zone_id` | Int64 | Parent zone id; null for top zones. |
| `zone_information_top_zone_official_name` | String | Parent zone name; always null in the samples, values unverified. |
| `zone_information_top_zone_code` | String | Parent zone code; always null in the samples, values unverified. |
| `competition_type` | String | National Teams or Clubs. |
| `competition_category_is_league` | Boolean | True for league-format club competitions. |
| `competition_category_age_type` | String | Age type code: S senior, U16. |
| `competition_category_code` | String | Category code (WC World Cup, OLY Olympic Tournament, CCC-1 continental club tier 1). |
| `competition_category_name` | String | Name of the competition's category (FIBA Basketball World Cup, Olympic Basketball Tournament...). |
| `official_flag_item_code` | String | Official-status code: O = official. |
| `official_flag_item_name` | String | Official-status label. |
| `official` | String | Official-status label. |
| `discipline` | String | Discipline (Basketball; 3x3 is a separate platform). |
| `competition_marketing_name_competition_marketing_name_id` | Int64 | Competition family id (201 = World Cup, 112 = BCL tier 1, 211 = EuroLeague Women); the <histId> of /en/history/ pages. |
| `competition_marketing_name_marketing_name` | String | Competition family name; slugified it is the <histSlug> of /en/history/ pages. |
| `competition_marketing_name_code` | String | Family code; null in every sample. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, or GDAP answered {} or [] for an unknown id). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, or a malformed id, season or date. |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_bcl_editions
fiba_bcl_editions(season=2025)
```

### fiba_bcl_pbp {#fiba_bcl_pbp}

`fiba_bcl_pbp(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Play-by-play of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

Convenience wrapper: a thin binding of `fiba_gamepage_pbp` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule). |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Required unless `comp` is given: a game function never guesses the edition. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. Checked to be an edition of this competition (one cached getGdapCompetitionById request). |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `statistic_system` | String | Live-stats system of the game: OVR (World Cup, Olympics) or FLS (club competitions, most qualifiers); they differ in the play-by-play action ids, order and text. |
| `period` | String | Period name: Q1..Q4, then OT1, OT2... |
| `period_number` | Int64 | 1-based position of the period in the game. |
| `action_id` | String | Action id (String): '<n>:<seq>' in OVR games (3:0015), a sequence number in FLS games; unique within the game. |
| `order` | Int64 | Sort key within the period (OVR: 1, 2, ...; FLS: 1000, 2000, ...); null on some period events. |
| `clock` | String | Game clock: time REMAINING in the period, 'm:ss', counting down from 10:00 (5:00 in overtime). |
| `clock_seconds` | Int64 | Seconds remaining in the period, derived from clock. |
| `wall_time_ms` | Int64 | Wall-clock time of the action, Unix epoch milliseconds. |
| `score_a` | Int64 | Team A score after the action. |
| `score_b` | Int64 | Team B score after the action. |
| `action_code` | String | Action code: P2 two-point shot, P3 three-point shot, FT free throw, REB rebound, TREB team rebound, ASS assist, ST steal, BS block, TO turnover, TTO team turnover, FOUL foul, RFOUL foul drawn, SUBST substitution, TIMO timeout, plus period events (STARTG, STARTP, ENDP, ENDG). |
| `action_family` | String | Action family: shot, subst, timeout, periods, and 'unknwown' (FIBA's spelling) for every other action. |
| `description` | String | English action text ('3pt jump shot missed', 'defensive rebound'); null on most OVR actions. |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `player_id` | Int64 | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `player2_id` | Int64 | FIBA person id (Int64) of the second player on a substitution; null when the game does not record it (the refs note FLS only, but the Paris 2024 final, an OVR game, carries it and the FLS games sampled leave it null). |
| `sub_direction` | String | Substitution direction: IN or OUT; null on other actions. |
| `made` | Boolean | True when the shot went in (two-pointers, three-pointers and free throws only; null otherwise). |
| `points_attempted` | Int64 | Points the attempt was worth (2, 3 or 1), made or not; null on other actions. |
| `shot_type` | String | Shot type derived from action_code: 2PT, 3PT or FT; null on other actions. |
| `shot_x` | Int64 | Shot x, integer pixels on a half-court image across the court width (observed 0-278, basket near x = 139). Present only on shots; free throws are 0. Which sideline is x = 0 and the scale are UNVERIFIED. |
| `shot_y` | Int64 | Shot y, integer pixels from the baseline (y = 0) towards midcourt (observed 0-224, basket near y = 25); both teams' shots are drawn on one basket. Free throws are 0. Scale UNVERIFIED. |
| `has_location` | Boolean | True when shot_x / shot_y locate the shot (two- and three-pointers); False for free throws (always 0, 0); null on other actions. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). FIBA lists no edition for the season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). `comp` is an edition of another competition family, or GDAP gives it no family to check. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. Neither `season` nor `comp` is given, or the season lists several editions (pass `comp`). |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_bcl_schedule, fiba_bcl_pbp
games = fiba_bcl_schedule(season=2025)
fiba_bcl_pbp(game_id=int(games['game_id'][0]), season=2025)
```

### fiba_bcl_player_box {#fiba_bcl_player_box}

`fiba_bcl_player_box(game_id: 'int', season: 'Optional[int]' = None, comp: 'Optional[int]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Player box score of one Basketball Champions League (FIBA Men's European Club Competitions Tier 1) game.

Basketball Champions League (FIBA Men's European Club Competitions Tier 1) (men's); season rule: END year (2024-25 = 2025).

Convenience wrapper: a thin binding of `fiba_gamepage_box_players` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: FIBA's terms restrict this source. The GDAP developer-portal terms limit API use to 'Authorized Users' (federations, organisers, FIBA partners) and say any broader use 'is not authorised'; the fiba.basketball terms say no portion of the site may be duplicated, redistributed or manipulated. The package owner reviewed both clauses on 2026-10-12, accepted them and approved the full intake, published data releases included. Users who are not themselves authorised by FIBA should check the terms before using the wrappers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | FIBA game id (game_id in the schedule). |
| `season` | `Optional[int]` | `None` | Edition season key (see the season rule above). Required unless `comp` is given: a game function never guesses the edition. |
| `comp` | `Optional[int]` | `None` | FIBA competition id (one per edition); skips the edition lookup. Needed when a season lists several editions. Checked to be an edition of this competition (one cached getGdapCompetitionById request). |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the game-page `Dict` (`game`, `playByPlay`, `gameDetails`, `playersTeamA`, `playersTeamB`) cut out of the RSC flight payload with its tokens resolved when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | FIBA game id (Int64), global across competitions: gdapGameId, the <gameId> of a game-page URL. |
| `competition_id` | Int64 | FIBA competition id (Int64) of the game's edition; the comp argument. |
| `team_side` | String | A or B: whether the row is the schedule's team A or team B (null when the page does not say). |
| `organisation_id` | Int64 | Stable federation or club id (Int64) of the team, the same in every competition (64 = Germany); the page's T_<id> and the play-by-play oId. NOT the per-competition team_id. |
| `player_id` | Int64 | FIBA person id (Int64), global across competitions and roles: the page's P_<id>, play-by-play pId, roster personId and fiba_gdap_roster.player_id. |
| `first_name` | String | Given name(s) as registered with FIBA. |
| `last_name` | String | Last name as registered with FIBA. |
| `jersey` | String | Jersey number as a string. |
| `position` | String | Playing position code from the lineup card (PG, SG, SF, PF, C). |
| `is_captain` | Boolean | True for the team captain. |
| `is_starter` | Boolean | True for the starting five. |
| `has_played` | Boolean | True when the player got on the court. |
| `minutes` | String | Minutes played as an 'm:ss' string ('0:0' = did not play). |
| `seconds_played` | Int64 | Seconds played, derived from minutes. |
| `points` | Int64 | Points scored in the game. |
| `fg_made` | Int64 | Field goals made (2 + 3 points). |
| `fg_attempted` | Int64 | Field goals attempted (2 + 3 points). |
| `fg_pct` | Float64 | Field-goal percentage, 0-100. |
| `fg2_made` | Int64 | Two-point field goals made. |
| `fg2_attempted` | Int64 | Two-point field goals attempted. |
| `fg2_pct` | Float64 | Two-point field-goal percentage, 0-100. |
| `fg3_made` | Int64 | Three-point field goals made. |
| `fg3_attempted` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point field-goal percentage, 0-100. |
| `ft_made` | Int64 | Free throws made. |
| `ft_attempted` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage, 0-100. |
| `reb_offensive` | Int64 | Offensive rebounds. |
| `reb_defensive` | Int64 | Defensive rebounds. |
| `reb_total` | Int64 | Total rebounds (a team row includes the team rebounds team_reb_total). |
| `assists` | Int64 | Assists credited in the game. |
| `steals` | Int64 | Steals credited in the game. |
| `blocks` | Int64 | Blocked shots (blocks made). |
| `blocks_received` | Int64 | Own shots blocked by the opponent (blocks received). |
| `turnovers` | Int64 | Turnovers (a team row includes the team turnovers team_turnovers). |
| `fouls` | Int64 | Personal fouls (a team row includes the team fouls team_fouls). |
| `fouls_drawn` | Int64 | Fouls drawn in the game. |
| `paint_fg_made` | Int64 | Field goals made in the paint (team points_in_paint = 2 x paint_fg_made on every sampled game). |
| `paint_fg_attempted` | Int64 | Field goals attempted in the paint. |
| `paint_fg_pct` | Float64 | Field-goal percentage in the paint, 0-100. |
| `efficiency` | Int64 | Efficiency (FIBA EFF: points + rebounds + assists + steals + blocks - missed field goals - missed free throws - turnovers). |
| `plus_minus` | Int64 | Plus/minus: the team's point differential while the player was on the court. |
| `cb` | Boolean | Boolean flag, false on every sampled player; meaning unverified. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FIBA has nothing for the request (HTTP 404, GDAP answered {} or [] for an unknown id, a listed game whose teams are not drawn yet, or a game page with no box score). FIBA lists no edition for the season. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed, or the answer is not about the request (an error envelope, HTTP 401 after a key refresh, HTTP 500, a body for another game, competition, team, player, federation, family or season, a game of another competition than comp, the HTTP 200 'not-found shell' of a wrong game page, a live game). `comp` is an edition of another competition family, or GDAP gives it no family to check. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. HTTP 400 / 422, a malformed id, season or date, or a game page requested without comp. Neither `season` nor `comp` is given, or the season lists several editions (pass `comp`). |

**Example**

```python
from sportsdataverse.basketball.fiba_hub import fiba_bcl_schedule, fiba_bcl_player_box
games = fiba_bcl_schedule(season=2025)
fiba_bcl_player_box(game_id=int(games['game_id'][0]), season=2025)
```
