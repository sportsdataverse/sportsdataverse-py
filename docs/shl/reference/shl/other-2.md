# SHL — SHL site API (www.shl.se/api, Sweden) — Other: standings–stats_info

> SHL — SHL site API (www.shl.se/api, Sweden) — Other: standings–stats_info — function reference in sdv-py, the SportsDataverse Python package.

## shl_standings

Standings of an ssgt (gameday widget).

**Endpoint URL:** `GET https://www.shl.se/api/gameday/team-standings/{ssgt_uuid}`

**Valid URL:** [https://www.shl.se/api/gameday/team-standings/iuzqg7dqk9](https://www.shl.se/api/gameday/team-standings/iuzqg7dqk9)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ssgt_uuid` | `ssgt_uuid` |  | `Y` |  | ssgt uuid: one season + series + game type (e.g. 2025/2026 SHL regular season). Directory: /statistics-v2/layout-info seasonSeriesGameTypeTeams[].ssgtUuid. |

### Returns {#shl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025/26). |
| `ssgt_uuid` | String | ssgt uuid (platform key space: one season + series + game type); label via layout-info seasonSeriesGameTypeTeams[]. |
| `team_code` | String | StatNet team code (SAIK, FHC). |
| `team_uuid` | String | Platform team uuid. |
| `team_name` | String | Name of the team. |
| `team_short_name` | String | Short display name of the team. |
| `rank` | Int64 | Position in the table (1 = top). |
| `rank_diff` | Int64 | Change in standings rank, in places (the reference point is unverified). |
| `gp` | Int64 | Number of games played. |
| `w` | Int64 | Regulation wins. |
| `t` | Int64 | Games tied after regulation, decided in overtime or a shootout (= OTW + OTL). |
| `l` | Int64 | Regulation losses. |
| `otw` | Int64 | Overtime and shootout wins. |
| `otl` | Int64 | Overtime and shootout losses. |
| `p` | Int64 | Standings points (3 per regulation win, 2 per OT/SO win, 1 per OT/SO loss). |
| `gf` | Int64 | Number of goals scored (goals for). |
| `ga` | Int64 | Number of goals conceded (goals against). |
| `diff` | Int64 | Goal difference. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_standings-example}

```python
shl_standings(ssgt_uuid='iuzqg7dqk9')
```

## shl_stats_info

Stats table of one module for an ssgt (standings, players, goalies, teams, games).

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/stats-info/{module}`

**Valid URL:** [https://www.shl.se/api/statistics-v2/stats-info/players_summary?count=1000&season=2026&gameType=regular](https://www.shl.se/api/statistics-v2/stats-info/players_summary?count=1000&season=2026&gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `module` | `module` |  | `Y` |  | Stats module: <group>_ . Every name under layout-info [].filters.<group>.modules. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025/26). Mapped to the provider's ssgt through /statistics-v2/layout-info (SHL 1975/76 on); a season it does not list raises NoDataError. |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (Grundserie), playoff (Slutspel), qualdown (relegation qualification) or playin (2023/24 on). |

### Returns {#shl_stats_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank in the table. |
| `gp` | Float64 | Number of games played. |
| `w` | Float64 | Wins. Team modules (teams_result): total wins, regulation + overtime + shootout; goalkeeper modules: wins credited to the goalie; standings_standings: regulation wins only. |
| `l` | Float64 | Losses. Team modules (teams_result): total losses, regulation + overtime + shootout; goalkeeper modules: losses charged to the goalie; standings_standings: regulation losses only. |
| `leading` | Float64 | Times leading: how often the team was ahead (exact definition unverified). |
| `trailing` | Float64 | Times trailing. |
| `g` | Float64 | Number of goals scored. |
| `ga` | Float64 | Number of goals conceded (goals against). |
| `gfg` | Float64 | First goal of the game. |
| `otw` | Float64 | Overtime wins, shootouts excluded (see SOW). |
| `otl` | Float64 | Overtime losses, shootouts excluded (see SOL). |
| `sow` | Float64 | Number of shootout wins. |
| `sol` | Float64 | Shootout losses. |
| `p_gp_format` | Float64 | Points per game (formatted). Numeric value sent as a string. |
| `gpg` | Float64 | Goals per game. Numeric value sent as a string. |
| `gapg` | Float64 | Goals against per game. Numeric value sent as a string. |
| `points` | Float64 | Standings points (3 per regulation win, 2 per OT/SO win, 1 per OT/SO loss). |
| `info_type` | String | Record type of the row's subject: team in team modules, athlete in player and goalie modules. |
| `info_id` | String | Team code (e.g. SAIK) in team and standings modules; null in player, goalie and games modules (use info.uuid). |
| `info_uuid` | String | Athlete uuid (platform key space) in player and goalie modules, team uuid in team modules; joins to the athlete routes or /sports-v2/teams/{team_uuid}. |
| `info_created_at` | String | Record creation time (ISO 8601; often null). |
| `info_created_by` | String | Record creator (site instance id; often null). |
| `info_deleted` | Boolean | Soft-delete flag of the record (false in every sample). |
| `info_gender` | String | Sex of the athlete or team (male / female). |
| `info_graphics_library` | String | Graphics asset key. |
| `info_last_modified` | String | Record last-modified time (ISO 8601). |
| `info_nationality` | String | Nationality (ISO country code). |
| `info_owner_instance_id` | String | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `info_ownersite` | String | Owning site code. |
| `info_team_code` | String | Team code of the player's team (e.g. FHC). |
| `info_series` | String | Series with their uuids (SHL, CHL). |
| `info_sport` | String | Sport records the club belongs to (ice hockey). |
| `info_team_media` | String | Media URL or string. |
| `info_team_names_code` | String | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `info_team_names_short` | String | Short club name ('Herr' = men's team). |
| `info_team_names_long` | String | Long club name ('Herr' = men's team). |
| `info_team_names_full` | String | Full club name ('Herr' = men's team). |
| `info_team_names_code_site` | String | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `info_team_names_short_site` | String | Short club name as shown on the site ('Herr' = men's team). |
| `info_team_names_full_site` | String | Full club name as shown on the site ('Herr' = men's team). |
| `info_team_names_long_site` | String | Long club name as shown on the site ('Herr' = men's team). |
| `info_club_name_code` | String | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `info_club_name_short` | String | Short club name ('Herr' = men's team). |
| `info_club_name_long` | String | Long club name ('Herr' = men's team). |
| `info_club_name_full` | String | Full club name ('Herr' = men's team). |
| `info_team_info_founded` | String | Club founding year (text). Numeric value sent as a string. |
| `info_team_info_golds` | String | Club's championships (text). |
| `info_team_info_finals` | String | Club's final appearances (text). |
| `info_team_info_address` | String | Club postal address. |
| `info_team_info_phone` | String | Telephone number of the club. |
| `info_team_info_fax` | String | Fax number of the club. |
| `info_team_info_email` | String | Contact email address of the club. |
| `info_team_info_chairman` | String | Name of the club chairman. |
| `info_team_info_manager` | String | Club general manager. |
| `info_team_info_accreditation` | String | Press-accreditation contact, free Swedish text (e.g. 'Senast dagen före match till …' = 'no later than the day before the game, to …'). |
| `info_team_info_facebook` | String | Club Facebook URL. |
| `info_team_info_instagram` | String | Club Instagram URL. |
| `info_team_info_twitter` | String | Club X/Twitter URL. |
| `info_team_ext_ids` | String | External ids of the team. |
| `info_ext_ids` | String | External ids of the record. |
| `info_logo` | String | URL of the team logo image. |
| `info_social_media` | String | Club social links. |
| `info_display_name` | String | Team display-name override; unused. Empty string in every sample (placeholder). |
| `info_code` | String | Team code (e.g. SAIK) in team and standings modules; absent for players. |
| `info_site_display_name` | String | Club name as shown on its own site (e.g. Frölunda HC). |
| `info_team_info_retired_numbers` | String | Retired jersey numbers (text). |
| `info_team_info_contact` | String | Club contact text. |
| `info_club_name` | String | Club names by length. |
| `player_id` | Int64 | isa (StatNet) player id, Int64. Play-by-play sends it as a string, every other route as an integer; it is cast here so the frames join. |
| `svs` | Float64 | Number of saves made by the goalie. |
| `svs_perc` | Float64 | Save %. Numeric value sent as a string. |
| `gpi` | Float64 | Games played with ice time. |
| `gaa` | Float64 | Goals against average (per 60 minutes). Numeric value sent as a string. |
| `t` | Float64 | Goalie ties (goalkeeper modules only); 0 in every sampled row, as overtime and shootouts now decide every game. |
| `so` | Float64 | Number of shutouts (games without a goal against). |
| `mip` | String | Minutes played (mm:ss). Clock/duration as an mm:ss string. |
| `valid_ranking` | Boolean | Whether the row qualifies for the ranking (minimum games / minutes). |
| `info_full_name` | String | Given name and surname. |
| `info_first_name` | String | Given (first) name of the player. |
| `info_last_name` | String | Surname (family name) of the player. |
| `info_birth_date` | String | Date of birth (YYYY-MM-DD). |
| `info_number` | Int64 | Jersey (shirt) number of the player. |
| `info_position` | String | Position code (CE = centre, LW, RW, D, GK, F); N/A in the athlete game logs. |
| `info_shoots` | String | Shooting side (L / R; Left / Right). |
| `info_weight_value` | Int64 | Weight of the player in kilograms. |
| `info_weight_format` | String | Unit of value: kg. |
| `info_height_value` | Int64 | Height of the player in centimetres. |
| `info_height_format` | String | Unit of value: cm. |
| `info_player_media_id` | Int64 | Media-library id of the portrait (joins to /api/media; not a player id). |
| `info_player_media_media_string` | String | Media reference (type\|source\|id). |
| `info_player_media_type` | String | Image kind (portrait). |
| `info_player_media_sort_order` | Int64 | Ordering index for display. |
| `info_team_uuid` | String | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `info_team_name` | String | Team name (e.g. Frölunda HC); empty in some rows. |
| `info_team_owner_instance_id` | String | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `info_team_site_display_name` | String | Club name as shown on its own site (e.g. Frölunda HC). |
| `info_team_id` | String | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `tp` | Float64 | Points (goals + assists). |
| `a` | Float64 | Number of assists credited. |
| `pim` | Float64 | Penalty minutes. |
| `gwg` | Float64 | Game-winning goals. |
| `ppg` | Float64 | Power-play goals. |
| `sog` | Float64 | Number of shots on goal. |
| `hits` | Float64 | Body checks delivered. |
| `bk_s` | Float64 | Number of blocked shots. |
| `plus` | Float64 | Goals for while on ice. |
| `minus` | Float64 | Goals against while on ice. |
| `plus_minus` | Float64 | Goals for minus goals against while on ice (+/-). |
| `toi_gp` | String | Time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `tppg` | Float64 | Points per game. Numeric value sent as a string. |
| `a_firsts` | Float64 | Primary assists. |
| `a_seconds` | Float64 | Secondary assists. |
| `ppa` | Float64 | Power-play assists. |
| `pptp` | Float64 | Power-play points. |
| `eqg` | Float64 | Even-strength goals. |
| `eqa` | Float64 | Even-strength assists. |
| `eqtp` | Float64 | Even-strength points. |
| `shg` | Float64 | Shorthanded goals. |
| `sha` | Float64 | Shorthanded assists. |
| `shtp` | Float64 | Shorthanded points. |
| `trailing2` | Float64 | Games trailing after period 2 (unverified). |
| `trailing1` | Float64 | Games trailing after period 1 (unverified). |
| `tiedg` | Float64 | Times tied (unverified). |
| `leading1` | Float64 | Games leading after period 1 (unverified). |
| `leading2` | Float64 | Games leading after period 2 (unverified). |
| `eng` | Float64 | Empty-net goals. |
| `toi` | String | Time on ice (mm:ss). Duration as an hh:mm:ss string. |
| `pptoi_gp` | String | Power-play time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `shtoi_gp` | String | Shorthanded time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `pptoi_perc` | Float64 | Share of time on ice on the power play (unverified). Numeric value sent as a string. |
| `shtoi_perc` | Float64 | Share of time on ice shorthanded (unverified). Numeric value sent as a string. |
| `pptoi_pp_plus` | String | Power-play time on ice per power-play goal for (unverified). Clock/duration as an mm:ss string. |
| `ppsog` | Float64 | Power-play shots on goal. |
| `ppsw` | Float64 | Power-play shots wide. |
| `ppsg_perc` | Float64 | Power-play shooting % (unverified). Numeric value sent as a string. |
| `pp_plus` | Float64 | Goals for while on ice on the power play. |
| `pp_minus` | Float64 | Goals against while on ice on the power play. |
| `pp_plus_minus` | Float64 | Power-play plus/minus. |
| `shsog` | Float64 | Shorthanded shots on goal. |
| `shsw` | Float64 | Shorthanded shots wide. |
| `sh_plus` | Float64 | Goals for while on ice shorthanded. |
| `sh_minus` | Float64 | Goals against while on ice shorthanded. |
| `sh_plus_minus` | Float64 | Shorthanded plus/minus. |
| `shtoi_sh_minus` | String | Shorthanded time on ice per shorthanded goal against (unverified). Clock/duration as an mm:ss string. |
| `esg` | Float64 | Even-strength goals. |
| `esa` | Float64 | Even-strength assists. |
| `estp` | Float64 | Even-strength points. |
| `essog` | Float64 | Even-strength shots on goal. |
| `essw` | Float64 | Even-strength shots wide. |
| `es_plus` | Float64 | Even-strength goals for while on ice. |
| `es_minus` | Float64 | Even-strength goals against while on ice. |
| `es_plus_minus` | Float64 | Even-strength plus/minus. |
| `estoi_gp` | String | Even-strength time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `eff_pim` | Float64 | Effective penalty minutes (served). |
| `pers_pim` | Float64 | Personal penalty minutes. |
| `off_pim` | Float64 | Offensive penalty minutes. |
| `minor` | Float64 | Minor penalties. |
| `major` | Float64 | Major penalties. |
| `misc` | Float64 | Misconduct penalties. |
| `gm` | Float64 | Game misconducts. |
| `mp` | Float64 | Match penalties. |
| `fo_perc` | Float64 | Face-off win %. Numeric value sent as a string. |
| `fo` | Float64 | Face-offs taken. |
| `fow` | Float64 | Number of face-offs won. |
| `fol` | Float64 | Face-offs lost. |
| `ofo_perc` | Float64 | Offensive-zone face-offs won %. Numeric value sent as a string. |
| `ofo` | Float64 | Offensive-zone face-offs. |
| `ofow` | Float64 | Offensive-zone face-offs won. |
| `ofol` | Float64 | Offensive-zone face-offs lost. |
| `dfo_perc` | Float64 | Defensive-zone face-offs won %. Numeric value sent as a string. |
| `dfo` | Float64 | Defensive-zone face-offs. |
| `dfow` | Float64 | Defensive-zone face-offs won. |
| `dfol` | Float64 | Defensive-zone face-offs lost. |
| `fo_violation_perc` | Float64 | Face-off violation %. Numeric value sent as a string. |
| `fo_violations` | Float64 | Face-off violations. |
| `sg_perc` | Float64 | Shooting %. Numeric value sent as a string. |
| `ms` | Float64 | Missed shots (wide + blocked). |
| `sog_perc` | Float64 | Shots on goal % of attempts. Numeric value sent as a string. |
| `ps` | Float64 | Number of penalty shots. |
| `psg` | Float64 | Penalty-shot goals. |
| `ps_perc` | Float64 | Penalty-shot conversion %. Numeric value sent as a string. |
| `so_ps` | Float64 | Shootout shots. |
| `so_psg` | Float64 | Shootout goals. |
| `so_ps_perc` | Float64 | Shootout shooting %. Numeric value sent as a string. |
| `cf` | Float64 | Corsi for (shot attempts for). |
| `ca` | Float64 | Corsi against (shot attempts against). |
| `cf_perc` | Float64 | Corsi for percentage. Numeric value sent as a string. |
| `close_cf_perc` | Float64 | Corsi for percentage in close-score situations. Numeric, but sent as a string in some rows and as a number in others. |
| `ff` | Float64 | Fenwick for (unblocked shot attempts for). |
| `fa` | Float64 | Fenwick against (unblocked shot attempts against). |
| `ff_perc` | Float64 | Fenwick for percentage. Numeric, but sent as a string in some rows and as a number in others. |
| `close_ff_perc` | Float64 | Fenwick for percentage in close-score situations. Numeric, but sent as a string in some rows and as a number in others. |
| `on_ice_goal_perc` | Float64 | On-ice shooting % (unverified). |
| `on_ice_svs_perc` | Float64 | On-ice save % (unverified). Numeric, but sent as a string in some rows and as a number in others. |
| `pdo` | Float64 | PDO (on-ice shooting % + on-ice save %). Numeric, but sent as a string in some rows and as a number in others. |
| `info_player_media` | String | Portrait image reference (mediaString, type). |
| `shga` | Float64 | Shorthanded goals against (conceded while on the power play). |
| `ppga` | Float64 | Power-play goals against (conceded while shorthanded). |
| `enga` | Float64 | Empty-net goals against. |
| `pp_opp` | Float64 | Power-play opportunities. |
| `pp_perc` | Float64 | Power-play %. Numeric value sent as a string. |
| `pp_time` | String | Time on the power play (hh:mm:ss). Duration as an hh:mm:ss string. |
| `st_perc` | Float64 | Special-teams % (power-play % + penalty-kill %). Numeric value sent as a string. |
| `pk_opp` | Float64 | Times shorthanded. |
| `pk_perc` | Float64 | Penalty-kill %. Numeric value sent as a string. |
| `ppsoga` | Float64 | Power-play shots on goal against. |
| `pk_time` | String | Time shorthanded (hh:mm:ss). Duration as an hh:mm:ss string. |
| `gp_home` | Float64 | Home games played. |
| `att_avg_home` | Float64 | Average home attendance per game. |
| `att_total_home` | Float64 | Total home attendance. |
| `att_avg_overall` | Float64 | Average attendance per game. |
| `att_total_overall` | Float64 | Total attendance. |
| `gp_road` | Float64 | Away games played. |
| `att_avg_road` | Float64 | Average away attendance per game. |
| `att_total_road` | Float64 | Total away attendance. |
| `team_pen` | Float64 | Team (bench) penalties. |
| `s_eff` | Float64 | Shooting %. Numeric value sent as a string. |
| `pps_eff` | Float64 | Power-play shooting %. Numeric value sent as a string. |
| `sw` | Float64 | Number of shots that missed the net. |
| `sib` | Float64 | Shots blocked (blocked attempts). |
| `ss` | Float64 | Scoring-chance shots (site label; unverified). |
| `soga` | Float64 | Shots on goal against. |
| `msa` | Float64 | Missed shots against (wide + blocked). |
| `swa` | Float64 | Shots wide against. |
| `sfa` | Float64 | Shots on goal against (unverified). |
| `ssa` | Float64 | Scoring-chance shots against (site label; unverified). |
| `fo_violations_perc` | Float64 | Face-off violation %. |
| `psa` | Float64 | Penalty-shot attempts. |
| `pssvs` | Float64 | Penalty shots saved. |
| `so_psa` | Float64 | Shootout attempts against. |
| `so_pssvs` | Float64 | Shootout saves. |
| `so_pssvs_perc` | Float64 | Shootout save %. Numeric value sent as a string. |
| `freezes` | Float64 | Puck freezes (stoppages caused). |
| `freezes_against` | Float64 | Puck freezes by the opponent. |
| `icings` | Float64 | Icing calls against the team. |
| `icing_against` | Float64 | Icings by the opponent. |
| `offsides` | Float64 | Offside calls against the team. |
| `offsides_against` | Float64 | Offsides by the opponent. |
| `ssga` | Float64 | Scoring-chance goals against (unverified). |
| `ssoga` | Float64 | Scoring-chance shots on goal against (unverified). |
| `sssvs_perc` | Float64 | Save % on scoring-chance shots. Numeric value sent as a string. |
| `ppsvs_perc` | Float64 | Save % against the power play (unverified). Numeric value sent as a string. |
| `shsoga` | Float64 | Shorthanded shots on goal against. |
| `shsvs_perc` | Float64 | Shorthanded save % (unverified). Numeric value sent as a string. |
| `esga` | Float64 | Even-strength goals against. |
| `essoga` | Float64 | Even-strength shots on goal against. |
| `essvs_perc` | Float64 | Even-strength save %. Numeric value sent as a string. |
| `psga` | Float64 | Penalty-shot goals against. |
| `pssvs_perc` | Float64 | Penalty-shot save %. |
| `so_psga` | Float64 | Shootout goals against. |
| `date_time` | String | Game date and time. |
| `home_team` | String | Home team code (e.g. BIF). |
| `guest_team` | String | Away team code (e.g. HV71). |
| `game_score_home_score` | Int64 | Goals scored by the home team. |
| `game_score_away_score` | Int64 | Goals scored by the away team. |
| `game_score_decided_in` | String | Filter values for how a game was decided. |
| `venue` | String | Name of the arena the game is played in. |
| `attendance` | Float64 | Spectators at the game. |
| `info_game_id` | Int64 | StatNet game number (StatNet key space, e.g. 22003); keys stats-info games{} and equals sports-v2 game-info extId. Not the game uuid. |
| `info_home_team_id` | String | Home team code (e.g. FHC); not a uuid. |
| `info_away_team_id` | String | Away team code (e.g. LHC); not a uuid. |
| `diff` | Float64 | Goal difference. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_stats_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_stats_info-example}

```python
shl_stats_info(module='players_summary', season=2026)
```
