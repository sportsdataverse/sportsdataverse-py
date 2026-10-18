---
title: "SHL — SHL site API (www.shl.se/api, Sweden) — Other: stats_info"
sidebar_label: "Other: stats_info"
sidebar_position: 5
description: "SHL — SHL site API (www.shl.se/api, Sweden) — Other: stats_info — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SHL — SHL site API (www.shl.se/api, Sweden) — Other: stats_info

## shl_stats_info

Stats table of one module for an ssgt (standings, players, goalies, teams, games).

**Endpoint URL:** `GET https://www.shl.se/api/statistics-v2/stats-info/{module}`

**Valid URL:** [https://www.shl.se/api/statistics-v2/stats-info/players_summary?count=1000&season=2026&gameType=regular](https://www.shl.se/api/statistics-v2/stats-info/players_summary?count=1000&season=2026&gameType=regular)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `module` | `module` |  | `Y` |  | Stats module: <group>_<table>. Every name under layout-info [].filters.<group>.modules. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025/26). Mapped to the provider's ssgt through /statistics-v2/layout-info (SHL 1975/76 on); a season it does not list raises NoDataError. |
| `gameType` | `game_type` |  |  | `Y` | Game type: regular (Grundserie), playoff (Slutspel), qualdown (relegation qualification) or playin (2023/24 on). |

### Returns {#shl_stats_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank in the table. |
| `gp` | numeric | Number of games played. |
| `w` | numeric | Wins. Team modules (teams_result): total wins, regulation + overtime + shootout; goalkeeper modules: wins credited to the goalie; standings_standings: regulation wins only. |
| `l` | numeric | Losses. Team modules (teams_result): total losses, regulation + overtime + shootout; goalkeeper modules: losses charged to the goalie; standings_standings: regulation losses only. |
| `leading` | numeric | Times leading: how often the team was ahead (exact definition unverified). |
| `trailing` | numeric | Times trailing. |
| `g` | numeric | Number of goals scored. |
| `ga` | numeric | Number of goals conceded (goals against). |
| `gfg` | numeric | First goal of the game. |
| `otw` | numeric | Overtime wins, shootouts excluded (see SOW). |
| `otl` | numeric | Overtime losses, shootouts excluded (see SOL). |
| `sow` | numeric | Number of shootout wins. |
| `sol` | numeric | Shootout losses. |
| `p_gp_format` | numeric | Points per game (formatted). Numeric value sent as a string. |
| `gpg` | numeric | Goals per game. Numeric value sent as a string. |
| `gapg` | numeric | Goals against per game. Numeric value sent as a string. |
| `points` | numeric | Standings points (3 per regulation win, 2 per OT/SO win, 1 per OT/SO loss). |
| `info_type` | character | Record type of the row's subject: team in team modules, athlete in player and goalie modules. |
| `info_id` | character | Team code (e.g. SAIK) in team and standings modules; null in player, goalie and games modules (use info.uuid). |
| `info_uuid` | character | Athlete uuid (platform key space) in player and goalie modules, team uuid in team modules; joins to the athlete routes or /sports-v2/teams/{team_uuid}. |
| `info_created_at` | character | Record creation time (ISO 8601; often null). |
| `info_created_by` | character | Record creator (site instance id; often null). |
| `info_deleted` | logical | Soft-delete flag of the record (false in every sample). |
| `info_gender` | character | Sex of the athlete or team (male / female). |
| `info_graphics_library` | character | Graphics asset key. |
| `info_last_modified` | character | Record last-modified time (ISO 8601). |
| `info_nationality` | character | Nationality (ISO country code). |
| `info_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `info_ownersite` | character | Owning site code. |
| `info_team_code` | character | Team code of the player's team (e.g. FHC). |
| `info_series` | character | Series with their uuids (SHL, CHL). |
| `info_sport` | character | Sport records the club belongs to (ice hockey). |
| `info_team_media` | character | Media URL or string. |
| `info_team_names_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `info_team_names_short` | character | Short club name ('Herr' = men's team). |
| `info_team_names_long` | character | Long club name ('Herr' = men's team). |
| `info_team_names_full` | character | Full club name ('Herr' = men's team). |
| `info_team_names_code_site` | character | Display abbreviation on the site; 'Herr' = men's team (e.g. 'DIF Herr'). |
| `info_team_names_short_site` | character | Short club name as shown on the site ('Herr' = men's team). |
| `info_team_names_full_site` | character | Full club name as shown on the site ('Herr' = men's team). |
| `info_team_names_long_site` | character | Long club name as shown on the site ('Herr' = men's team). |
| `info_club_name_code` | character | Display abbreviation; differs from the team code for some clubs (SKE vs SAIK, ÖRE vs OHK). |
| `info_club_name_short` | character | Short club name ('Herr' = men's team). |
| `info_club_name_long` | character | Long club name ('Herr' = men's team). |
| `info_club_name_full` | character | Full club name ('Herr' = men's team). |
| `info_team_info_founded` | character | Club founding year (text). Numeric value sent as a string. |
| `info_team_info_golds` | character | Club's championships (text). |
| `info_team_info_finals` | character | Club's final appearances (text). |
| `info_team_info_address` | character | Club postal address. |
| `info_team_info_phone` | character | Telephone number of the club. |
| `info_team_info_fax` | character | Fax number of the club. |
| `info_team_info_email` | character | Contact email address of the club. |
| `info_team_info_chairman` | character | Name of the club chairman. |
| `info_team_info_manager` | character | Club general manager. |
| `info_team_info_accreditation` | character | Press-accreditation contact, free Swedish text (e.g. 'Senast dagen före match till …' = 'no later than the day before the game, to …'). |
| `info_team_info_facebook` | character | Club Facebook URL. |
| `info_team_info_instagram` | character | Club Instagram URL. |
| `info_team_info_twitter` | character | Club X/Twitter URL. |
| `info_team_ext_ids` | character | External ids of the team. |
| `info_ext_ids` | character | External ids of the record. |
| `info_logo` | character | URL of the team logo image. |
| `info_social_media` | character | Club social links. |
| `info_display_name` | character | Team display-name override; unused. Empty string in every sample (placeholder). |
| `info_code` | character | Team code (e.g. SAIK) in team and standings modules; absent for players. |
| `info_site_display_name` | character | Club name as shown on its own site (e.g. Frölunda HC). |
| `info_team_info_retired_numbers` | character | Retired jersey numbers (text). |
| `info_team_info_contact` | character | Club contact text. |
| `info_club_name` | character | Club names by length. |
| `player_id` | integer | isa (StatNet) player id, Int64. Play-by-play sends it as a string, every other route as an integer; it is cast here so the frames join. |
| `svs` | numeric | Number of saves made by the goalie. |
| `svs_perc` | numeric | Save %. Numeric value sent as a string. |
| `gpi` | numeric | Games played with ice time. |
| `gaa` | numeric | Goals against average (per 60 minutes). Numeric value sent as a string. |
| `t` | numeric | Goalie ties (goalkeeper modules only); 0 in every sampled row, as overtime and shootouts now decide every game. |
| `so` | numeric | Number of shutouts (games without a goal against). |
| `mip` | character | Minutes played (mm:ss). Clock/duration as an mm:ss string. |
| `valid_ranking` | logical | Whether the row qualifies for the ranking (minimum games / minutes). |
| `info_full_name` | character | Given name and surname. |
| `info_first_name` | character | Given (first) name of the player. |
| `info_last_name` | character | Surname (family name) of the player. |
| `info_birth_date` | character | Date of birth (YYYY-MM-DD). |
| `info_number` | integer | Jersey (shirt) number of the player. |
| `info_position` | character | Position code (CE = centre, LW, RW, D, GK, F); N/A in the athlete game logs. |
| `info_shoots` | character | Shooting side (L / R; Left / Right). |
| `info_weight_value` | integer | Weight of the player in kilograms. |
| `info_weight_format` | character | Unit of value: kg. |
| `info_height_value` | integer | Height of the player in centimetres. |
| `info_height_format` | character | Unit of value: cm. |
| `info_player_media_id` | integer | Media-library id of the portrait (joins to /api/media; not a player id). |
| `info_player_media_media_string` | character | Media reference (type\|source\|id). |
| `info_player_media_type` | character | Image kind (portrait). |
| `info_player_media_sort_order` | integer | Ordering index for display. |
| `info_team_uuid` | character | Team uuid (platform key space, e.g. 087a-087aTQv9u); joins to /sports-v2/teams/{team_uuid} and schedule homeTeamInfo/awayTeamInfo.uuid. Not the team code. |
| `info_team_name` | character | Team name (e.g. Frölunda HC); empty in some rows. |
| `info_team_owner_instance_id` | character | Club site instance id (e.g. fhc1_fhc); also names the logo at sportality.cdn.s8y.se/team-logos/{id}.svg. |
| `info_team_site_display_name` | character | Club name as shown on its own site (e.g. Frölunda HC). |
| `info_team_id` | character | Team code (e.g. FHC) in gameday and standings payloads; not the team uuid. |
| `tp` | numeric | Points (goals + assists). |
| `a` | numeric | Number of assists credited. |
| `pim` | numeric | Penalty minutes. |
| `gwg` | numeric | Game-winning goals. |
| `ppg` | numeric | Power-play goals. |
| `sog` | numeric | Number of shots on goal. |
| `hits` | numeric | Body checks delivered. |
| `bk_s` | numeric | Number of blocked shots. |
| `plus` | numeric | Goals for while on ice. |
| `minus` | numeric | Goals against while on ice. |
| `plus_minus` | numeric | Goals for minus goals against while on ice (+/-). |
| `toi_gp` | character | Time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `tppg` | numeric | Points per game. Numeric value sent as a string. |
| `a_firsts` | numeric | Primary assists. |
| `a_seconds` | numeric | Secondary assists. |
| `ppa` | numeric | Power-play assists. |
| `pptp` | numeric | Power-play points. |
| `eqg` | numeric | Even-strength goals. |
| `eqa` | numeric | Even-strength assists. |
| `eqtp` | numeric | Even-strength points. |
| `shg` | numeric | Shorthanded goals. |
| `sha` | numeric | Shorthanded assists. |
| `shtp` | numeric | Shorthanded points. |
| `trailing2` | numeric | Games trailing after period 2 (unverified). |
| `trailing1` | numeric | Games trailing after period 1 (unverified). |
| `tiedg` | numeric | Times tied (unverified). |
| `leading1` | numeric | Games leading after period 1 (unverified). |
| `leading2` | numeric | Games leading after period 2 (unverified). |
| `eng` | numeric | Empty-net goals. |
| `toi` | character | Time on ice (mm:ss). Duration as an hh:mm:ss string. |
| `pptoi_gp` | character | Power-play time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `shtoi_gp` | character | Shorthanded time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `pptoi_perc` | numeric | Share of time on ice on the power play (unverified). Numeric value sent as a string. |
| `shtoi_perc` | numeric | Share of time on ice shorthanded (unverified). Numeric value sent as a string. |
| `pptoi_pp_plus` | character | Power-play time on ice per power-play goal for (unverified). Clock/duration as an mm:ss string. |
| `ppsog` | numeric | Power-play shots on goal. |
| `ppsw` | numeric | Power-play shots wide. |
| `ppsg_perc` | numeric | Power-play shooting % (unverified). Numeric value sent as a string. |
| `pp_plus` | numeric | Goals for while on ice on the power play. |
| `pp_minus` | numeric | Goals against while on ice on the power play. |
| `pp_plus_minus` | numeric | Power-play plus/minus. |
| `shsog` | numeric | Shorthanded shots on goal. |
| `shsw` | numeric | Shorthanded shots wide. |
| `sh_plus` | numeric | Goals for while on ice shorthanded. |
| `sh_minus` | numeric | Goals against while on ice shorthanded. |
| `sh_plus_minus` | numeric | Shorthanded plus/minus. |
| `shtoi_sh_minus` | character | Shorthanded time on ice per shorthanded goal against (unverified). Clock/duration as an mm:ss string. |
| `esg` | numeric | Even-strength goals. |
| `esa` | numeric | Even-strength assists. |
| `estp` | numeric | Even-strength points. |
| `essog` | numeric | Even-strength shots on goal. |
| `essw` | numeric | Even-strength shots wide. |
| `es_plus` | numeric | Even-strength goals for while on ice. |
| `es_minus` | numeric | Even-strength goals against while on ice. |
| `es_plus_minus` | numeric | Even-strength plus/minus. |
| `estoi_gp` | character | Even-strength time on ice per game (mm:ss). Clock/duration as an mm:ss string. |
| `eff_pim` | numeric | Effective penalty minutes (served). |
| `pers_pim` | numeric | Personal penalty minutes. |
| `off_pim` | numeric | Offensive penalty minutes. |
| `minor` | numeric | Minor penalties. |
| `major` | numeric | Major penalties. |
| `misc` | numeric | Misconduct penalties. |
| `gm` | numeric | Game misconducts. |
| `mp` | numeric | Match penalties. |
| `fo_perc` | numeric | Face-off win %. Numeric value sent as a string. |
| `fo` | numeric | Face-offs taken. |
| `fow` | numeric | Number of face-offs won. |
| `fol` | numeric | Face-offs lost. |
| `ofo_perc` | numeric | Offensive-zone face-offs won %. Numeric value sent as a string. |
| `ofo` | numeric | Offensive-zone face-offs. |
| `ofow` | numeric | Offensive-zone face-offs won. |
| `ofol` | numeric | Offensive-zone face-offs lost. |
| `dfo_perc` | numeric | Defensive-zone face-offs won %. Numeric value sent as a string. |
| `dfo` | numeric | Defensive-zone face-offs. |
| `dfow` | numeric | Defensive-zone face-offs won. |
| `dfol` | numeric | Defensive-zone face-offs lost. |
| `fo_violation_perc` | numeric | Face-off violation %. Numeric value sent as a string. |
| `fo_violations` | numeric | Face-off violations. |
| `sg_perc` | numeric | Shooting %. Numeric value sent as a string. |
| `ms` | numeric | Missed shots (wide + blocked). |
| `sog_perc` | numeric | Shots on goal % of attempts. Numeric value sent as a string. |
| `ps` | numeric | Number of penalty shots. |
| `psg` | numeric | Penalty-shot goals. |
| `ps_perc` | numeric | Penalty-shot conversion %. Numeric value sent as a string. |
| `so_ps` | numeric | Shootout shots. |
| `so_psg` | numeric | Shootout goals. |
| `so_ps_perc` | numeric | Shootout shooting %. Numeric value sent as a string. |
| `cf` | numeric | Corsi for (shot attempts for). |
| `ca` | numeric | Corsi against (shot attempts against). |
| `cf_perc` | numeric | Corsi for percentage. Numeric value sent as a string. |
| `close_cf_perc` | numeric | Corsi for percentage in close-score situations. Numeric, but sent as a string in some rows and as a number in others. |
| `ff` | numeric | Fenwick for (unblocked shot attempts for). |
| `fa` | numeric | Fenwick against (unblocked shot attempts against). |
| `ff_perc` | numeric | Fenwick for percentage. Numeric, but sent as a string in some rows and as a number in others. |
| `close_ff_perc` | numeric | Fenwick for percentage in close-score situations. Numeric, but sent as a string in some rows and as a number in others. |
| `on_ice_goal_perc` | numeric | On-ice shooting % (unverified). |
| `on_ice_svs_perc` | numeric | On-ice save % (unverified). Numeric, but sent as a string in some rows and as a number in others. |
| `pdo` | numeric | PDO (on-ice shooting % + on-ice save %). Numeric, but sent as a string in some rows and as a number in others. |
| `info_player_media` | character | Portrait image reference (mediaString, type). |
| `shga` | numeric | Shorthanded goals against (conceded while on the power play). |
| `ppga` | numeric | Power-play goals against (conceded while shorthanded). |
| `enga` | numeric | Empty-net goals against. |
| `pp_opp` | numeric | Power-play opportunities. |
| `pp_perc` | numeric | Power-play %. Numeric value sent as a string. |
| `pp_time` | character | Time on the power play (hh:mm:ss). Duration as an hh:mm:ss string. |
| `st_perc` | numeric | Special-teams % (power-play % + penalty-kill %). Numeric value sent as a string. |
| `pk_opp` | numeric | Times shorthanded. |
| `pk_perc` | numeric | Penalty-kill %. Numeric value sent as a string. |
| `ppsoga` | numeric | Power-play shots on goal against. |
| `pk_time` | character | Time shorthanded (hh:mm:ss). Duration as an hh:mm:ss string. |
| `gp_home` | numeric | Home games played. |
| `att_avg_home` | numeric | Average home attendance per game. |
| `att_total_home` | numeric | Total home attendance. |
| `att_avg_overall` | numeric | Average attendance per game. |
| `att_total_overall` | numeric | Total attendance. |
| `gp_road` | numeric | Away games played. |
| `att_avg_road` | numeric | Average away attendance per game. |
| `att_total_road` | numeric | Total away attendance. |
| `team_pen` | numeric | Team (bench) penalties. |
| `s_eff` | numeric | Shooting %. Numeric value sent as a string. |
| `pps_eff` | numeric | Power-play shooting %. Numeric value sent as a string. |
| `sw` | numeric | Number of shots that missed the net. |
| `sib` | numeric | Shots blocked (blocked attempts). |
| `ss` | numeric | Scoring-chance shots (site label; unverified). |
| `soga` | numeric | Shots on goal against. |
| `msa` | numeric | Missed shots against (wide + blocked). |
| `swa` | numeric | Shots wide against. |
| `sfa` | numeric | Shots on goal against (unverified). |
| `ssa` | numeric | Scoring-chance shots against (site label; unverified). |
| `fo_violations_perc` | numeric | Face-off violation %. |
| `psa` | numeric | Penalty-shot attempts. |
| `pssvs` | numeric | Penalty shots saved. |
| `so_psa` | numeric | Shootout attempts against. |
| `so_pssvs` | numeric | Shootout saves. |
| `so_pssvs_perc` | numeric | Shootout save %. Numeric value sent as a string. |
| `freezes` | numeric | Puck freezes (stoppages caused). |
| `freezes_against` | numeric | Puck freezes by the opponent. |
| `icings` | numeric | Icing calls against the team. |
| `icing_against` | numeric | Icings by the opponent. |
| `offsides` | numeric | Offside calls against the team. |
| `offsides_against` | numeric | Offsides by the opponent. |
| `ssga` | numeric | Scoring-chance goals against (unverified). |
| `ssoga` | numeric | Scoring-chance shots on goal against (unverified). |
| `sssvs_perc` | numeric | Save % on scoring-chance shots. Numeric value sent as a string. |
| `ppsvs_perc` | numeric | Save % against the power play (unverified). Numeric value sent as a string. |
| `shsoga` | numeric | Shorthanded shots on goal against. |
| `shsvs_perc` | numeric | Shorthanded save % (unverified). Numeric value sent as a string. |
| `esga` | numeric | Even-strength goals against. |
| `essoga` | numeric | Even-strength shots on goal against. |
| `essvs_perc` | numeric | Even-strength save %. Numeric value sent as a string. |
| `psga` | numeric | Penalty-shot goals against. |
| `pssvs_perc` | numeric | Penalty-shot save %. |
| `so_psga` | numeric | Shootout goals against. |
| `date_time` | character | Game date and time. |
| `home_team` | character | Home team code (e.g. BIF). |
| `guest_team` | character | Away team code (e.g. HV71). |
| `game_score_home_score` | integer | Goals scored by the home team. |
| `game_score_away_score` | integer | Goals scored by the away team. |
| `game_score_decided_in` | character | Filter values for how a game was decided. |
| `venue` | character | Name of the arena the game is played in. |
| `attendance` | numeric | Spectators at the game. |
| `info_game_id` | integer | StatNet game number (StatNet key space, e.g. 22003); keys stats-info games{} and equals sports-v2 game-info extId. Not the game uuid. |
| `info_home_team_id` | character | Home team code (e.g. FHC); not a uuid. |
| `info_away_team_id` | character | Away team code (e.g. LHC); not a uuid. |
| `diff` | numeric | Goal difference. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#shl_stats_info-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | SHL answered 404, an empty body (gameday routes before 2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the season and game type are not in layout-info. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#shl_stats_info-example}

```python
shl_stats_info(module='players_summary', season=2026)
```

_Last validated n/a._
