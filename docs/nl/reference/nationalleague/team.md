# NL — National League app API (www.nationalleague.ch/api, Switzerland) — Team

> NL — National League app API (www.nationalleague.ch/api, Switzerland) — Team — function reference in sdv-py, the SportsDataverse Python package.

## nl_team_schedule

One team's games for one season.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/team/{team_id}`

**Valid URL:** [https://www.nationalleague.ch/api/games/team/101144?season=2026](https://www.nationalleague.ch/api/games/team/101144?season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | SIHF team id (team_id in nl_standings; the same id as the sihf family's team_id), e.g. 101144 = EV Zug. |
| `season` | `season` |  |  | `Y` | Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |

### Returns {#nl_team_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `home_team_short_name` | character | Home team abbreviation (e.g. HCD) |
| `home_team_name` | character | Home team display name |
| `away_team_short_name` | character | Away team abbreviation |
| `away_team_name` | character | Away team display name |
| `home_team_id` | integer | Home team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `away_team_id` | integer | Away team. SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `vimeo_video_id` | integer | Full-game video. Vimeo video id (Int64): vimeo.com/<id>. |
| `date` | character | Scheduled start, ISO 8601 with UTC offset |
| `status` | character | Game state: finished, canceled, beforeStartOfPlay (others appear live) |
| `base_status` | character | Coarse game state; equal to status in every sampled game |
| `home_team_result` | integer | Home team goals (final or current score; 0 before the game) |
| `away_team_result` | integer | Away team goals (final or current score; 0 before the game) |
| `is_overtime` | logical | Game went to overtime |
| `is_shootout` | logical | Game went to a shootout |
| `highlight_vimeo_video_id` | integer | Highlights video. Vimeo video id (Int64): vimeo.com/<id>. |
| `has_highlight` | logical | A highlight video exists |
| `is_current` | logical | Game belongs to the current game day |
| `is_current_for_team` | logical | Game is the team's current game (team-scoped views) |
| `tv_partner` | character | Broadcaster logo image URLs; unobserved: null, absent or empty in every sampled row |
| `spectators` | integer | Attendance (Int64; the API sends a digit string). |
| `has_my_sports` | logical | Broadcast on MySports |
| `referees` | character | Officials as '#<number> <name>' strings (referees and linesmen; one entry in some games) |
| `is_exhibition` | logical | Exhibition game, not NL competition (competition code other than 1105) |
| `home_team_coach` | character | Home head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `away_team_coach` | character | Away head coach name; filled in a game's `overview`, null in the season lists; unobserved: null, absent or empty in every sampled row |
| `game_time` | integer | Elapsed game minutes; 0 on every game in the season lists, 60 in a finished game's overview |
| `show_game_time` | logical | App flag: show gameTime |
| `arena` | character | Name of the arena the game is played in |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_team_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_team_schedule-example}

```python
nl_team_schedule(season=2026, team_id=101144)
```

_Last validated n/a._

## nl_team_stats

One game's team totals: shots by period, faceoffs by zone, power play and penalty kill.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/{game_id}`

**Valid URL:** [https://www.nationalleague.ch/api/games/20261105000325](https://www.nationalleague.ch/api/games/20261105000325)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | SIHF game id, 14 digits `<season(4)><competition(4)><sequence(6)>` (game_id in nl_schedule; the same id as the sihf family's game_id), e.g. 20261105000325. |

### Returns {#nl_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `team_side` | character | home or away: the side of the team this row belongs to. |
| `team_id` | integer | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `g` | integer | Number of goals scored |
| `ga` | integer | Goals against: number of goals conceded |
| `sa` | integer | Shots on goal against |
| `sog1` | integer | Shots on goal in the 1st period |
| `sog2` | integer | Shots on goal in the 2nd period |
| `sog3` | integer | Shots on goal in the 3rd period |
| `sog_ot` | integer | Shots on goal in overtime, all periods |
| `sog_ot1` | integer | Shots on goal in overtime period 1 |
| `sog_ot2` | integer | Shots on goal in overtime period 2 |
| `sog_ot3` | integer | Shots on goal in overtime period 3 |
| `shm` | integer | Shots that missed the net |
| `shp` | integer | Shots that hit the post |
| `shb` | integer | Own shots blocked by an opponent |
| `bks` | integer | Opponent shots blocked |
| `fow_d` | integer | Defensive-zone faceoffs won (0 in every sampled game) |
| `fow_n` | integer | Neutral-zone faceoffs won (0 in every sampled game) |
| `fow_o` | integer | Offensive-zone faceoffs won (0 in every sampled game) |
| `fol_n` | integer | Neutral-zone faceoffs lost (0 in every sampled game) |
| `fol_o` | integer | Offensive-zone faceoffs lost (0 in every sampled game) |
| `fol_d` | integer | Defensive-zone faceoffs lost (0 in every sampled game) |
| `ppt` | integer | Power-play time, seconds |
| `ppo` | integer | Power-play opportunities |
| `ppg` | integer | Power-play goals |
| `ppga` | integer | Shorthanded goals allowed while on the power play |
| `pkt` | integer | Shorthanded time, seconds |
| `pks` | integer | Times shorthanded |
| `pkg` | integer | Shorthanded goals scored |
| `pkga` | integer | Power-play goals allowed while shorthanded |
| `faceoffs` | integer | Faceoffs taken (upstream `fo`: fow + fol). Not penalties. |
| `pim` | integer | Penalty minutes |
| `sog` | integer | Number of shots on goal |
| `sog_percentage` | numeric | Shooting percentage, 100 * g / sog |
| `pp_percentage` | numeric | Power-play percentage, 100 * ppg / ppo |
| `pk_percentage` | numeric | Penalty-kill percentage, 0-100 |
| `fow` | integer | Number of faceoffs won |
| `fol` | integer | Number of faceoffs lost |
| `faceoffs_percentage` | numeric | Faceoff win percentage (upstream `foPercentage`); -1 without faceoffs. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_team_stats-example}

```python
nl_team_stats(game_id=20261105000325)
```

_Last validated n/a._

## nl_team_player_stats

Season stats for one team's players.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/player/team/{team_id}`

**Valid URL:** [https://www.nationalleague.ch/api/player/team/101144?season=2026&phase=4940](https://www.nationalleague.ch/api/player/team/101144?season=2026&phase=4940)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | SIHF team id (team_id in nl_standings; the same id as the sihf family's team_id), e.g. 101144 = EV Zug. |
| `season` | `season` |  |  | `Y` | Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |
| `phase` | `phase` |  |  | `Y` | SIHF phase id (see x-phase-ids). On /player, omitting it served the regular season (byte-identical to phase=4940); that default is assumed but unverified on this route. Phase ids change every season; whether the metadata ids apply to 2027 is unverified. |

### Returns {#nl_team_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `first_name` | character | First (given) name of the player |
| `last_name` | character | Last (family) name of the player |
| `team_id` | integer | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `team_short_name` | character | Team abbreviation |
| `team_name` | character | Team name as the league app displays it |
| `number` | character | Sweater number, a string |
| `position` | character | goalkeeper, defender or forwarder (sic); one of 502 rows in the full 2026 body has the integer 0 |
| `topscorer_points` | integer | PostFinance Top Scorer game score (an app fantasy rating, not on-ice points) |
| `topscorer_point_average` | numeric | PostFinance Top Scorer game score per game |
| `topscorer_market_value` | integer | PostFinance Top Scorer game market value |
| `topscorer_market_value_trend` | integer | Market-value trend: 1 up, 0 flat, -1 down |
| `topscorer_amount` | integer | CHF amount credited to the team's Top Scorer jersey holder (non-zero for 14 players, one per team) |
| `is_topscorer` | logical | Wears the team's PostFinance Top Scorer jersey (one per team) |
| `nationality` | character | Nationality; unobserved: null or empty in every sampled row |
| `birth` | character | Date of birth, YYYY-MM-DD |
| `height` | integer | Height; unobserved: null or empty in every sampled row |
| `weight` | integer | Weight; unobserved: null or empty in every sampled row |
| `hand` | character | Shooting or catching hand; unobserved: null or empty in every sampled row |
| `lcrid` | integer | Not determined (small integer, 0-4 in 2026) |
| `noc` | integer | Not determined (small integer, 0-2 in 2026) |
| `gp` | integer | Number of games played |
| `g` | integer | Number of goals scored |
| `a1` | integer | First (primary) assists credited |
| `a2` | integer | Second (secondary) assists credited |
| `penalties` | integer | Penalties taken (upstream `fo`: fo2 + fo5 + fo10 + gm + mp). Not faceoffs: see face_offs / fot. |
| `pim` | integer | Penalty minutes |
| `geq` | integer | Even-strength goals |
| `gpp` | integer | Power-play goals |
| `gpk` | integer | Shorthanded goals |
| `gwg` | integer | Game-winning goals |
| `ps` | integer | Shootout shots taken |
| `pg` | integer | Not determined; 0 on every 2026 row |
| `pl` | integer | Plus count: on ice for an even-strength or shorthanded goal for |
| `mi` | integer | Minus count: on ice for a goal against |
| `toi` | integer | Time on ice, seconds |
| `toi_eq` | integer | Even-strength time on ice, seconds |
| `toipp` | integer | Power-play time on ice, seconds (lower-case pp, unlike the box score's toiPp) |
| `toi_pk` | integer | Shorthanded time on ice, seconds |
| `sog` | integer | Number of shots on goal |
| `shm` | integer | Shots that missed the net |
| `shp` | integer | Shots that hit the post |
| `shb` | integer | Own shots blocked by an opponent |
| `bks` | integer | Opponent shots blocked |
| `fow` | integer | Number of faceoffs won |
| `fol` | integer | Number of faceoffs lost |
| `shi` | integer | Number of shifts played |
| `gwgk` | integer | Not determined; 0 on every 2026 row |
| `gw` | integer | Goaltender wins (incl. overtime and shootout) |
| `gl` | integer | Goaltender losses (incl. overtime and shootout) |
| `fk` | integer | Games started in goal (inferred: fk <= gp; 42 of 44 for the 2026 games-played leader) |
| `ga` | integer | Goals against: number of goals conceded |
| `sa` | integer | Shots on goal against |
| `svs` | integer | Number of saves made by the goaltender |
| `so` | integer | Number of shutouts (games without a goal conceded) |
| `mip` | integer | Goaltender time played, hundredths of a minute (6000 = 60:00; = seconds / 0.6) |
| `assists` | integer | Assists (a1 + a2; verified on every 2026 row) |
| `points` | integer | Points (g + assists; verified) |
| `goal_efficiency` | numeric | Shooting percentage, 100 * g / sog (verified); -1 with no shots |
| `save_percentage` | numeric | Save percentage, 0-100 |
| `face_offs_percentage` | numeric | Faceoff win percentage, 0-100; -1 with no faceoffs |
| `face_offs` | integer | Faceoffs taken (fow + fol; verified) |
| `is_captain` | logical | Whether the player is the team captain |
| `first_goalkeeper` | logical | Team's first-choice goaltender |
| `ga_per_game` | numeric | Goals against per game (ga / gp; verified) |
| `points_per_game` | numeric | Points per game |
| `svs_per_game` | numeric | Goaltender saves per game played |
| `bks_per_game` | numeric | Blocked shots per game |
| `sog_per_game` | numeric | Shots on goal per game |
| `shm_per_game` | numeric | Missed shots per game |
| `shp_per_game` | numeric | Post hits per game |
| `fow_per_game` | numeric | Faceoffs won per game |
| `fol_per_game` | numeric | Faceoffs lost per game |
| `toi_eq_per_game` | numeric | Even-strength time on ice per game, seconds |
| `toi_pp_per_game` | numeric | Power-play time on ice per game, seconds |
| `toi_pk_per_game` | numeric | Shorthanded time on ice per game, seconds |
| `face_offs_per_game_percentage` | numeric | Not determined; 0 on the sampled rows |
| `toi_average` | numeric | Time on ice per game, seconds |
| `plus_minus` | integer | Plus/minus (pl - mi; verified) |
| `tgs` | integer | Team's total game time, seconds (about 3600 per game incl. overtime) |
| `sep` | integer | Goaltender time played, seconds |
| `toi_percentage` | numeric | Share of the team's game time in goal, 100 * sep / tgs (verified) |
| `gwit` | integer | Wins in regulation time (gw = gwit + overtime/shootout wins; verified) |
| `glit` | integer | Losses in regulation time (gl = glit + overtime/shootout losses; verified) |
| `sag` | integer | Shots on goal against that were saved (= svs on every 2026 goaltender row) |
| `sago` | integer | Shots on goal against that went in (= ga on every 2026 goaltender row) |
| `sam` | integer | Shots against that missed the net |
| `sap` | integer | Shots against that hit the post |
| `sab` | integer | Shots against that were blocked |
| `saslot` | integer | Slot shots against |
| `sagoslot` | integer | Slot shots against that went in |
| `aeq` | integer | Even-strength assists |
| `app` | integer | Power-play assists |
| `apk` | integer | Shorthanded assists |
| `ha` | integer | Hat tricks (inferred; at most 2 in 2026) |
| `fo2` | integer | Minor (2-minute) penalties |
| `fo5` | integer | Major (5-minute) penalties |
| `fo10` | integer | 10-minute misconducts |
| `gm` | integer | Game misconducts |
| `mp` | integer | Match penalties |
| `pm` | integer | Not determined; 0 on every 2026 row |
| `pwg` | integer | Not determined; 0 on every 2026 row |
| `sgo` | integer | Not determined. Equals gff + gfd on every team row; skater values are close to but not always g |
| `fowoz` | integer | Offensive-zone faceoffs won |
| `fownz` | integer | Neutral-zone faceoffs won |
| `fowdz` | integer | Defensive-zone faceoffs won |
| `foloz` | integer | Offensive-zone faceoffs lost |
| `folnz` | integer | Neutral-zone faceoffs lost |
| `foldz` | integer | Defensive-zone faceoffs lost |
| `psg` | integer | Goals scored in shootouts |
| `pssvs` | integer | Saves made in shootouts |
| `got` | integer | Overtime goals (a subset of g) |
| `ot_rank` | integer | Overtime-scoring rank; 0 on the 2026 rows (filled in 2027) |
| `ot_g` | integer | Overtime goals in the overtime table; 0 on every 2026 row |
| `ot_a1` | integer | Overtime first assists; 0 on every 2026 row |
| `ot_a2` | integer | Overtime second assists; 0 on every 2026 row |
| `ot_toi` | integer | Overtime time on ice, seconds; 0 on every 2026 row |
| `ot_a` | integer | Overtime assists; 0 on every 2026 row |
| `ot_p` | integer | Overtime points; 0 on every 2026 row |
| `psgwg` | integer | Game-deciding shootout goals; 0 on every 2026 row |
| `sogslot` | integer | Shots on goal from the slot |
| `pswg` | integer | Shootout winners (inferred) |
| `toishi` | numeric | Time on ice per shift, seconds |
| `shigp` | numeric | Shifts per game |
| `saslotgp` | numeric | Slot shots against per game |
| `samgp` | numeric | Missed shots against per game |
| `sapgp` | numeric | Post hits against per game |
| `svs_pslot` | numeric | Slot save percentage, 100 * (saslot - sagoslot) / saslot |
| `sa_per_game` | numeric | Shots on goal against per game |
| `otw` | integer | Goaltender overtime or shootout wins |
| `otl` | integer | Goaltender overtime or shootout losses |
| `fopoz` | numeric | Offensive-zone faceoff win percentage, 0-100 |
| `fopnz` | numeric | Neutral-zone faceoff win percentage, 0-100 |
| `fopdz` | numeric | Defensive-zone faceoff win percentage, 0-100 |
| `fot` | integer | Faceoffs taken (fow + fol) |
| `fogp` | numeric | Faceoffs taken per game |
| `psm` | integer | Shootout misses (inferred; ps = psg + psm fails for 67 of 447 skaters) |
| `ps_percentage` | numeric | Shootout success percentage, 0-100 |
| `gw_shutout` | integer | Not determined; equals gw on goaltender rows |
| `sogslot_per_game` | numeric | Slot shots on goal per game |
| `psag` | integer | Shootout goals against (equals ps on goaltender rows) |
| `pssvs_percentage` | numeric | Shootout save percentage, 0-100; -1 with no shootout shots faced |
| `show_rank` | logical | App flag: show the row's rank |
| `chf` | integer | CHF credited for the player's points: 300 * points on every 2026 row (PostFinance youth-fund scheme; inferred) |
| `team_stats` | character | Per-team split for a player who changed teams; unobserved: null or empty in every sampled row |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_team_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_team_player_stats-example}

```python
nl_team_player_stats(phase=4940, season=2026, team_id=101144)
```

_Last validated n/a._

## nl_team_info

Club CMS record: names, logo, website, ticket links.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/teams/{team_id}/info`

**Valid URL:** [https://www.nationalleague.ch/api/teams/101144/info](https://www.nationalleague.ch/api/teams/101144/info)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | SIHF team id (team_id in nl_standings; the same id as the sihf family's team_id), e.g. 101144 = EV Zug. |

### Returns {#nl_team_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | character | Club name from the club's CMS record |
| `cms_node_id` | integer | Umbraco CMS node id of the club page (Int64; not a team id). |
| `url_segment` | character | URL slug of the club page |
| `type` | character | CMS node kind ('Content') |
| `content_type` | character | CMS document type ('club') |
| `url` | character | Site path of the club page |
| `fields_grid` | character | Page layout blocks; unobserved: null or empty in every sampled row |
| `fields_logo_name` | character | Title of the club logo media item in the club CMS |
| `fields_logo_id` | integer | Umbraco CMS media id; not a team id |
| `fields_logo_url_segment` | character | URL slug; unobserved: null or empty in every sampled row |
| `fields_logo_type` | character | CMS node kind ('Media') |
| `fields_logo_content_type` | character | Media kind ('Image') |
| `fields_logo_url` | character | Site path of the image |
| `fields_logo_fields_umbraco_file_src` | character | Site path of the file |
| `fields_logo_fields_umbraco_file_focal_point` | character | Crop focal point; unobserved: null or empty in every sampled row |
| `fields_logo_fields_umbraco_file_crops` | character | Crop presets; unobserved: null or empty in every sampled row |
| `fields_logo_fields_umbraco_width` | integer | Image width, pixels |
| `fields_logo_fields_umbraco_height` | integer | Image height, pixels |
| `fields_logo_fields_umbraco_bytes` | integer | File size, bytes |
| `fields_logo_fields_umbraco_extension` | character | File extension of the club logo media file |
| `team_id` | integer | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `fields_website` | character | Club website URL |
| `fields_ticket_url` | character | Club ticket-shop URL |
| `fields_game_ticket_url` | character | Per-game ticket URL template; empty string in the sample |
| `fields_overriden_name` | character | Display-name override (sic); empty string in the sample |
| `fields_players` | character | Roster override; empty string in the sample |
| `fields_api_name` | character | Club key in the app's backend (e.g. 'evz') |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_team_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_team_info-example}

```python
nl_team_info(team_id=101144)
```

_Last validated n/a._
