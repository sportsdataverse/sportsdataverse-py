# National League — National League app API (www.nationalleague.ch/api, Switzerland) — Player

> National League — National League app API (www.nationalleague.ch/api, Switzerland) — Player — function reference in sdv-py, the SportsDataverse Python package.

## nl_player_box

One game's player box score: goals, assists, shots, faceoffs, ice time, goaltending.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/games/{game_id}`

**Valid URL:** [https://www.nationalleague.ch/api/games/20261105000325](https://www.nationalleague.ch/api/games/20261105000325)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | SIHF game id, 14 digits `<season(4)><competition(4)><sequence(6)>` (game_id in nl_schedule; the same id as the sihf family's game_id), e.g. 20261105000325. |

### Returns {#nl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | SIHF game id `<season(4)><competition(4)><sequence(6)>` (Int64; e.g. 20261105000325, 1105 = National League). The same id as the sihf family's game_id; its first four digits are the season END year. |
| `team_side` | String | home or away: the side of the team this row belongs to. |
| `team_id` | Int64 | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `player_id` | Int64 | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `number` | String | Sweater number, a string |
| `first_name` | String | First (given) name of the player |
| `last_name` | String | Last (family) name of the player |
| `short_name` | String | Initial and last name, e.g. 'S. Aeschlimann' |
| `name` | String | First and last name |
| `position` | String | goalkeeper, defender or forwarder (sic) |
| `fow` | Int64 | Number of faceoffs won |
| `fol` | Int64 | Number of faceoffs lost |
| `sog` | Int64 | Number of shots on goal |
| `shm` | Int64 | Shots that missed the net |
| `shp` | Int64 | Shots that hit the post |
| `shb` | Int64 | Own shots blocked by an opponent |
| `bks` | Int64 | Opponent shots blocked |
| `toi` | Int64 | Time on ice, seconds |
| `toi_eq` | Int64 | Even-strength time on ice, seconds |
| `toi_pp` | Int64 | Power-play time on ice, seconds |
| `toi_pk` | Int64 | Shorthanded time on ice, seconds |
| `pim` | Int64 | Penalty minutes |
| `g` | Int64 | Number of goals scored |
| `a1` | Int64 | First (primary) assists credited |
| `a2` | Int64 | Second (secondary) assists credited |
| `pl` | Int64 | Plus count: on ice for an even-strength or shorthanded goal for |
| `mi` | Int64 | Minus count: on ice for a goal against |
| `mip` | Int64 | Goaltender time played, hundredths of a minute (6000 = 60:00; = seconds / 0.6) |
| `ga` | Int64 | Goals against: number of goals conceded |
| `svs` | Int64 | Number of saves made by the goaltender |
| `sa` | Int64 | Shots on goal against |
| `gaa` | Float64 | Goals-against average; 0 in every sampled row, even for goaltenders who conceded |
| `save_percentage` | Float64 | Save percentage, 0-100 |
| `faceoffs` | Int64 | Faceoffs taken (upstream `fo`: fow + fol). Not penalties. |
| `faceoffs_percentage` | Float64 | Faceoff win percentage (upstream `foPercentage`); -1 without faceoffs. |
| `a` | Int64 | Assists (a1 + a2) |
| `pts` | Int64 | Points (goals + assists) |
| `pl_mi` | Int64 | Plus/minus (pl - mi) |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_player_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_player_box-example}

```python
nl_player_box(game_id=20261105000325)
```

## nl_player_stats

Season stats for every player, with playerId.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/player`

**Valid URL:** [https://www.nationalleague.ch/api/player?season=2026&phase=4940](https://www.nationalleague.ch/api/player?season=2026&phase=4940)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |
| `phase` | `phase` |  |  | `Y` | SIHF phase id (see x-phase-ids). Omitted = the regular season: `/player?season=2026` and `phase=4940` answer byte-identical bodies, `phase=5297` (playoffs) a different one (max gp 19 vs 52). Phase ids change every season; whether the metadata ids apply to 2027 is unverified. |

### Returns {#nl_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `first_name` | String | First (given) name of the player |
| `last_name` | String | Last (family) name of the player |
| `team_id` | Int64 | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `team_short_name` | String | Team abbreviation |
| `team_name` | String | Team name as the league app displays it |
| `number` | String | Sweater number, a string |
| `position` | String | goalkeeper, defender or forwarder (sic); one of 502 rows in the full 2026 body has the integer 0 |
| `topscorer_points` | Int64 | PostFinance Top Scorer game score (an app fantasy rating, not on-ice points) |
| `topscorer_point_average` | Float64 | PostFinance Top Scorer game score per game |
| `topscorer_market_value` | Int64 | PostFinance Top Scorer game market value |
| `topscorer_market_value_trend` | Int64 | Market-value trend: 1 up, 0 flat, -1 down |
| `topscorer_amount` | Int64 | CHF amount credited to the team's Top Scorer jersey holder (non-zero for 14 players, one per team) |
| `is_topscorer` | Boolean | Wears the team's PostFinance Top Scorer jersey (one per team) |
| `nationality` | String | Nationality; unobserved: null or empty in every sampled row |
| `birth` | String | Date of birth, YYYY-MM-DD |
| `height` | Int64 | Height; unobserved: null or empty in every sampled row |
| `weight` | Int64 | Weight; unobserved: null or empty in every sampled row |
| `hand` | String | Shooting or catching hand; unobserved: null or empty in every sampled row |
| `lcrid` | Int64 | Not determined (small integer, 0-4 in 2026) |
| `noc` | Int64 | Not determined (small integer, 0-2 in 2026) |
| `gp` | Int64 | Number of games played |
| `g` | Int64 | Number of goals scored |
| `a1` | Int64 | First (primary) assists credited |
| `a2` | Int64 | Second (secondary) assists credited |
| `penalties` | Int64 | Penalties taken (upstream `fo`: fo2 + fo5 + fo10 + gm + mp). Not faceoffs: see face_offs / fot. |
| `pim` | Int64 | Penalty minutes |
| `geq` | Int64 | Even-strength goals |
| `gpp` | Int64 | Power-play goals |
| `gpk` | Int64 | Shorthanded goals |
| `gwg` | Int64 | Game-winning goals |
| `ps` | Int64 | Shootout shots taken |
| `pg` | Int64 | Not determined; 0 on every 2026 row |
| `pl` | Int64 | Plus count: on ice for an even-strength or shorthanded goal for |
| `mi` | Int64 | Minus count: on ice for a goal against |
| `toi` | Int64 | Time on ice, seconds |
| `toi_eq` | Int64 | Even-strength time on ice, seconds |
| `toipp` | Int64 | Power-play time on ice, seconds (lower-case pp, unlike the box score's toiPp) |
| `toi_pk` | Int64 | Shorthanded time on ice, seconds |
| `sog` | Int64 | Number of shots on goal |
| `shm` | Int64 | Shots that missed the net |
| `shp` | Int64 | Shots that hit the post |
| `shb` | Int64 | Own shots blocked by an opponent |
| `bks` | Int64 | Opponent shots blocked |
| `fow` | Int64 | Number of faceoffs won |
| `fol` | Int64 | Number of faceoffs lost |
| `shi` | Int64 | Number of shifts played |
| `gwgk` | Int64 | Not determined; 0 on every 2026 row |
| `gw` | Int64 | Goaltender wins (incl. overtime and shootout) |
| `gl` | Int64 | Goaltender losses (incl. overtime and shootout) |
| `fk` | Int64 | Games started in goal (inferred: fk <= gp; 42 of 44 for the 2026 games-played leader) |
| `ga` | Int64 | Goals against: number of goals conceded |
| `sa` | Int64 | Shots on goal against |
| `svs` | Int64 | Number of saves made by the goaltender |
| `so` | Int64 | Number of shutouts (games without a goal conceded) |
| `mip` | Int64 | Goaltender time played, hundredths of a minute (6000 = 60:00; = seconds / 0.6) |
| `assists` | Int64 | Assists (a1 + a2; verified on every 2026 row) |
| `points` | Int64 | Points (g + assists; verified) |
| `goal_efficiency` | Float64 | Shooting percentage, 100 * g / sog (verified); -1 with no shots |
| `save_percentage` | Float64 | Save percentage, 0-100 |
| `face_offs_percentage` | Float64 | Faceoff win percentage, 0-100; -1 with no faceoffs |
| `face_offs` | Int64 | Faceoffs taken (fow + fol; verified) |
| `is_captain` | Boolean | Whether the player is the team captain |
| `first_goalkeeper` | Boolean | Team's first-choice goaltender |
| `ga_per_game` | Float64 | Goals against per game (ga / gp; verified) |
| `points_per_game` | Float64 | Points per game |
| `svs_per_game` | Float64 | Goaltender saves per game played |
| `bks_per_game` | Float64 | Blocked shots per game |
| `sog_per_game` | Float64 | Shots on goal per game |
| `shm_per_game` | Float64 | Missed shots per game |
| `shp_per_game` | Float64 | Post hits per game |
| `fow_per_game` | Float64 | Faceoffs won per game |
| `fol_per_game` | Float64 | Faceoffs lost per game |
| `toi_eq_per_game` | Float64 | Even-strength time on ice per game, seconds |
| `toi_pp_per_game` | Float64 | Power-play time on ice per game, seconds |
| `toi_pk_per_game` | Float64 | Shorthanded time on ice per game, seconds |
| `face_offs_per_game_percentage` | Float64 | Not determined; 0 on the sampled rows |
| `toi_average` | Float64 | Time on ice per game, seconds |
| `plus_minus` | Int64 | Plus/minus (pl - mi; verified) |
| `tgs` | Int64 | Team's total game time, seconds (about 3600 per game incl. overtime) |
| `sep` | Int64 | Goaltender time played, seconds |
| `toi_percentage` | Float64 | Share of the team's game time in goal, 100 * sep / tgs (verified) |
| `gwit` | Int64 | Wins in regulation time (gw = gwit + overtime/shootout wins; verified) |
| `glit` | Int64 | Losses in regulation time (gl = glit + overtime/shootout losses; verified) |
| `sag` | Int64 | Shots on goal against that were saved (= svs on every 2026 goaltender row) |
| `sago` | Int64 | Shots on goal against that went in (= ga on every 2026 goaltender row) |
| `sam` | Int64 | Shots against that missed the net |
| `sap` | Int64 | Shots against that hit the post |
| `sab` | Int64 | Shots against that were blocked |
| `saslot` | Int64 | Slot shots against |
| `sagoslot` | Int64 | Slot shots against that went in |
| `aeq` | Int64 | Even-strength assists |
| `app` | Int64 | Power-play assists |
| `apk` | Int64 | Shorthanded assists |
| `ha` | Int64 | Hat tricks (inferred; at most 2 in 2026) |
| `fo2` | Int64 | Minor (2-minute) penalties |
| `fo5` | Int64 | Major (5-minute) penalties |
| `fo10` | Int64 | 10-minute misconducts |
| `gm` | Int64 | Game misconducts |
| `mp` | Int64 | Match penalties |
| `pm` | Int64 | Not determined; 0 on every 2026 row |
| `pwg` | Int64 | Not determined; 0 on every 2026 row |
| `sgo` | Int64 | Not determined. Equals gff + gfd on every team row; skater values are close to but not always g |
| `fowoz` | Int64 | Offensive-zone faceoffs won |
| `fownz` | Int64 | Neutral-zone faceoffs won |
| `fowdz` | Int64 | Defensive-zone faceoffs won |
| `foloz` | Int64 | Offensive-zone faceoffs lost |
| `folnz` | Int64 | Neutral-zone faceoffs lost |
| `foldz` | Int64 | Defensive-zone faceoffs lost |
| `psg` | Int64 | Goals scored in shootouts |
| `pssvs` | Int64 | Saves made in shootouts |
| `got` | Int64 | Overtime goals (a subset of g) |
| `ot_rank` | Int64 | Overtime-scoring rank; 0 on the 2026 rows (filled in 2027) |
| `ot_g` | Int64 | Overtime goals in the overtime table; 0 on every 2026 row |
| `ot_a1` | Int64 | Overtime first assists; 0 on every 2026 row |
| `ot_a2` | Int64 | Overtime second assists; 0 on every 2026 row |
| `ot_toi` | Int64 | Overtime time on ice, seconds; 0 on every 2026 row |
| `ot_a` | Int64 | Overtime assists; 0 on every 2026 row |
| `ot_p` | Int64 | Overtime points; 0 on every 2026 row |
| `psgwg` | Int64 | Game-deciding shootout goals; 0 on every 2026 row |
| `sogslot` | Int64 | Shots on goal from the slot |
| `pswg` | Int64 | Shootout winners (inferred) |
| `toishi` | Float64 | Time on ice per shift, seconds |
| `shigp` | Float64 | Shifts per game |
| `saslotgp` | Float64 | Slot shots against per game |
| `samgp` | Float64 | Missed shots against per game |
| `sapgp` | Float64 | Post hits against per game |
| `svs_pslot` | Float64 | Slot save percentage, 100 * (saslot - sagoslot) / saslot |
| `sa_per_game` | Float64 | Shots on goal against per game |
| `otw` | Int64 | Goaltender overtime or shootout wins |
| `otl` | Int64 | Goaltender overtime or shootout losses |
| `fopoz` | Float64 | Offensive-zone faceoff win percentage, 0-100 |
| `fopnz` | Float64 | Neutral-zone faceoff win percentage, 0-100 |
| `fopdz` | Float64 | Defensive-zone faceoff win percentage, 0-100 |
| `fot` | Int64 | Faceoffs taken (fow + fol) |
| `fogp` | Float64 | Faceoffs taken per game |
| `psm` | Int64 | Shootout misses (inferred; ps = psg + psm fails for 67 of 447 skaters) |
| `ps_percentage` | Float64 | Shootout success percentage, 0-100 |
| `gw_shutout` | Int64 | Not determined; equals gw on goaltender rows |
| `sogslot_per_game` | Float64 | Slot shots on goal per game |
| `psag` | Int64 | Shootout goals against (equals ps on goaltender rows) |
| `pssvs_percentage` | Float64 | Shootout save percentage, 0-100; -1 with no shootout shots faced |
| `show_rank` | Boolean | App flag: show the row's rank |
| `chf` | Int64 | CHF credited for the player's points: 300 * points on every 2026 row (PostFinance youth-fund scheme; inferred) |
| `team_stats` | String | Per-team split for a player who changed teams; unobserved: null or empty in every sampled row |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_player_stats-example}

```python
nl_player_stats(phase=4940, season=2026)
```

## nl_player_season

One player's season stats.

**Endpoint URL:** `GET https://www.nationalleague.ch/api/player/{player_id}`

**Valid URL:** [https://www.nationalleague.ch/api/player/305519?season=2026&phase=4940](https://www.nationalleague.ch/api/player/305519?season=2026&phase=4940)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | SIHF licence number (player_id in nl_player_stats; joins the sihf family's player_id), e.g. 305519. |
| `season` | `season` |  |  | `Y` | Omitted = the current season: /player/305519 without it answered 2027 (gp 7; 51 for season=2026). Season END year: `2026` = 2025/26. Diffing 2026 vs 2027 changes every list, and /player/{playerId} gp (51 vs 7), so it scopes. Lists are served for 2019-2027 (the app's season picker, /api/content/metadata `seasons`) and patchily before: 2012 and 2017 answered, 2010/2014/2015/2016/2018 answered `[]` (probed 2026-10-12). |
| `phase` | `phase` |  |  | `Y` | SIHF phase id (see x-phase-ids). On /player, omitting it served the regular season (byte-identical to phase=4940); that default is assumed but unverified on this route. Phase ids change every season; whether the metadata ids apply to 2027 is unverified. |

### Returns {#nl_player_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | SIHF licence number (Int64; e.g. 305519 = Tobias Geisser). Joins the sihf family's player_id and every SIHF *LicenceNr. |
| `first_name` | String | First (given) name of the player |
| `last_name` | String | Last (family) name of the player |
| `team_id` | Int64 | SIHF team id (Int64; e.g. 101151 = HC Davos). The same key space as the sihf family's team_id. |
| `team_short_name` | String | Team abbreviation |
| `team_name` | String | Team name as the league app displays it |
| `number` | String | Sweater number, a string |
| `position` | String | goalkeeper, defender or forwarder (sic); one of 502 rows in the full 2026 body has the integer 0 |
| `topscorer_points` | Int64 | PostFinance Top Scorer game score (an app fantasy rating, not on-ice points) |
| `topscorer_point_average` | Float64 | PostFinance Top Scorer game score per game |
| `topscorer_market_value` | Int64 | PostFinance Top Scorer game market value |
| `topscorer_market_value_trend` | Int64 | Market-value trend: 1 up, 0 flat, -1 down |
| `topscorer_amount` | Int64 | CHF amount credited to the team's Top Scorer jersey holder (non-zero for 14 players, one per team) |
| `is_topscorer` | Boolean | Wears the team's PostFinance Top Scorer jersey (one per team) |
| `nationality` | String | Nationality; unobserved: null or empty in every sampled row |
| `birth` | String | Date of birth, YYYY-MM-DD |
| `height` | Int64 | Height; unobserved: null or empty in every sampled row |
| `weight` | Int64 | Weight; unobserved: null or empty in every sampled row |
| `hand` | String | Shooting or catching hand; unobserved: null or empty in every sampled row |
| `lcrid` | Int64 | Not determined (small integer, 0-4 in 2026) |
| `noc` | Int64 | Not determined (small integer, 0-2 in 2026) |
| `gp` | Int64 | Number of games played |
| `g` | Int64 | Number of goals scored |
| `a1` | Int64 | First (primary) assists credited |
| `a2` | Int64 | Second (secondary) assists credited |
| `penalties` | Int64 | Penalties taken (upstream `fo`: fo2 + fo5 + fo10 + gm + mp). Not faceoffs: see face_offs / fot. |
| `pim` | Int64 | Penalty minutes |
| `geq` | Int64 | Even-strength goals |
| `gpp` | Int64 | Power-play goals |
| `gpk` | Int64 | Shorthanded goals |
| `gwg` | Int64 | Game-winning goals |
| `ps` | Int64 | Shootout shots taken |
| `pg` | Int64 | Not determined; 0 on every 2026 row |
| `pl` | Int64 | Plus count: on ice for an even-strength or shorthanded goal for |
| `mi` | Int64 | Minus count: on ice for a goal against |
| `toi` | Int64 | Time on ice, seconds |
| `toi_eq` | Int64 | Even-strength time on ice, seconds |
| `toipp` | Int64 | Power-play time on ice, seconds (lower-case pp, unlike the box score's toiPp) |
| `toi_pk` | Int64 | Shorthanded time on ice, seconds |
| `sog` | Int64 | Number of shots on goal |
| `shm` | Int64 | Shots that missed the net |
| `shp` | Int64 | Shots that hit the post |
| `shb` | Int64 | Own shots blocked by an opponent |
| `bks` | Int64 | Opponent shots blocked |
| `fow` | Int64 | Number of faceoffs won |
| `fol` | Int64 | Number of faceoffs lost |
| `shi` | Int64 | Number of shifts played |
| `gwgk` | Int64 | Not determined; 0 on every 2026 row |
| `gw` | Int64 | Goaltender wins (incl. overtime and shootout) |
| `gl` | Int64 | Goaltender losses (incl. overtime and shootout) |
| `fk` | Int64 | Games started in goal (inferred: fk <= gp; 42 of 44 for the 2026 games-played leader) |
| `ga` | Int64 | Goals against: number of goals conceded |
| `sa` | Int64 | Shots on goal against |
| `svs` | Int64 | Number of saves made by the goaltender |
| `so` | Int64 | Number of shutouts (games without a goal conceded) |
| `mip` | Int64 | Goaltender time played, hundredths of a minute (6000 = 60:00; = seconds / 0.6) |
| `assists` | Int64 | Assists (a1 + a2; verified on every 2026 row) |
| `points` | Int64 | Points (g + assists; verified) |
| `goal_efficiency` | Float64 | Shooting percentage, 100 * g / sog (verified); -1 with no shots |
| `save_percentage` | Float64 | Save percentage, 0-100 |
| `face_offs_percentage` | Float64 | Faceoff win percentage, 0-100; -1 with no faceoffs |
| `face_offs` | Int64 | Faceoffs taken (fow + fol; verified) |
| `is_captain` | Boolean | Whether the player is the team captain |
| `first_goalkeeper` | Boolean | Team's first-choice goaltender |
| `ga_per_game` | Float64 | Goals against per game (ga / gp; verified) |
| `points_per_game` | Float64 | Points per game |
| `svs_per_game` | Float64 | Goaltender saves per game played |
| `bks_per_game` | Float64 | Blocked shots per game |
| `sog_per_game` | Float64 | Shots on goal per game |
| `shm_per_game` | Float64 | Missed shots per game |
| `shp_per_game` | Float64 | Post hits per game |
| `fow_per_game` | Float64 | Faceoffs won per game |
| `fol_per_game` | Float64 | Faceoffs lost per game |
| `toi_eq_per_game` | Float64 | Even-strength time on ice per game, seconds |
| `toi_pp_per_game` | Float64 | Power-play time on ice per game, seconds |
| `toi_pk_per_game` | Float64 | Shorthanded time on ice per game, seconds |
| `face_offs_per_game_percentage` | Float64 | Not determined; 0 on the sampled rows |
| `toi_average` | Float64 | Time on ice per game, seconds |
| `plus_minus` | Int64 | Plus/minus (pl - mi; verified) |
| `tgs` | Int64 | Team's total game time, seconds (about 3600 per game incl. overtime) |
| `sep` | Int64 | Goaltender time played, seconds |
| `toi_percentage` | Float64 | Share of the team's game time in goal, 100 * sep / tgs (verified) |
| `gwit` | Int64 | Wins in regulation time (gw = gwit + overtime/shootout wins; verified) |
| `glit` | Int64 | Losses in regulation time (gl = glit + overtime/shootout losses; verified) |
| `sag` | Int64 | Shots on goal against that were saved (= svs on every 2026 goaltender row) |
| `sago` | Int64 | Shots on goal against that went in (= ga on every 2026 goaltender row) |
| `sam` | Int64 | Shots against that missed the net |
| `sap` | Int64 | Shots against that hit the post |
| `sab` | Int64 | Shots against that were blocked |
| `saslot` | Int64 | Slot shots against |
| `sagoslot` | Int64 | Slot shots against that went in |
| `aeq` | Int64 | Even-strength assists |
| `app` | Int64 | Power-play assists |
| `apk` | Int64 | Shorthanded assists |
| `ha` | Int64 | Hat tricks (inferred; at most 2 in 2026) |
| `fo2` | Int64 | Minor (2-minute) penalties |
| `fo5` | Int64 | Major (5-minute) penalties |
| `fo10` | Int64 | 10-minute misconducts |
| `gm` | Int64 | Game misconducts |
| `mp` | Int64 | Match penalties |
| `pm` | Int64 | Not determined; 0 on every 2026 row |
| `pwg` | Int64 | Not determined; 0 on every 2026 row |
| `sgo` | Int64 | Not determined. Equals gff + gfd on every team row; skater values are close to but not always g |
| `fowoz` | Int64 | Offensive-zone faceoffs won |
| `fownz` | Int64 | Neutral-zone faceoffs won |
| `fowdz` | Int64 | Defensive-zone faceoffs won |
| `foloz` | Int64 | Offensive-zone faceoffs lost |
| `folnz` | Int64 | Neutral-zone faceoffs lost |
| `foldz` | Int64 | Defensive-zone faceoffs lost |
| `psg` | Int64 | Goals scored in shootouts |
| `pssvs` | Int64 | Saves made in shootouts |
| `got` | Int64 | Overtime goals (a subset of g) |
| `ot_rank` | Int64 | Overtime-scoring rank; 0 on the 2026 rows (filled in 2027) |
| `ot_g` | Int64 | Overtime goals in the overtime table; 0 on every 2026 row |
| `ot_a1` | Int64 | Overtime first assists; 0 on every 2026 row |
| `ot_a2` | Int64 | Overtime second assists; 0 on every 2026 row |
| `ot_toi` | Int64 | Overtime time on ice, seconds; 0 on every 2026 row |
| `ot_a` | Int64 | Overtime assists; 0 on every 2026 row |
| `ot_p` | Int64 | Overtime points; 0 on every 2026 row |
| `psgwg` | Int64 | Game-deciding shootout goals; 0 on every 2026 row |
| `sogslot` | Int64 | Shots on goal from the slot |
| `pswg` | Int64 | Shootout winners (inferred) |
| `toishi` | Float64 | Time on ice per shift, seconds |
| `shigp` | Float64 | Shifts per game |
| `saslotgp` | Float64 | Slot shots against per game |
| `samgp` | Float64 | Missed shots against per game |
| `sapgp` | Float64 | Post hits against per game |
| `svs_pslot` | Float64 | Slot save percentage, 100 * (saslot - sagoslot) / saslot |
| `sa_per_game` | Float64 | Shots on goal against per game |
| `otw` | Int64 | Goaltender overtime or shootout wins |
| `otl` | Int64 | Goaltender overtime or shootout losses |
| `fopoz` | Float64 | Offensive-zone faceoff win percentage, 0-100 |
| `fopnz` | Float64 | Neutral-zone faceoff win percentage, 0-100 |
| `fopdz` | Float64 | Defensive-zone faceoff win percentage, 0-100 |
| `fot` | Int64 | Faceoffs taken (fow + fol) |
| `fogp` | Float64 | Faceoffs taken per game |
| `psm` | Int64 | Shootout misses (inferred; ps = psg + psm fails for 67 of 447 skaters) |
| `ps_percentage` | Float64 | Shootout success percentage, 0-100 |
| `gw_shutout` | Int64 | Not determined; equals gw on goaltender rows |
| `sogslot_per_game` | Float64 | Slot shots on goal per game |
| `psag` | Int64 | Shootout goals against (equals ps on goaltender rows) |
| `pssvs_percentage` | Float64 | Shootout save percentage, 0-100; -1 with no shootout shots faced |
| `show_rank` | Boolean | App flag: show the row's rank |
| `chf` | Int64 | CHF credited for the player's points: 300 * points on every 2026 row (PostFinance youth-fund scheme; inferred) |
| `team_stats` | String | Per-team split for a player who changed teams; unobserved: null or empty in every sampled row |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nl_player_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. A 404, or an empty answer (no games or rows for that season, team or phase). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. A failed fetch, an error object in a 200, a body without the ids it must carry, or an answer for another season, team, game or player. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#nl_player_season-example}

```python
nl_player_season(phase=4940, player_id=305519, season=2026)
```
