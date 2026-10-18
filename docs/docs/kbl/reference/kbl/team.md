---
title: "KBL — KBL API (api.kbl.or.kr, Korea) — Team"
sidebar_label: "Team"
sidebar_position: 3
description: "KBL — KBL API (api.kbl.or.kr, Korea) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# KBL — KBL API (api.kbl.or.kr, Korea) — Team

## kbl_teams

The current ten clubs with Korean and English names and the code of each club's D-League side.

**Endpoint URL:** `GET https://api.kbl.or.kr/common/teamList`

**Valid URL:** [https://api.kbl.or.kr/common/teamList](https://api.kbl.or.kr/common/teamList)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kbl_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `team_code_d` | character | Team code of the club's D-League (reserve) side, in the same team-code space as `teamCode`. |
| `team_name` | character | Team short name (Korean, no space, e.g. '창원LG' = Changwon LG); the English form is `teamNameEn`. |
| `team_name_full` | character | Team full name (Korean). |
| `team_name_en` | character | Team short name in English. |
| `team_name_full_en` | character | Team full name in English (e.g. 'CHANGWON LG SAKERS'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score or shot chart, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_teams-example}

```python
kbl_teams()
```

_Last validated n/a._

## kbl_team_box

Team box score of a final game with advanced rates and team-only counts, one row per team.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/{gmkey}/team-record`

**Valid URL:** [https://api.kbl.or.kr/match/S47G01N49/team-record](https://api.kbl.or.kr/match/S47G01N49/team-record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gmkey` | `gmkey` |  | `Y` |  | Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError. |

### Returns {#kbl_team_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame. |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `game_no` | integer | Game number inside the competition (glkey): the N part of the gmkey. |
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `score` | integer | Points. This game, team total. |
| `fg` | integer | Two-point field goals made, dunks included: 201 + 207 rows (score = 2*fg + 3*threep + ft holds on every sampled box). This game, team total. |
| `fg_a` | integer | Two-point field-goal attempts, made + missed, dunks included: 201 + 202 + 207 + 208 rows (checked per team on S47G01N49). This game, team total. |
| `fgt` | integer | Field goals made, two- and three-point (fg + threep). This game, team total. |
| `fgt_a` | integer | Field-goal attempts, two- and three-point (fgA + threepA). This game, team total. |
| `threep` | integer | Three-point field goals made (action 205). This game, team total. |
| `threep_a` | integer | Three-point attempts, made + missed (205 + 206). This game, team total. |
| `ft` | integer | Free throws made (action 203). This game, team total. |
| `ft_a` | integer | Free-throw attempts, made + missed (203 + 204). This game, team total. |
| `dk` | integer | Dunks made (action 207); included in `fg`. This game, team total. |
| `dk_a` | integer | Dunk attempts (made + missed, actions 207 + 208). This game, team total. |
| `rb` | integer | Total rebounds (offr + defr; team rebounds are `teamR`). This game, team total. |
| `offr` | integer | Offensive rebounds (action 209). This game, team total. |
| `defr` | integer | Defensive rebounds (action 210). This game, team total. |
| `ast` | integer | Assists (action 211). This game, team total. |
| `stl` | integer | Steals (action 212). This game, team total. |
| `bs` | integer | Blocked shots (action 213). This game, team total. |
| `to` | integer | Turnovers (action 214). This game, team total. |
| `foul` | integer | Personal fouls (wft + woft). This game, team total. |
| `wft` | integer | Fouls that awarded free throws (action 215). This game, team total. |
| `woft` | integer | Fouls that awarded no free throws (action 216). This game, team total. |
| `pp` | integer | Field goals made in the paint. This game, team total. |
| `pp_a` | integer | Field-goal attempts in the paint. This game, team total. |
| `pscore` | integer | Points in the paint (2 * pp). This game, team total. |
| `sast` | integer | Screen assists (action 226; tracked in recent seasons only, 0 before). This game, team total. |
| `dfl` | integer | Deflections (action 227; tracked in recent seasons only, 0 before). This game, team total. |
| `gd` | integer | Good-defense plays (action 221). This game, team total. |
| `idf` | integer | Illegal-defense violations (action 219; an early-KBL rule, so non-zero only in old seasons). This game, team total. |
| `fb` | integer | Fast breaks credited to the player (action 217 rows that name a player, as in 1997-98); 0 in recent seasons, where fast breaks are team rows (`tfb`). This game, team total. |
| `ef` | integer | Meaning unverified (0 in every game sample; non-zero in some season totals). This game, team total. |
| `tf` | integer | Probably technical fouls (unverified; 0 in most rows). This game, team total. |
| `inout` | integer | Times the player entered the game, the start included (checked on the 2025-26 sample game); a team row sums its players. This game, team total. |
| `inout1` | integer | Times the player was substituted out before the end (checked on the 2025-26 sample game); a team row sums its players. This game, team total. |
| `play_min` | integer | Minutes of playing time summed over the players (200 = 5 x 40 in regulation); add playSec. This game, team total. |
| `play_sec` | integer | Seconds part of the playing time (0-59), added to `playMin`. This game, team total. |
| `bf` | integer | Meaning unverified (probably bench fouls; 0 in every game sample). This game, team total. |
| `foulout` | integer | Players fouled out (unverified). This game, team total. |
| `team_r` | integer | Team rebounds (action 218). This game, team total. |
| `tfb` | integer | Team fast breaks (action 217). This game, team total. |
| `tto` | integer | Team turnovers (action 223). This game, team total. |
| `fb_score_cn` | integer | Fast-break points. This game, team total. |
| `turnover_score_cn` | integer | Points off turnovers. This game, team total. |
| `sec_chance_score_cn` | integer | Second-chance points. This game, team total. |
| `bench_score_cn` | integer | Bench points. This game, team total. |
| `max_conti_score_cn` | integer | Longest scoring run: the most consecutive points without an opponent score. This game, team total. |
| `max_lead_score_cn` | integer | Largest lead, in points. This game, team total. |
| `score_high_time_cn` | integer | Seconds the team led (the two teams' values sum to less than the 2,400-second game when it was tied or level at times). This game, team total. |
| `offrtg` | numeric | Offensive rating: points scored per 100 possessions (estimated). This game, team total. |
| `defrtg` | numeric | Defensive rating: points allowed per 100 possessions (estimated). This game, team total. |
| `netrtg` | numeric | Net rating: offrtg - defrtg. This game, team total. |
| `efg_rt` | numeric | Effective field-goal percentage, 0-100. This game, team total. |
| `ts_rt` | numeric | True-shooting percentage, 0-100. This game, team total. |
| `ast_rt` | numeric | Assist rate: share of teammates' field goals assisted while on court, 0-100. This game, team total. |
| `ast_to` | numeric | Assist-to-turnover ratio. This game, team total. |
| `ast_ratio` | numeric | Assist ratio: assists per 100 possessions used. This game, team total. |
| `oreb_rt` | numeric | Offensive-rebound percentage, 0-100. This game, team total. |
| `dreb_rt` | numeric | Defensive-rebound percentage, 0-100. This game, team total. |
| `reb_rt` | numeric | Total-rebound percentage, 0-100. This game, team total. |
| `pace` | numeric | Possessions per 40 minutes (estimated; a player row covers his minutes on court). This game, team total. |
| `tov_rt` | numeric | Turnover rate: turnovers per 100 possessions, 0-100. This game, team total. |
| `poss` | numeric | Possessions (estimated). This game, team total. |
| `minutes_played` | numeric | Playing time in minutes (play_min + play_sec / 60), derived. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_team_box-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score or shot chart, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_team_box-example}

```python
kbl_team_box(gmkey='S47G01N49')
```

_Last validated n/a._

## kbl_team_tracks

Each team's standing after every game of the season, one row per team and game.

**Endpoint URL:** `GET https://api.kbl.or.kr/league/rank/{season}/team-tracks`

**Valid URL:** [https://api.kbl.or.kr/league/rank/2025/team-tracks?division=kbl&gameCode=01](https://api.kbl.or.kr/league/rank/2025/team-tracks?division=kbl&gameCode=01)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the KBL seasonCode (47 = 2025-26). |
| `division` | `division` |  |  | `Y` | 'kbl' (the KBL itself, the default) or 'd_league' (the KBL D-League reserve league, seasonGrade 2, which has its own season codes; it starts with the 2009 summer edition). |
| `gameCode` | `game_code` |  |  | `Y` | Competition inside the season, two digits: '01' regular season (default; the first stage of a D-League edition), '03' playoffs, '04' championship final, '07' EASL, '08' D-League stage 2, '10' All-Star, '11'/'13' cups, '14' Open Match Day. Only '01' was probed live. |

### Returns {#kbl_team_tracks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `glkey` | character | Competition key S{seasonCode}G{gameCode} (e.g. S47G01 = 2025-26 regular season): the id of the season routes. |
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `team_name` | character | Team short name in English (e.g. 'KT'). |
| `game_number` | integer | 1-based game number in the team's season: the n-th entry of the team's rank track. |
| `rank` | integer | Standing (1 = first): after the game_number-th game on the rank track, the current rank on the standings routes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kbl_team_tracks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a final game with no box score or shot chart, no playoff bracket, no schedule rows in a window). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: any non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a game that is not final, or a body for another game, season or window than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown division or game_code, or bad schedule dates (raised before any request). |

### Example {#kbl_team_tracks-example}

```python
kbl_team_tracks(season=2025)
```

_Last validated n/a._
