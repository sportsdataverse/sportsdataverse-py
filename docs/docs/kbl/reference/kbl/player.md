---
title: "KBL — KBL API (api.kbl.or.kr, Korea) — Player"
sidebar_label: "Player"
sidebar_position: 1
description: "KBL — KBL API (api.kbl.or.kr, Korea) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# KBL — KBL API (api.kbl.or.kr, Korea) — Player

## kbl_player_box

Player box score of a final game with the vendor's advanced rates, one row per player of both teams.

**Endpoint URL:** `GET https://api.kbl.or.kr/match/{gmkey}/player-stat`

**Valid URL:** [https://api.kbl.or.kr/match/S47G01N49/player-stat](https://api.kbl.or.kr/match/S47G01N49/player-stat)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gmkey` | `gmkey` |  | `Y` |  | Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError. |

### Returns {#kbl_player_box-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gmkey` | character | Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame. |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `game_no` | integer | Game number inside the competition (glkey): the N part of the gmkey. |
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `home_away` | character | Side (string): '1' home, '2' away; '3' on both teams' rows in the 1997-98 sample, so join on `player.tcode` instead. |
| `started` | integer | 1 when the player started the game, else 0. |
| `pcode` | character | KBL player code (6-digit string). Joins to `pcode` on every route and to `playerNo` in /league/round-mvp. |
| `player_name` | character | Player name in Korean (foreign players in Hangul transliteration). |
| `player_name_en` | character | Player name in English / romanized (sometimes Hangul for former players, e.g. 'CJ 레슬리'). |
| `team_name` | character | Team short name (Korean, e.g. '창원 LG' = Changwon LG), as it was at the time. English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `back_num` | character | Jersey number (string). |
| `pos` | character | Position code (string): C center, FD forward, GD guard, FC forward-center, GF guard-forward; table in the returns md. |
| `player_flag` | character | Player type (string): '0' domestic, '1' foreign import, '2' Asian-quota player. |
| `score` | integer | Points. This game. |
| `fg` | integer | Two-point field goals made, dunks included: 201 + 207 rows (score = 2*fg + 3*threep + ft holds on every sampled box). This game. |
| `fg_a` | integer | Two-point field-goal attempts, made + missed, dunks included: 201 + 202 + 207 + 208 rows (checked per team on S47G01N49). This game. |
| `fgt` | integer | Field goals made, two- and three-point (fg + threep). This game. |
| `fgt_a` | integer | Field-goal attempts, two- and three-point (fgA + threepA). This game. |
| `threep` | integer | Three-point field goals made (action 205). This game. |
| `threep_a` | integer | Three-point attempts, made + missed (205 + 206). This game. |
| `ft` | integer | Free throws made (action 203). This game. |
| `ft_a` | integer | Free-throw attempts, made + missed (203 + 204). This game. |
| `dk` | integer | Dunks made (action 207); included in `fg`. This game. |
| `dk_a` | integer | Dunk attempts (made + missed, actions 207 + 208). This game. |
| `rb` | integer | Total rebounds (offr + defr; team rebounds are `teamR`). This game. |
| `offr` | integer | Offensive rebounds (action 209). This game. |
| `defr` | integer | Defensive rebounds (action 210). This game. |
| `ast` | integer | Assists (action 211). This game. |
| `stl` | integer | Steals (action 212). This game. |
| `bs` | integer | Blocked shots (action 213). This game. |
| `to` | integer | Turnovers (action 214). This game. |
| `foul` | integer | Personal fouls (wft + woft). This game. |
| `wft` | integer | Fouls that awarded free throws (action 215). This game. |
| `woft` | integer | Fouls that awarded no free throws (action 216). This game. |
| `pp` | integer | Field goals made in the paint. This game. |
| `pp_a` | integer | Field-goal attempts in the paint. This game. |
| `pscore` | integer | Points in the paint (2 * pp). This game. |
| `sast` | integer | Screen assists (action 226; tracked in recent seasons only, 0 before). This game. |
| `dfl` | integer | Deflections (action 227; tracked in recent seasons only, 0 before). This game. |
| `gd` | integer | Good-defense plays (action 221). This game. |
| `idf` | integer | Illegal-defense violations (action 219; an early-KBL rule, so non-zero only in old seasons). This game. |
| `fb` | integer | Fast breaks credited to the player (action 217 rows that name a player, as in 1997-98); 0 in recent seasons, where fast breaks are team rows (`tfb`). This game. |
| `ef` | integer | Meaning unverified (0 in every game sample; non-zero in some season totals). This game. |
| `tf` | integer | Probably technical fouls (unverified; 0 in most rows). This game. |
| `inout` | integer | Times the player entered the game, the start included (checked on the 2025-26 sample game); a team row sums its players. This game. |
| `inout1` | integer | Times the player was substituted out before the end (checked on the 2025-26 sample game); a team row sums its players. This game. |
| `play_min` | integer | Minutes of playing time; add `playSec` seconds. This game. |
| `play_sec` | integer | Seconds part of the playing time (0-59), added to `playMin`. This game. |
| `fo` | integer | Fouls drawn (the player rows of a team sum to the opponent's `foul`, checked on the 2025-26 sample game). This game. |
| `margin_cn` | integer | Plus-minus: the point margin while the player was on court. This game. |
| `offrtg` | numeric | Offensive rating: points scored per 100 possessions (estimated). This game. |
| `defrtg` | numeric | Defensive rating: points allowed per 100 possessions (estimated). This game. |
| `netrtg` | numeric | Net rating: offrtg - defrtg. This game. |
| `efg_rt` | numeric | Effective field-goal percentage, 0-100. This game. |
| `ts_rt` | numeric | True-shooting percentage, 0-100. This game. |
| `ast_rt` | numeric | Assist rate: share of teammates' field goals assisted while on court, 0-100. This game. |
| `ast_to` | numeric | Assist-to-turnover ratio. This game. |
| `ast_ratio` | numeric | Assist ratio: assists per 100 possessions used. This game. |
| `oreb_rt` | numeric | Offensive-rebound percentage, 0-100. This game. |
| `dreb_rt` | numeric | Defensive-rebound percentage, 0-100. This game. |
| `reb_rt` | numeric | Total-rebound percentage, 0-100. This game. |
| `pace` | numeric | Possessions per 40 minutes (estimated; a player row covers his minutes on court). This game. |
| `tov_rt` | numeric | Turnover rate: turnovers per 100 possessions, 0-100. This game. |
| `poss` | numeric | Possessions (estimated). This game. |
| `per` | numeric | Player efficiency rating (KBL's formula; scale unverified). This game. |
| `pie` | numeric | Player impact estimate: share of the game's positive events, as a percentage. This game. |
| `usg_rt` | numeric | Usage rate: share of team possessions used while on court, 0-100. This game. |
| `to_ratio` | numeric | Turnover ratio: turnovers per 100 possessions used. This game. |
| `minutes_played` | numeric | Playing time in minutes (play_min + play_sec / 60), derived. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kbl_player_box-example}

```python
kbl_player_box(gmkey='S47G01N49')
```

_Last validated n/a._

## kbl_player_totals

Season player totals of one competition (every player with at least one game).

**Endpoint URL:** `GET https://api.kbl.or.kr/leagues/{season}/stats/players`

**Valid URL:** [https://api.kbl.or.kr/leagues/2025/stats/players?division=kbl&gameCode=01](https://api.kbl.or.kr/leagues/2025/stats/players?division=kbl&gameCode=01)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the KBL seasonCode (47 = 2025-26). |
| `division` | `division` |  |  | `Y` | 'kbl' (the KBL itself, the default) or 'd_league' (the KBL D-League reserve league, seasonGrade 2, which has its own season codes; it starts with the 2009 summer edition). |
| `gameCode` | `game_code` |  |  | `Y` | Competition inside the season, two digits: '01' regular season (default; the first stage of a D-League edition), '03' playoffs, '04' championship final, '07' EASL, '08' D-League stage 2, '10' All-Star, '11'/'13' cups, '14' Open Match Day. Only '01' was probed live. |

### Returns {#kbl_player_totals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS). |
| `season_code` | integer | KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League). |
| `game_code` | character | Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day. |
| `glkey` | character | Competition key S{seasonCode}G{gameCode} (e.g. S47G01 = 2025-26 regular season): the id of the season routes. |
| `pcode` | character | KBL player code (six-digit string). |
| `player_name` | character | Player name in Korean (foreign players in Hangul transliteration). |
| `player_name_en` | character | Player name in English / romanized (sometimes Hangul for former players, e.g. 'CJ 레슬리'). |
| `team_code` | character | KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `team_name` | character | Team short name (Korean, e.g. '창원 LG' = Changwon LG), as it was at the time. English names: /common/teamList `teamNameEn` / `teamNameFullEn`, joined on the team code. |
| `back_num` | character | Jersey number (string). |
| `pos` | character | Position code (string): C center, FD forward, GD guard, FC forward-center, GF guard-forward; table in the returns md. |
| `player_flag` | character | Player type (string): '0' domestic, '1' foreign import, '2' Asian-quota player. |
| `game_count` | integer | Games played (this season or competition). |
| `start_count` | integer | Meaning unverified: up to 4 x gameCount in every sampled row, so probably quarters started. |
| `score` | integer | Points. Season total for the competition (glkey). |
| `fg` | integer | Two-point field goals made, dunks included: 201 + 207 rows (score = 2*fg + 3*threep + ft holds on every sampled box). Season total for the competition (glkey). |
| `fg_a` | integer | Two-point field-goal attempts, made + missed, dunks included: 201 + 202 + 207 + 208 rows (checked per team on S47G01N49). Season total for the competition (glkey). |
| `fgt` | integer | Field goals made, two- and three-point (fg + threep). Season total for the competition (glkey). |
| `fgt_a` | integer | Field-goal attempts, two- and three-point (fgA + threepA). Season total for the competition (glkey). |
| `threep` | integer | Three-point field goals made (action 205). Season total for the competition (glkey). |
| `threep_a` | integer | Three-point attempts, made + missed (205 + 206). Season total for the competition (glkey). |
| `ft` | integer | Free throws made (action 203). Season total for the competition (glkey). |
| `ft_a` | integer | Free-throw attempts, made + missed (203 + 204). Season total for the competition (glkey). |
| `dk` | integer | Dunks made (action 207); included in `fg`. Season total for the competition (glkey). |
| `dk_a` | integer | Dunk attempts (made + missed, actions 207 + 208). Season total for the competition (glkey). |
| `rb` | integer | Total rebounds (offr + defr; team rebounds are `teamR`). Season total for the competition (glkey). |
| `offr` | integer | Offensive rebounds (action 209). Season total for the competition (glkey). |
| `defr` | integer | Defensive rebounds (action 210). Season total for the competition (glkey). |
| `ast` | integer | Assists (action 211). Season total for the competition (glkey). |
| `stl` | integer | Steals (action 212). Season total for the competition (glkey). |
| `bs` | integer | Blocked shots (action 213). Season total for the competition (glkey). |
| `to` | integer | Turnovers (action 214). Season total for the competition (glkey). |
| `foul` | integer | Personal fouls (wft + woft). Season total for the competition (glkey). |
| `wft` | integer | Fouls that awarded free throws (action 215). Season total for the competition (glkey). |
| `woft` | integer | Fouls that awarded no free throws (action 216). Season total for the competition (glkey). |
| `pp` | integer | Field goals made in the paint. Season total for the competition (glkey). |
| `pp_a` | integer | Field-goal attempts in the paint. Season total for the competition (glkey). |
| `pscore` | integer | Points in the paint (2 * pp). Season total for the competition (glkey). |
| `sast` | integer | Screen assists (action 226; tracked in recent seasons only, 0 before). Season total for the competition (glkey). |
| `dfl` | integer | Deflections (action 227; tracked in recent seasons only, 0 before). Season total for the competition (glkey). |
| `gd` | integer | Good-defense plays (action 221). Season total for the competition (glkey). |
| `idf` | integer | Illegal-defense violations (action 219; an early-KBL rule, so non-zero only in old seasons). Season total for the competition (glkey). |
| `fb` | integer | Fast breaks credited to the player (action 217 rows that name a player, as in 1997-98); 0 in recent seasons, where fast breaks are team rows (`tfb`). Season total for the competition (glkey). |
| `ef` | integer | Meaning unverified (0 in every game sample; non-zero in some season totals). Season total for the competition (glkey). |
| `tf` | integer | Probably technical fouls (unverified; 0 in most rows). Season total for the competition (glkey). |
| `inout` | integer | Times the player entered the game, the start included (checked on the 2025-26 sample game); a team row sums its players. Season total for the competition (glkey). |
| `inout1` | integer | Times the player was substituted out before the end (checked on the 2025-26 sample game); a team row sums its players. Season total for the competition (glkey). |
| `play_min` | integer | Minutes of playing time; add `playSec` seconds. Season total for the competition (glkey). |
| `play_sec` | integer | Seconds part of the playing time (0-59), added to `playMin`. Season total for the competition (glkey). |
| `foulout` | integer | Meaning unverified: values exceed games played (70 for a 40-game player with 74 fouls); possibly fouls drawn, as this route has no `fo`. |
| `minutes_played` | numeric | Playing time in minutes (play_min + play_sec / 60), derived. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kbl_player_totals-example}

```python
kbl_player_totals(season=2025)
```

_Last validated n/a._

## kbl_players

The site's player directory (current and former players) with their current or last team.

**Endpoint URL:** `GET https://api.kbl.or.kr/players`

**Valid URL:** [https://api.kbl.or.kr/players](https://api.kbl.or.kr/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kbl_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `pcode` | character | KBL player code (6-digit string). Joins to `pcode` on every route and to `playerNo` in /league/round-mvp. |
| `player_name` | character | Player name in Korean (foreign players in Hangul transliteration). |
| `player_name_en` | character | Player name in English / romanized (sometimes Hangul for former players, e.g. 'CJ 레슬리'). |
| `team_code` | character | Team code of the player's current or last team. KBL team code (2-digit string, e.g. '50' = Changwon LG). Joins to /common/teamList `teamCode`, game `tcodeH`/`tcodeA`, `t` in text-cast. Not stable across ownership changes: the Anyang club is '40' (SBS) in 1997-98 and '70' today. D-League sides have their own codes (`teamCodeD`). |
| `team_name` | character | Short name (Korean) of the player's current or last team. |
| `team_name_en` | character | Current team short name in English (e.g. 'JUNG KWAN JANG'). |
| `back_num` | character | Jersey number (string). |
| `pos` | character | Position code (string): C center, FD forward, GD guard, FC forward-center, GF guard-forward; table in the returns md. |
| `player_flag` | character | Player type (string): '0' domestic, '1' foreign import, '2' Asian-quota player. |
| `last_career` | character | Meaning unverified: a season-code-like number, 'A' or ''. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#kbl_players-example}

```python
kbl_players()
```

_Last validated n/a._
