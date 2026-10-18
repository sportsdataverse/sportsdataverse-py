---
title: "WJBL — W League API (api.wjbl.01core.app, Japan women) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "WJBL — W League API (api.wjbl.01core.app, Japan women) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WJBL — W League API (api.wjbl.01core.app, Japan women) — Team

## wjbl_team_box_score

Team line and totals of each side of a final game's box score, with the shooting percentages.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/boxscore`

**Valid URL:** [https://api.wjbl.01core.app/api/boxscore?play_schedule_id=7987](https://api.wjbl.01core.app/api/boxscore?play_schedule_id=7987)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `play_schedule_id` | `play_schedule_id` |  | `Y` |  | Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. 7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final. |

### Returns {#wjbl_team_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `play_schedule_id` | integer | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `team_side` | character | Which side of the game the row belongs to: home or away (the API's home / away blocks). |
| `team_id` | integer | Franchise id team_id of the side (stable across seasons and sponsor renames; 13 = Toyota Antelopes). |
| `row_type` | character | Which line of the side: team (stats credited to the team itself) or total (the whole side). |
| `pts` | integer | Points scored by the side (the sum of its players' points). |
| `p2m` | integer | Two-point field goals made. |
| `p2a` | integer | Two-point field goals attempted. |
| `p3m` | integer | Three-point field goals made. |
| `p3a` | integer | Three-point field goals attempted. |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throws attempted. |
| `f` | integer | Personal fouls committed. |
| `off` | integer | Offensive rebounds. |
| `def` | integer | Defensive rebounds. |
| `tot` | integer | Total rebounds (offensive plus defensive). |
| `ast` | integer | Assists credited to the side. |
| `stl` | integer | Steals credited to the side. |
| `blk` | integer | Blocked shots credited to the side. |
| `to` | integer | Turnovers committed by the side. |
| `min` | character | Minutes: summed minutes 'mmm:ss' on the total row ('200:00' = 5 x 40:00), always '00:00' on the team row. |
| `min_seconds` | integer | Minutes played in whole seconds, from the 'mm:ss' clock string min (null when it does not parse). |
| `p2p` | numeric | Two-point field-goal percentage 0-100 (a string on the wire); total row only. |
| `p3p` | numeric | Three-point field-goal percentage 0-100 (a string on the wire); total row only. |
| `ftp` | numeric | Free-throw percentage 0-100 (a string on the wire); total row only. |
| `fgp` | numeric | Field-goal percentage 0-100, two- and three-pointers together (a string on the wire); total row only. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_team_box_score-example}

```python
wjbl_team_box_score(play_schedule_id=7987)
```

_Last validated n/a._

## wjbl_team_period_stats

Team stat summary per period, half and game (2018-19 on): percentages, rebounds, assists, turnovers, lead changes.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/pbp/team-stats`

**Valid URL:** [https://api.wjbl.01core.app/api/pbp/team-stats?play_schedule_id=7987](https://api.wjbl.01core.app/api/pbp/team-stats?play_schedule_id=7987)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `play_schedule_id` | `play_schedule_id` |  | `Y` |  | Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. 7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final. |

### Returns {#wjbl_team_period_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `play_schedule_id` | integer | Game id play_schedule_id (key space: W League games, ONE global sequence across every league_id and season, e.g. 4243 = 2013-14 final, 7987 = 2025-26 final). The play_schedule_id parameter of every game route and of the site's /schedule_result/detail/*?play_schedule_id= pages. |
| `home_team_id` | integer | Home team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `away_team_id` | integer | Away team: Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `period_category` | integer | The period category again, as an integer (see periods). |
| `period_label` | character | Readable period: Q1-Q4 quarters, OT1-OT10 overtimes (period categories 5-14), H1 / H2 halves (15 / 16), OT all overtimes together (17), GAME the whole game (18). |
| `home_ptr` | numeric | Home field-goal percentage (2PT + 3PT) in the period, 0-100, a NUMBER with up to one decimal (61.5; whole values come as integers). |
| `home_pt2r` | numeric | Home two-point percentage, 0-100, number. |
| `home_pt3r` | numeric | Home three-point percentage, 0-100, number. |
| `home_ftr` | numeric | Home free-throw percentage, 0-100, number (0 when no attempts). |
| `home_rebound_off` | integer | Home offensive rebounds in the period, the first number of the wire's 'off-def-total' string. |
| `home_rebound_def` | integer | Home defensive rebounds in the period, the second number of the wire's 'off-def-total' string. |
| `home_rebound_tot` | integer | Home total rebounds in the period, the third number of the wire's 'off-def-total' string. |
| `home_assist` | integer | Home assists (integer). |
| `home_turnovers` | integer | Home turnovers in the period, from the wire's counted string ('6本' = 6); the wire key is spelled home_trun_over. |
| `home_max_difference` | integer | Home team's largest lead in the period, in points; the wire key is spelled home_max_differenct. |
| `away_ptr` | numeric | Away field-goal percentage (2PT + 3PT) in the period, 0-100, a NUMBER with up to one decimal (61.5; whole values come as integers). |
| `away_pt2r` | numeric | Away two-point percentage, 0-100, number. |
| `away_pt3r` | numeric | Away three-point percentage, 0-100, number. |
| `away_ftr` | numeric | Away free-throw percentage, 0-100, number (0 when no attempts). |
| `away_rebound_off` | integer | Away offensive rebounds in the period, the first number of the wire's 'off-def-total' string. |
| `away_rebound_def` | integer | Away defensive rebounds in the period, the second number of the wire's 'off-def-total' string. |
| `away_rebound_tot` | integer | Away total rebounds in the period, the third number of the wire's 'off-def-total' string. |
| `away_assist` | integer | Away assists (integer). |
| `away_turnovers` | integer | Away turnovers in the period, from the wire's counted string ('6本' = 6); the wire key is spelled away_trun_over. |
| `away_max_difference` | integer | Away team's largest lead in the period, points (integer). |
| `lead_count` | integer | Lead changes in the period, from the wire's counted string ('0回' = 0 times). |
| `tie_count` | integer | Times the score was tied in the period, from the wire's counted string ('4回' = 4 times). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_team_period_stats-example}

```python
wjbl_team_period_stats(play_schedule_id=7987)
```

_Last validated n/a._

## wjbl_team_standings

League table of one or more season phases: rank and record of every team.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/team-standings`

**Valid URL:** [https://api.wjbl.01core.app/api/team-standings?season_ids=169](https://api.wjbl.01core.app/api/team-standings?season_ids=169)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_ids` | `season_ids` |  | `Y` |  | Season phase id or ids (season_id in wjbl_standing_seasons; a phase inside a league edition, not a year): an integer, a list or a comma string such as '169,170' (2025-26 regular season Premier and Future divisions). |

### Returns {#wjbl_team_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | integer | Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_title` | character | Phase title in Japanese: 'レギュラーシーズン' = regular season, 'プレーオフ　ファイナル' / 'プレーオフ／ファイナル' = playoff final, 'プレーオフ／セミファイナル' = playoff semifinal (the separator varies by era). |
| `league_name` | character | Division name, Japanese ('プレミア' = Premier). |
| `season_league_name` | character | Phase / division label in Japanese: 'プレミア' = Premier, 'フューチャー' = Future, 'グループステージ' = group stage, '2次ラウンド 下位リーグ' = second round, lower league; empty for a single-table phase. |
| `season_league_flg` | integer | Division layout flag: 1 = one table per division (Premier / Future), 2 = split by season_team_area (East / West, by the site's JS), 0 = cup / group stage. |
| `update_date` | character | As-of date in Japanese, 'YYYY年M月D日(曜)'. |
| `team_id` | integer | Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `season_team_name` | character | Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `season_team_short_name` | character | Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `team_entry_flg` | integer | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |
| `position` | integer | Rank of the team in the phase (a string on the wire, equal to the map key). |
| `season_team_area` | character | Unobserved: null in every sample, so no type and no meaning is claimed. (By the site's standings JS: 0 = East, 1 = West conference when season_league_flg = 2.) |
| `win_lost` | character | Record in Japanese, 'N勝M負' ('23勝5負' = 23 wins, 5 losses). |
| `wins` | integer | Wins in the record, parsed from win_lost ('23勝5負' = 23 wins). |
| `losses` | integer | Losses in the record, parsed from win_lost ('23勝5負' = 5 losses). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_team_standings-example}

```python
wjbl_team_standings(season_ids='169')
```

_Last validated n/a._

## wjbl_team_ranking

League table with per-game team averages (points, shooting, rebounds, assists) of one or more season phases.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/team-ranking`

**Valid URL:** [https://api.wjbl.01core.app/api/team-ranking?season_ids=169%2C170](https://api.wjbl.01core.app/api/team-ranking?season_ids=169%2C170)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_ids` | `season_ids` |  | `Y` |  | Season phase id or ids (season_id in wjbl_standing_seasons; a phase inside a league edition, not a year): an integer, a list or a comma string such as '169,170' (2025-26 regular season Premier and Future divisions). |

### Returns {#wjbl_team_ranking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | integer | Competition-edition id league_id (key space: W League competitions; sequential, ONE id per season of the league AND per all-star game, cup or camp: 45 = '25-26 Wリーグ', 47 = '26-27', 20 = '13-14', 46 = All-Star 2025-26). The league_id parameter of /play-schedules and /play-schedule-months; enumerate with /leagues. The sdv season is the END year of league_title (END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000). |
| `league_title` | character | Competition-edition title in Japanese: '25-26 Wリーグ' = 2025-26 W League (sdv END year 2026; END = 2000 + the second YY: '25-26' -> 2026, '99-00' -> 2000); also 'Wリーグオールスター2025-26' = All-Star game, 'オータムカップ2021' = Autumn Cup, 'サマーキャンプ2019' = Summer Camp, 'WリーグSUPERGAMES'. |
| `season` | integer | Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, cup or camp. |
| `season_id` | integer | Season-phase id season_id (key space: phases WITHIN a league_id, e.g. 169 = 25-26 regular season Premier, 170 = 25-26 regular season Future, 176 = 25-26 playoff final). Not a year: one league_id has several season_ids. The season_ids parameter of /team-standings and /team-ranking and the season_id of /team/roster. |
| `season_title` | character | Phase title in Japanese: 'レギュラーシーズン' = regular season, 'プレーオフ　ファイナル' / 'プレーオフ／ファイナル' = playoff final, 'プレーオフ／セミファイナル' = playoff semifinal (the separator varies by era). |
| `season_full_name` | character | league_title + phase (+ division) in Japanese: '25-26 Wリーグ レギュラーシーズン プレミア' = 2025-26 regular season, Premier division ('フューチャー' = Future division, from 2024-25). |
| `season_league_name` | character | Phase / division label in Japanese: 'プレミア' = Premier, 'フューチャー' = Future, 'グループステージ' = group stage, '2次ラウンド 下位リーグ' = second round, lower league; empty for a single-table phase. |
| `season_league_flg` | integer | Division layout flag: 1 = one table per division (Premier / Future), 2 = split by season_team_area (East / West, by the site's JS), 0 = cup / group stage. |
| `season_league_img` | character | Division badge image path relative to https://www.wjbl.org. |
| `season_tournament_text` | character | Tournament text (HTML); empty in every sample. |
| `update_info` | character | As-of note in Japanese, 'YYYY年M月D日現在' ('2026年2月8日現在' = as of 8 Feb 2026). |
| `team_id` | integer | Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `season_team_name` | character | Team name as registered for the season, in Japanese (company + nickname: 'トヨタ自動車 アンテロープス' = Toyota Antelopes). |
| `season_team_short_name` | character | Short team name for that season, Japanese ('トヨタ自動車' = Toyota). |
| `team_entry_flg` | integer | Official-league participant flag (公式戦参加フラグ): 1 = a W League member team (the site links its roster), 0 = not (guest or all-star side). Integer. |
| `position` | integer | Rank of the team in the phase (a string on the wire). |
| `season_team_area` | character | Unobserved: null in every sample, so no type and no meaning is claimed. (By the site's standings JS: 0 = East, 1 = West conference when season_league_flg = 2.) |
| `win` | integer | Wins (勝), integer. |
| `lost` | integer | Losses (負), integer. |
| `winning_rate` | numeric | Winning percentage as a FRACTION 0-1 with three decimals, a string ('0.821'); parsed to a number. |
| `ppg` | numeric | Points per game, a string with two decimals; parsed to a number. |
| `opp` | numeric | Opponent points per game (OPP), a string with two decimals; parsed to a number. |
| `fg` | numeric | Field-goal percentage 0-100 with a '%' sign, a string ('41.99%'); parsed to a number. |
| `opp_fg` | numeric | Opponents' field-goal percentage 0-100 with '%', a string; parsed to a number. |
| `p3pg` | numeric | Three-pointers made per game (3PPG), a string; parsed to a number. |
| `rpg` | numeric | Rebounds per game, a string; parsed to a number. |
| `apg` | numeric | Assists per game, a string; parsed to a number. |
| `spg` | numeric | Steals per game, a string; parsed to a number. |
| `bpg` | numeric | Blocks per game, a string; parsed to a number. |
| `tpg` | numeric | Turnovers per game, a string; parsed to a number. |
| `gpd` | numeric | Point differential summed over the season (GPD), NOT per game: (ppg - opp) x games ('193.00'), a string; parsed to a number. |
| `win_point` | character | Unobserved: null in every sample, so no type and no meaning is claimed. (By the site's JS: league points, a column shown only when filled.) |
| `unsatisfied` | character | Unobserved: null in every sample, so no type and no meaning is claimed. (By the site's JS: a column shown only when filled; meaning unverified.) |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_team_ranking-example}

```python
wjbl_team_ranking(season_ids='169,170')
```

_Last validated n/a._

## wjbl_teams

Every franchise (team_id) the site knows, all-star sides included.

**Endpoint URL:** `GET https://api.wjbl.01core.app/api/teams/base`

**Valid URL:** [https://api.wjbl.01core.app/api/teams/base](https://api.wjbl.01core.app/api/teams/base)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#wjbl_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Franchise id team_id (key space: W League franchises, STABLE across seasons and sponsor renames: 13 = Toyota Antelopes, 12 = Denso Iris, 8 = ENEOS Sunflowers; all-star sides have their own ids, e.g. 591). The team_id of /team/roster and of the site's /team/roster/?team_id=. Not season_team_id. |
| `team_name` | character | Franchise name, Japanese or English ('富士通レッドウェーブ' = Fujitsu Red Wave; 'TEAM シリウス' = all-star side Team Sirius). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON (a bare list as `{'items': [...]}`) plus `_request` (the ids asked for, with `season` the END year).

### Example {#wjbl_teams-example}

```python
wjbl_teams()
```

_Last validated n/a._
