---
title: MBB — Bart Torvik T-Rank (barttorvik.com)
sidebar_label: Bart Torvik T-Rank (barttorvik.com)
description: "MBB — Bart Torvik T-Rank (barttorvik.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# MBB — Bart Torvik T-Rank (barttorvik.com)

`sportsdataverse.mbb` — 5 endpoints.

## torvik_ratings

GET /{year}_team_results.csv — men's T-Rank team ratings (adjoe/adjde/barthag, one row per team; the team/conf pair feeds the MBB crosswalk).

**Endpoint URL:** `GET https://barttorvik.com/{year}_team_results.csv`

**Valid URL:** [https://barttorvik.com/2025_team_results.csv](https://barttorvik.com/2025_team_results.csv)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#torvik_ratings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | integer | T-Rank position (overall barthag rank). |
| `team` | character | Torvik team name (the crosswalk join key). |
| `conf` | character | Torvik conference abbreviation (e.g. B12, ACC, BE). |
| `record` | character | Overall win-loss record to date. |
| `adjoe` | numeric | Adjusted offensive efficiency (points per 100 possessions vs an average defense). |
| `oe_rank` | integer | National rank of adjusted offensive efficiency. |
| `adjde` | numeric | Adjusted defensive efficiency (points allowed per 100 possessions vs an average offense). |
| `de_rank` | integer | National rank of adjusted defensive efficiency. |
| `barthag` | numeric | Torvik power rating: win probability vs an average team on a neutral floor. |
| `rank_2` | integer | Barthag rank repeated as shipped in the source CSV (duplicate of rank). |
| `proj_w` | numeric | Projected full-season wins. |
| `proj_l` | numeric | Projected full-season losses. |
| `pro_con_w` | numeric | Projected conference wins. |
| `pro_con_l` | numeric | Projected conference losses. |
| `con_rec` | character | Conference win-loss record to date. |
| `sos` | numeric | Strength of schedule faced to date. |
| `ncsos` | numeric | Non-conference strength of schedule. |
| `consos` | numeric | Conference strength of schedule. |
| `proj_sos` | numeric | Projected full-season strength of schedule. |
| `proj_noncon_sos` | numeric | Projected non-conference strength of schedule. |
| `proj_con_sos` | numeric | Projected conference strength of schedule. |
| `elite_sos` | numeric | Elite strength of schedule (share of schedule vs elite opponents). |
| `elite_noncon_sos` | numeric | Elite non-conference strength of schedule. |
| `opp_oe` | numeric | Average opponent offensive efficiency. |
| `opp_de` | numeric | Average opponent defensive efficiency. |
| `opp_proj_oe` | numeric | Projected average opponent offensive efficiency. |
| `opp_proj_de` | numeric | Projected average opponent defensive efficiency. |
| `con_adj_oe` | numeric | Adjusted offensive efficiency in conference games only. |
| `con_adj_de` | numeric | Adjusted defensive efficiency in conference games only. |
| `qual_o` | numeric | Offensive efficiency vs quality (top-tier) opponents. |
| `qual_d` | numeric | Defensive efficiency vs quality (top-tier) opponents. |
| `qual_barthag` | numeric | Barthag computed from the quality-opponent efficiency splits. |
| `qual_games` | numeric | Number of quality games underlying the qual_* splits. |
| `fun` | numeric | Torvik's FUN style/entertainment index. |
| `con_pf` | numeric | Points scored in conference play. |
| `con_pa` | numeric | Points allowed in conference play. |
| `con_poss` | numeric | Possessions played in conference play. |
| `con_oe` | numeric | Raw offensive efficiency in conference play. |
| `con_de` | numeric | Raw defensive efficiency in conference play. |
| `con_sos_remain` | numeric | Strength of the remaining conference schedule. |
| `conf_win_percent` | numeric | Conference win percentage. |
| `wab` | numeric | Wins above bubble (resume quality vs a bubble-level team). |
| `wab_rk` | integer | National rank of wins above bubble. |
| `fun_rk` | integer | National rank of the FUN index. |
| `adjt` | numeric | Adjusted tempo (possessions per 40 minutes). |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Example {#torvik_ratings-example}

```python
torvik_ratings(year=2025)
```

_Last validated n/a._

## torvik_team_factors

GET /{year}_fffinal.csv — men's four-factors splits (eFG%/FTR/OR%/TO% offense + defense, with per-stat ranks).

**Endpoint URL:** `GET https://barttorvik.com/{year}_fffinal.csv`

**Valid URL:** [https://barttorvik.com/2025_fffinal.csv](https://barttorvik.com/2025_fffinal.csv)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#torvik_team_factors-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_name` | character | Torvik team name. |
| `e_fg_percent` | numeric | Effective field-goal percentage on offense. |
| `rk` | integer | National rank of offensive eFG%. |
| `e_fg_percent_def` | numeric | Effective field-goal percentage allowed on defense. |
| `rk_2` | integer | National rank of defensive eFG% allowed. |
| `ftr` | numeric | Free-throw rate on offense (FTA per FGA). |
| `rk_3` | integer | National rank of offensive free-throw rate. |
| `ftr_def` | numeric | Free-throw rate allowed on defense. |
| `rk_4` | integer | National rank of defensive free-throw rate allowed. |
| `or_percent` | numeric | Offensive rebound percentage. |
| `rk_5` | integer | National rank of offensive rebound percentage. |
| `dr_percent` | numeric | Defensive rebound percentage. |
| `rk_6` | integer | National rank of defensive rebound percentage. |
| `to_percent` | numeric | Turnover percentage on offense. |
| `rk_7` | integer | National rank of offensive turnover percentage. |
| `to_percent_def` | numeric | Turnover percentage forced on defense. |
| `rk_8` | integer | National rank of defensive turnover percentage forced. |
| `x3p_percent` | numeric | Three-point percentage on offense. |
| `rk_9` | integer | National rank of offensive three-point percentage. |
| `x3p_d_percent` | numeric | Three-point percentage allowed on defense. |
| `rk_10` | integer | National rank of three-point percentage allowed. |
| `x2p_percent` | numeric | Two-point percentage on offense. |
| `rk_11` | integer | National rank of offensive two-point percentage. |
| `x2p_percent_d` | numeric | Two-point percentage allowed on defense. |
| `rk_12` | integer | National rank of two-point percentage allowed. |
| `ft_percent` | numeric | Free-throw percentage. |
| `rk_13` | integer | National rank of free-throw percentage. |
| `ft_percent_d` | numeric | Opponent free-throw percentage. |
| `rk_14` | integer | National rank of opponent free-throw percentage. |
| `x3p_rate` | numeric | Three-point attempt rate on offense (3PA per FGA). |
| `rk_15` | integer | National rank of offensive three-point attempt rate. |
| `x3p_rate_d` | numeric | Three-point attempt rate allowed on defense. |
| `rk_16` | integer | National rank of three-point attempt rate allowed. |
| `arate` | numeric | Assist rate on offense (assists per made field goal). |
| `rk_17` | integer | National rank of offensive assist rate. |
| `arate_d` | numeric | Assist rate allowed on defense. |
| `rk_18` | integer | National rank of assist rate allowed. |
| `unnamed` | numeric | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_2` | integer | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_3` | numeric | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_4` | integer | Headerless trailing column shipped in the source CSV (undocumented upstream). |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Example {#torvik_team_factors-example}

```python
torvik_team_factors(year=2025)
```

_Last validated n/a._

## torvik_game_stats

GET /getgamestats.php?year=&json=1 — men's per-team-game efficiency and four-factors log (one row per team-game, 31 positional fields).

**Endpoint URL:** `GET https://barttorvik.com/getgamestats.php`

**Valid URL:** [https://barttorvik.com/getgamestats.php?year=2025](https://barttorvik.com/getgamestats.php?year=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | 4-digit season ending year (2025 = the 2024-25 season). |
| `json` | `json` |  |  | `Y` | Response format switch; leave at 1 (the parser expects the headerless JSON array). |

### Returns {#torvik_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `date` | character | Game date as Torvik prints it (m/d/yy). |
| `type` | integer | Torvik game-type code (observed 0-3; 1 is the most common); Torvik does not document the codes. |
| `team` | character | Torvik team name for the row's team (the perspective of every other column). |
| `conf` | character | Torvik conference abbreviation of the row's team (e.g. B12, ACC). |
| `opp` | character | Torvik team name of the opponent. |
| `venue` | character | Venue from the row team's view: H home, A away, N neutral. |
| `result` | character | Result from the row team's view, e.g. 'L, 88-57' (W/L, team points, opponent points). |
| `adj_oe` | numeric | Row team's pre-game adjusted offensive efficiency (points per 100 possessions). |
| `adj_de` | numeric | Row team's pre-game adjusted defensive efficiency (points allowed per 100 possessions). |
| `oe` | numeric | Row team's raw offensive efficiency in this game (points per 100 possessions). |
| `off_efg` | numeric | Row team's offensive effective field-goal percentage in this game. |
| `off_to` | numeric | Row team's offensive turnover percentage in this game. |
| `off_or` | numeric | Row team's offensive rebound percentage in this game. |
| `off_ftr` | numeric | Row team's free-throw rate on offense in this game. |
| `de` | numeric | Row team's raw defensive efficiency in this game (points allowed per 100 possessions). |
| `def_efg` | numeric | Opponent's effective field-goal percentage against the row team. |
| `def_to` | numeric | Opponent's turnover percentage against the row team. |
| `def_or` | numeric | Opponent's offensive rebound percentage against the row team. |
| `def_ftr` | numeric | Opponent's free-throw rate against the row team. |
| `game_score` | numeric | Torvik's Gscore, a 0-100 single-game performance rating for the row team. |
| `opp_conf` | character | Torvik conference abbreviation of the opponent. |
| `quad` | integer | Torvik 'quad' field: observed values are only 1 and 2, tagging which side of the game the row is (each game appears once with each); not the NCAA quadrant. |
| `year` | integer | Season ending year (2025 = the 2024-25 season). |
| `tempo` | numeric | Possessions per 40 minutes in this game. |
| `muid` | character | Torvik matchup id: opponent-order-free concatenation of team names, date and venue; the same value appears in both teams' rows and in the schedule's muid. |
| `coach` | character | Head coach of the row team. |
| `opp_coach` | character | Head coach of the opponent. |
| `margin` | numeric | Signed margin from the row team's view (positive for a win, negative for a loss); Torvik ships it adjusted, so it differs from the raw score difference (-15.37 for an 88-57 loss). |
| `win_prob` | numeric | Torvik 'win_prob' field as shipped; it is not the row team's own pre-game win probability (a 31-point loser carries 0.92), so treat it as opaque. |
| `game_stats` | character | JSON-encoded array of raw per-game box-score counts exactly as Torvik ships it; the positional layout is Torvik's and undocumented. |
| `overtimes` | integer | Torvik 'overtimes' field as shipped; observed integers 0-13, which is too large for an overtime count, so treat it as opaque. |
| `game_date` | Date | date parsed to a Date (null when it does not parse). |

**`return_parsed=False`** — the decoded JSON response body (the positional-field array).

### Example {#torvik_game_stats-example}

```python
torvik_game_stats(year=2025)
```

_Last validated n/a._

## torvik_player_stats

GET /getadvstats.php?year=&csv=1 — men's player advanced stats (one row per player, 67 positional fields).

**Endpoint URL:** `GET https://barttorvik.com/getadvstats.php`

**Valid URL:** [https://barttorvik.com/getadvstats.php?year=2025](https://barttorvik.com/getadvstats.php?year=2025)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | 4-digit season ending year (2025 = the 2024-25 season). |
| `csv` | `csv` |  |  | `Y` | Response format switch; leave at 1 (the parser expects the headerless CSV). |

### Returns {#torvik_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_name` | character | Player name as Torvik lists it, 'First Last' (e.g. Robby Carmody). |
| `team` | character | Torvik team name the player is listed with (e.g. Le Moyne); matches the ratings team column. |
| `conf` | character | Torvik conference abbreviation. |
| `games` | integer | Games the player appeared in this season. |
| `min_pct` | numeric | Share of team minutes played, in percent. |
| `o_rtg` | numeric | Offensive rating (points produced per 100 individual possessions). |
| `usage` | numeric | Usage rate: share of team possessions used while on the floor, in percent. |
| `e_fg` | numeric | Effective field-goal percentage. |
| `ts_pct` | numeric | True shooting percentage. |
| `orb_pct` | numeric | Offensive rebound percentage. |
| `drb_pct` | numeric | Defensive rebound percentage. |
| `ast_pct` | numeric | Assist percentage. |
| `to_pct` | numeric | Turnover percentage. |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throws attempted. |
| `ft_pct` | numeric | Free-throw percentage as a fraction (0-1). |
| `two_pm` | numeric | Two-point field goals made. |
| `two_pa` | numeric | Two-point field goals attempted. |
| `two_p_pct` | numeric | Two-point field-goal percentage as a fraction (0-1). |
| `three_pm` | numeric | Three-point field goals made. |
| `three_pa` | numeric | Three-point field goals attempted. |
| `three_p_pct` | numeric | Three-point field-goal percentage as a fraction (0-1). |
| `blk_pct` | numeric | Block percentage. |
| `stl_pct` | numeric | Steal percentage. |
| `ftr` | numeric | Free-throw rate (FTA per FGA, in percent). |
| `class` | character | Class year (Fr, So, Jr, Sr). |
| `height` | character | Listed height as feet-inches, e.g. 6-4. |
| `number` | integer | Jersey number as listed (Int64). |
| `porpag` | numeric | Points over replacement per adjusted game (Torvik's PRPG!). |
| `adj_oe` | numeric | Player's adjusted offensive efficiency. |
| `pfr` | numeric | Personal fouls committed per 40 minutes. |
| `year` | integer | Season ending year (2025 = the 2024-25 season). |
| `player_id` | integer | Torvik numeric player id (Int64); the join key to other Torvik player pages. |
| `hometown` | character | Hometown as listed, e.g. 'Mars, PA'; blank for some players. |
| `rec_rank` | numeric | High-school recruiting rating; null for unranked players. |
| `ast_to` | numeric | Assist-to-turnover ratio. |
| `rim_made` | numeric | Made shots at the rim. |
| `rim_attempts` | numeric | Attempted shots at the rim. |
| `mid_made` | numeric | Made two-point jump shots (non-rim twos). |
| `mid_attempts` | numeric | Attempted two-point jump shots (non-rim twos). |
| `rim_pct` | numeric | Field-goal percentage at the rim as a fraction (0-1). |
| `mid_pct` | numeric | Field-goal percentage on non-rim twos as a fraction (0-1). |
| `dunks_made` | numeric | Dunks made over the season. |
| `dunks_attempts` | numeric | Dunks attempted. |
| `dunks_pct` | numeric | Dunk percentage as a fraction (0-1). |
| `pick` | numeric | NBA draft pick number; null for players not drafted. |
| `drtg` | numeric | Defensive rating (points allowed per 100 individual defensive possessions). |
| `adrtg` | numeric | Adjusted defensive rating. |
| `dporpag` | numeric | Defensive points over replacement per adjusted game. |
| `stops` | numeric | Defensive stops. |
| `bpm` | numeric | Box plus/minus. |
| `obpm` | numeric | Offensive box plus/minus. |
| `dbpm` | numeric | Defensive box plus/minus. |
| `gbpm` | numeric | Torvik's game-level box plus/minus. |
| `minutes` | numeric | Share of team minutes played, in percent (as on the Torvik player page). |
| `ogbpm` | numeric | Offensive game box plus/minus. |
| `dgbpm` | numeric | Defensive game box plus/minus. |
| `oreb` | numeric | Offensive rebounds per game. |
| `dreb` | numeric | Defensive rebounds per game. |
| `treb` | numeric | Total rebounds per game. |
| `ast` | numeric | Assists per game. |
| `stl` | numeric | Steals per game. |
| `blk` | numeric | Blocks per game. |
| `pts` | numeric | Points per game. |
| `role` | character | Torvik's position/role label, e.g. 'Combo G' or 'Scoring PG'. |
| `threat` | numeric | Torvik's scoring-threat rating. |
| `recruit_date` | character | Torvik's 'recruit_date' field; the values are ISO dates that look like birth dates, kept verbatim as text. |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Example {#torvik_player_stats-example}

```python
torvik_player_stats(year=2025)
```

_Last validated n/a._

## torvik_game_schedule

GET /{year}_super_sked.json — men's season schedule/results with T-Rank projections (one row per game, 55 positional fields).

**Endpoint URL:** `GET https://barttorvik.com/{year}_super_sked.json`

**Valid URL:** [https://barttorvik.com/2025_super_sked.json](https://barttorvik.com/2025_super_sked.json)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#torvik_game_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `muid` | character | Torvik matchup id; equals the game_stats muid. |
| `date` | character | Game date as Torvik prints it (m/d/yy). |
| `conmatch` | character | Conference matchup descriptor, e.g. 'D2 at Amer'. |
| `matchup` | character | Rank-annotated matchup text, e.g. '0 Northeastern St. at 243 Tulsa'. |
| `prediction` | character | Pre-game prediction text (favorite, margin, score, win probability). |
| `ttq` | character | Torvik Thrill Quotient (game excitement). |
| `conf` | character | Conference-game flag/code as Torvik ships it (observed 0 and 99). |
| `venue` | character | Venue / neutral-site flag as Torvik ships it (0 observed on home/away games). |
| `team1` | character | First-listed team name. |
| `t1oe` | character | team1 adjusted offensive efficiency going into the game. |
| `t1de` | character | team1 adjusted defensive efficiency going into the game. |
| `t1py` | character | team1 pre-game win probability (fraction 0-1). |
| `t1wp` | character | team1 win indicator as Torvik ships it (1 when team1 won). |
| `t1propt` | character | team1 projected points. |
| `team2` | character | Second-listed team name. |
| `t2oe` | character | team2 adjusted offensive efficiency going into the game. |
| `t2de` | character | team2 adjusted defensive efficiency going into the game. |
| `t2py` | character | team2 pre-game win probability (fraction 0-1). |
| `t2wp` | character | team2 win indicator as Torvik ships it (1 when team2 won). |
| `t2propt` | character | team2 projected points. |
| `tpro` | character | Projected tempo (possessions) for the game. |
| `t1qual` | character | Torvik quality marker for team1: the team name when set, blank otherwise. |
| `t2qual` | character | Torvik quality marker for team2: the team name when set, blank otherwise. |
| `gp` | character | Game-played flag (1 if completed). |
| `result` | character | Final result text from team1's view; blank until played. |
| `tempo` | character | Actual tempo (possessions per 40 minutes); blank until played. |
| `possessions` | character | Actual possessions; blank until played. |
| `t1pts` | character | team1 final points; blank until played. |
| `t2pts` | character | team2 final points; blank until played. |
| `winner` | character | Winning team name; blank until played. |
| `loser` | character | Losing team name; blank until played. |
| `t1adjt` | character | team1 adjusted tempo. |
| `t2adjt` | character | team2 adjusted tempo. |
| `t1adjo` | character | Game-level adjusted offensive efficiency for team1 as Torvik ships it (0 when unrated). |
| `t1adjd` | character | Game-level adjusted defensive efficiency for team1 as Torvik ships it (0 when unrated). |
| `t2adjo` | character | Game-level adjusted offensive efficiency for team2 as Torvik ships it (0 when unrated). |
| `t2adjd` | character | Game-level adjusted defensive efficiency for team2 as Torvik ships it (0 when unrated). |
| `gamevalue` | character | Torvik's single-game quality value for the matchup (0 for games between unrated teams). |
| `mismatch` | character | Torvik's mismatch rating (how lopsided the matchup is). |
| `blowout` | character | Torvik's blowout rating. |
| `t1elite` | character | Torvik elite-performance component for team1 (0-1). |
| `t2elite` | character | Torvik elite-performance component for team2 (0-1). |
| `ord_date` | character | Proleptic Gregorian ordinal of the game date (Python date.toordinal). |
| `t1ppp` | character | team1 points per possession in the game. |
| `t2ppp` | character | team2 points per possession in the game. |
| `gameppp` | character | Combined points per possession in the game. |
| `t1rk` | character | team1 T-Rank rank at game time. |
| `t2rk` | character | team2 T-Rank rank at game time. |
| `t1gs` | character | team1 Gscore as a fraction 0-1; '-' when unrated. |
| `t2gs` | character | team2 Gscore as a fraction 0-1; '-' when unrated. |
| `gamestats` | character | Per-game stat array collapsed to a ';'-joined string. |
| `overtimes` | character | Overtime field as Torvik ships it; blank on the captured games. |
| `t1fun` | character | Torvik fun/watchability component for team1 (0-1). |
| `t2fun` | character | Torvik fun/watchability component for team2 (0-1). |
| `results` | character | Results blob as Torvik ships it (text). |
| `game_date` | Date | date parsed to a Date (null when it does not parse). |
| `year` | integer | Season ending year inferred from game_date (July onward is the next season). |

**`return_parsed=False`** — the decoded JSON response body (the positional-field array).

### Example {#torvik_game_schedule-example}

```python
torvik_game_schedule(year=2025)
```

_Last validated n/a._
