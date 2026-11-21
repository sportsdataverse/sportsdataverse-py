# Men's college basketball — Bart Torvik T-Rank (barttorvik.com)

> Men's college basketball — Bart Torvik T-Rank (barttorvik.com) — endpoint reference in sdv-py, the SportsDataverse Python package.

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
| `rank` | Int64 | T-Rank position (overall barthag rank). |
| `team` | String | Torvik team name (the crosswalk join key). |
| `conf` | String | Torvik conference abbreviation (e.g. B12, ACC, BE). |
| `record` | String | Overall win-loss record to date. |
| `adjoe` | Float64 | Adjusted offensive efficiency (points per 100 possessions vs an average defense). |
| `oe_rank` | Int64 | National rank of adjusted offensive efficiency. |
| `adjde` | Float64 | Adjusted defensive efficiency (points allowed per 100 possessions vs an average offense). |
| `de_rank` | Int64 | National rank of adjusted defensive efficiency. |
| `barthag` | Float64 | Torvik power rating: win probability vs an average team on a neutral floor. |
| `rank_2` | Int64 | Barthag rank repeated as shipped in the source CSV (duplicate of rank). |
| `proj_w` | Float64 | Projected full-season wins. |
| `proj_l` | Float64 | Projected full-season losses. |
| `pro_con_w` | Float64 | Projected conference wins. |
| `pro_con_l` | Float64 | Projected conference losses. |
| `con_rec` | String | Conference win-loss record to date. |
| `sos` | Float64 | Strength of schedule faced to date. |
| `ncsos` | Float64 | Non-conference strength of schedule. |
| `consos` | Float64 | Conference strength of schedule. |
| `proj_sos` | Float64 | Projected full-season strength of schedule. |
| `proj_noncon_sos` | Float64 | Projected non-conference strength of schedule. |
| `proj_con_sos` | Float64 | Projected conference strength of schedule. |
| `elite_sos` | Float64 | Elite strength of schedule (share of schedule vs elite opponents). |
| `elite_noncon_sos` | Float64 | Elite non-conference strength of schedule. |
| `opp_oe` | Float64 | Average opponent offensive efficiency. |
| `opp_de` | Float64 | Average opponent defensive efficiency. |
| `opp_proj_oe` | Float64 | Projected average opponent offensive efficiency. |
| `opp_proj_de` | Float64 | Projected average opponent defensive efficiency. |
| `con_adj_oe` | Float64 | Adjusted offensive efficiency in conference games only. |
| `con_adj_de` | Float64 | Adjusted defensive efficiency in conference games only. |
| `qual_o` | Float64 | Offensive efficiency vs quality (top-tier) opponents. |
| `qual_d` | Float64 | Defensive efficiency vs quality (top-tier) opponents. |
| `qual_barthag` | Float64 | Barthag computed from the quality-opponent efficiency splits. |
| `qual_games` | Float64 | Number of quality games underlying the qual_* splits. |
| `fun` | Float64 | Torvik's FUN style/entertainment index. |
| `con_pf` | Float64 | Points scored in conference play. |
| `con_pa` | Float64 | Points allowed in conference play. |
| `con_poss` | Float64 | Possessions played in conference play. |
| `con_oe` | Float64 | Raw offensive efficiency in conference play. |
| `con_de` | Float64 | Raw defensive efficiency in conference play. |
| `con_sos_remain` | Float64 | Strength of the remaining conference schedule. |
| `conf_win_percent` | Float64 | Conference win percentage. |
| `wab` | Float64 | Wins above bubble (resume quality vs a bubble-level team). |
| `wab_rk` | Int64 | National rank of wins above bubble. |
| `fun_rk` | Int64 | National rank of the FUN index. |
| `adjt` | Float64 | Adjusted tempo (possessions per 40 minutes). |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#torvik_ratings-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. Barttorvik.com returned 404 (no data file for that season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#torvik_ratings-example}

```python
torvik_ratings(year=2025)
```

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
| `team_name` | String | Torvik team name. |
| `e_fg_percent` | Float64 | Effective field-goal percentage on offense. |
| `rk` | Int64 | National rank of offensive eFG%. |
| `e_fg_percent_def` | Float64 | Effective field-goal percentage allowed on defense. |
| `rk_2` | Int64 | National rank of defensive eFG% allowed. |
| `ftr` | Float64 | Free-throw rate on offense (FTA per FGA). |
| `rk_3` | Int64 | National rank of offensive free-throw rate. |
| `ftr_def` | Float64 | Free-throw rate allowed on defense. |
| `rk_4` | Int64 | National rank of defensive free-throw rate allowed. |
| `or_percent` | Float64 | Offensive rebound percentage. |
| `rk_5` | Int64 | National rank of offensive rebound percentage. |
| `dr_percent` | Float64 | Defensive rebound percentage. |
| `rk_6` | Int64 | National rank of defensive rebound percentage. |
| `to_percent` | Float64 | Turnover percentage on offense. |
| `rk_7` | Int64 | National rank of offensive turnover percentage. |
| `to_percent_def` | Float64 | Turnover percentage forced on defense. |
| `rk_8` | Int64 | National rank of defensive turnover percentage forced. |
| `x3p_percent` | Float64 | Three-point percentage on offense. |
| `rk_9` | Int64 | National rank of offensive three-point percentage. |
| `x3p_d_percent` | Float64 | Three-point percentage allowed on defense. |
| `rk_10` | Int64 | National rank of three-point percentage allowed. |
| `x2p_percent` | Float64 | Two-point percentage on offense. |
| `rk_11` | Int64 | National rank of offensive two-point percentage. |
| `x2p_percent_d` | Float64 | Two-point percentage allowed on defense. |
| `rk_12` | Int64 | National rank of two-point percentage allowed. |
| `ft_percent` | Float64 | Free-throw percentage. |
| `rk_13` | Int64 | National rank of free-throw percentage. |
| `ft_percent_d` | Float64 | Opponent free-throw percentage. |
| `rk_14` | Int64 | National rank of opponent free-throw percentage. |
| `x3p_rate` | Float64 | Three-point attempt rate on offense (3PA per FGA). |
| `rk_15` | Int64 | National rank of offensive three-point attempt rate. |
| `x3p_rate_d` | Float64 | Three-point attempt rate allowed on defense. |
| `rk_16` | Int64 | National rank of three-point attempt rate allowed. |
| `arate` | Float64 | Assist rate on offense (assists per made field goal). |
| `rk_17` | Int64 | National rank of offensive assist rate. |
| `arate_d` | Float64 | Assist rate allowed on defense. |
| `rk_18` | Int64 | National rank of assist rate allowed. |
| `unnamed` | Float64 | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_2` | Int64 | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_3` | Float64 | Headerless trailing column shipped in the source CSV (undocumented upstream). |
| `unnamed_4` | Int64 | Headerless trailing column shipped in the source CSV (undocumented upstream). |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#torvik_team_factors-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. Barttorvik.com returned 404 (no data file for that season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#torvik_team_factors-example}

```python
torvik_team_factors(year=2025)
```

## torvik_game_stats

GET /getgamestats.php?year=&json=1 — men's per-team-game efficiency and four-factors log (one row per team-game, 31 positional fields).

**Endpoint URL:** `GET https://barttorvik.com/getgamestats.php`

**Valid URL:** [https://barttorvik.com/getgamestats.php?year=2025&json=1](https://barttorvik.com/getgamestats.php?year=2025&json=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | 4-digit season ending year (2025 = the 2024-25 season). |
| `json` | `json` |  |  | `Y` | Response format switch; leave at 1 (the parser expects the headerless JSON array). |

### Returns {#torvik_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | String | Game date as Torvik prints it (m/d/yy). |
| `type` | Int64 | Torvik game-type code (observed 0-3; 1 is the most common); Torvik does not document the codes. |
| `team` | String | Torvik team name for the row's team (the perspective of every other column). |
| `conf` | String | Torvik conference abbreviation of the row's team (e.g. B12, ACC). |
| `opp` | String | Torvik team name of the opponent. |
| `venue` | String | Venue from the row team's view: H home, A away, N neutral. |
| `result` | String | Result from the row team's view, e.g. 'L, 88-57' (W/L, team points, opponent points). |
| `adj_oe` | Float64 | Row team's pre-game adjusted offensive efficiency (points per 100 possessions). |
| `adj_de` | Float64 | Row team's pre-game adjusted defensive efficiency (points allowed per 100 possessions). |
| `oe` | Float64 | Row team's raw offensive efficiency in this game (points per 100 possessions). |
| `off_efg` | Float64 | Row team's offensive effective field-goal percentage in this game. |
| `off_to` | Float64 | Row team's offensive turnover percentage in this game. |
| `off_or` | Float64 | Row team's offensive rebound percentage in this game. |
| `off_ftr` | Float64 | Row team's free-throw rate on offense in this game. |
| `de` | Float64 | Row team's raw defensive efficiency in this game (points allowed per 100 possessions). |
| `def_efg` | Float64 | Opponent's effective field-goal percentage against the row team. |
| `def_to` | Float64 | Opponent's turnover percentage against the row team. |
| `def_or` | Float64 | Opponent's offensive rebound percentage against the row team. |
| `def_ftr` | Float64 | Opponent's free-throw rate against the row team. |
| `game_score` | Float64 | Torvik's Gscore, a 0-100 single-game performance rating for the row team. |
| `opp_conf` | String | Torvik conference abbreviation of the opponent. |
| `quad` | Int64 | Torvik 'quad' field: observed values are only 1 and 2, tagging which side of the game the row is (each game appears once with each); not the NCAA quadrant. |
| `year` | Int64 | Season ending year (2025 = the 2024-25 season). |
| `tempo` | Float64 | Possessions per 40 minutes in this game. |
| `muid` | String | Torvik matchup id: opponent-order-free concatenation of team names, date and venue; the same value appears in both teams' rows and in the schedule's muid. |
| `coach` | String | Head coach of the row team. |
| `opp_coach` | String | Head coach of the opponent. |
| `margin` | Float64 | Signed margin from the row team's view (positive for a win, negative for a loss); Torvik ships it adjusted, so it differs from the raw score difference (-15.37 for an 88-57 loss). |
| `win_prob` | Float64 | Torvik 'win_prob' field as shipped; it is not the row team's own pre-game win probability (a 31-point loser carries 0.92), so treat it as opaque. |
| `game_stats` | String | JSON-encoded array of raw per-game box-score counts exactly as Torvik ships it; the positional layout is Torvik's and undocumented. |
| `overtimes` | Int64 | Torvik 'overtimes' field as shipped; observed integers 0-13, which is too large for an overtime count, so treat it as opaque. |
| `game_date` | Date | date parsed to a Date (null when it does not parse). |

**`return_parsed=False`** — the decoded JSON response body (the positional-field array).

### Raises {#torvik_game_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. Barttorvik.com returned 404 (no data file for that season). |
| `requests.exceptions.RequestException` | Connection-level failure after `dl_utils.download` exhausts its retries. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#torvik_game_stats-example}

```python
torvik_game_stats(year=2025)
```

## torvik_player_stats

GET /getadvstats.php?year=&csv=1 — men's player advanced stats (one row per player, 67 positional fields).

**Endpoint URL:** `GET https://barttorvik.com/getadvstats.php`

**Valid URL:** [https://barttorvik.com/getadvstats.php?year=2025&csv=1](https://barttorvik.com/getadvstats.php?year=2025&csv=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | 4-digit season ending year (2025 = the 2024-25 season). |
| `csv` | `csv` |  |  | `Y` | Response format switch; leave at 1 (the parser expects the headerless CSV). |

### Returns {#torvik_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_name` | String | Player name as Torvik lists it, 'First Last' (e.g. Robby Carmody). |
| `team` | String | Torvik team name the player is listed with (e.g. Le Moyne); matches the ratings team column. |
| `conf` | String | Torvik conference abbreviation. |
| `games` | Int64 | Games the player appeared in this season. |
| `min_pct` | Float64 | Share of team minutes played, in percent. |
| `o_rtg` | Float64 | Offensive rating (points produced per 100 individual possessions). |
| `usage` | Float64 | Usage rate: share of team possessions used while on the floor, in percent. |
| `e_fg` | Float64 | Effective field-goal percentage. |
| `ts_pct` | Float64 | True shooting percentage. |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `ast_pct` | Float64 | Assist percentage. |
| `to_pct` | Float64 | Turnover percentage. |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage as a fraction (0-1). |
| `two_pm` | Float64 | Two-point field goals made. |
| `two_pa` | Float64 | Two-point field goals attempted. |
| `two_p_pct` | Float64 | Two-point field-goal percentage as a fraction (0-1). |
| `three_pm` | Float64 | Three-point field goals made. |
| `three_pa` | Float64 | Three-point field goals attempted. |
| `three_p_pct` | Float64 | Three-point field-goal percentage as a fraction (0-1). |
| `blk_pct` | Float64 | Block percentage. |
| `stl_pct` | Float64 | Steal percentage. |
| `ftr` | Float64 | Free-throw rate (FTA per FGA, in percent). |
| `class` | String | Class year (Fr, So, Jr, Sr). |
| `height` | String | Listed height as feet-inches, e.g. 6-4. |
| `number` | String | Jersey number as listed, as text ("00" is not "0"). |
| `porpag` | Float64 | Points over replacement per adjusted game (Torvik's PRPG!). |
| `adj_oe` | Float64 | Player's adjusted offensive efficiency. |
| `pfr` | Float64 | Personal fouls committed per 40 minutes. |
| `year` | Int64 | Season ending year (2025 = the 2024-25 season). |
| `player_id` | Int64 | Torvik numeric player id (Int64); the join key to other Torvik player pages. |
| `hometown` | String | Hometown as listed, e.g. 'Mars, PA'; blank for some players. |
| `rec_rank` | Float64 | High-school recruiting rating; null for unranked players. |
| `ast_to` | Float64 | Assist-to-turnover ratio. |
| `rim_made` | Float64 | Made shots at the rim. |
| `rim_attempts` | Float64 | Attempted shots at the rim. |
| `mid_made` | Float64 | Made two-point jump shots (non-rim twos). |
| `mid_attempts` | Float64 | Attempted two-point jump shots (non-rim twos). |
| `rim_pct` | Float64 | Field-goal percentage at the rim as a fraction (0-1). |
| `mid_pct` | Float64 | Field-goal percentage on non-rim twos as a fraction (0-1). |
| `dunks_made` | Float64 | Dunks made over the season. |
| `dunks_attempts` | Float64 | Dunks attempted. |
| `dunks_pct` | Float64 | Dunk percentage as a fraction (0-1). |
| `pick` | Float64 | NBA draft pick number; null for players not drafted. |
| `drtg` | Float64 | Defensive rating (points allowed per 100 individual defensive possessions). |
| `adrtg` | Float64 | Adjusted defensive rating. |
| `dporpag` | Float64 | Defensive points over replacement per adjusted game. |
| `stops` | Float64 | Defensive stops. |
| `bpm` | Float64 | Box plus/minus. |
| `obpm` | Float64 | Offensive box plus/minus. |
| `dbpm` | Float64 | Defensive box plus/minus. |
| `gbpm` | Float64 | Torvik's game-level box plus/minus. |
| `minutes` | Float64 | Share of team minutes played, in percent (as on the Torvik player page). |
| `ogbpm` | Float64 | Offensive game box plus/minus. |
| `dgbpm` | Float64 | Defensive game box plus/minus. |
| `oreb` | Float64 | Offensive rebounds per game. |
| `dreb` | Float64 | Defensive rebounds per game. |
| `treb` | Float64 | Total rebounds per game. |
| `ast` | Float64 | Assists per game. |
| `stl` | Float64 | Steals per game. |
| `blk` | Float64 | Blocks per game. |
| `pts` | Float64 | Points per game. |
| `role` | String | Torvik's position/role label, e.g. 'Combo G' or 'Scoring PG'. |
| `threat` | Float64 | Torvik's scoring-threat rating. |
| `recruit_date` | String | Torvik's 'recruit_date' field; the values are ISO dates that look like birth dates, kept verbatim as text. |

**`return_parsed=False`** — the raw CSV response body (`str`).

### Raises {#torvik_player_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. Barttorvik.com returned 404 (no data file for that season). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#torvik_player_stats-example}

```python
torvik_player_stats(year=2025)
```

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
| `muid` | String | Torvik matchup id; equals the game_stats muid. |
| `date` | String | Game date as Torvik prints it (m/d/yy). |
| `conmatch` | String | Conference matchup descriptor, e.g. 'D2 at Amer'. |
| `matchup` | String | Rank-annotated matchup text, e.g. '0 Northeastern St. at 243 Tulsa'. |
| `prediction` | String | Pre-game prediction text (favorite, margin, score, win probability). |
| `ttq` | String | Torvik Thrill Quotient (game excitement). |
| `conf` | String | Conference-game flag/code as Torvik ships it (observed 0 and 99). |
| `venue` | String | Venue / neutral-site flag as Torvik ships it (0 observed on home/away games). |
| `team1` | String | First-listed team name. |
| `t1oe` | String | team1 adjusted offensive efficiency going into the game. |
| `t1de` | String | team1 adjusted defensive efficiency going into the game. |
| `t1py` | String | team1 pre-game win probability (fraction 0-1). |
| `t1wp` | String | team1 win indicator as Torvik ships it (1 when team1 won). |
| `t1propt` | String | team1 projected points. |
| `team2` | String | Second-listed team name. |
| `t2oe` | String | team2 adjusted offensive efficiency going into the game. |
| `t2de` | String | team2 adjusted defensive efficiency going into the game. |
| `t2py` | String | team2 pre-game win probability (fraction 0-1). |
| `t2wp` | String | team2 win indicator as Torvik ships it (1 when team2 won). |
| `t2propt` | String | team2 projected points. |
| `tpro` | String | Projected tempo (possessions) for the game. |
| `t1qual` | String | Torvik quality marker for team1: the team name when set, blank otherwise. |
| `t2qual` | String | Torvik quality marker for team2: the team name when set, blank otherwise. |
| `gp` | String | Game-played flag (1 if completed). |
| `result` | String | Final result text from team1's view; blank until played. |
| `tempo` | String | Actual tempo (possessions per 40 minutes); blank until played. |
| `possessions` | String | Actual possessions; blank until played. |
| `t1pts` | String | team1 final points; blank until played. |
| `t2pts` | String | team2 final points; blank until played. |
| `winner` | String | Winning team name; blank until played. |
| `loser` | String | Losing team name; blank until played. |
| `t1adjt` | String | team1 adjusted tempo. |
| `t2adjt` | String | team2 adjusted tempo. |
| `t1adjo` | String | Game-level adjusted offensive efficiency for team1 as Torvik ships it (0 when unrated). |
| `t1adjd` | String | Game-level adjusted defensive efficiency for team1 as Torvik ships it (0 when unrated). |
| `t2adjo` | String | Game-level adjusted offensive efficiency for team2 as Torvik ships it (0 when unrated). |
| `t2adjd` | String | Game-level adjusted defensive efficiency for team2 as Torvik ships it (0 when unrated). |
| `gamevalue` | String | Torvik's single-game quality value for the matchup (0 for games between unrated teams). |
| `mismatch` | String | Torvik's mismatch rating (how lopsided the matchup is). |
| `blowout` | String | Torvik's blowout rating. |
| `t1elite` | String | Torvik elite-performance component for team1 (0-1). |
| `t2elite` | String | Torvik elite-performance component for team2 (0-1). |
| `ord_date` | String | Proleptic Gregorian ordinal of the game date (Python date.toordinal). |
| `t1ppp` | String | team1 points per possession in the game. |
| `t2ppp` | String | team2 points per possession in the game. |
| `gameppp` | String | Combined points per possession in the game. |
| `t1rk` | String | team1 T-Rank rank at game time. |
| `t2rk` | String | team2 T-Rank rank at game time. |
| `t1gs` | String | team1 Gscore as a fraction 0-1; '-' when unrated. |
| `t2gs` | String | team2 Gscore as a fraction 0-1; '-' when unrated. |
| `gamestats` | String | Per-game stat array collapsed to a ';'-joined string. |
| `overtimes` | String | Overtime field as Torvik ships it; blank on the captured games. |
| `t1fun` | String | Torvik fun/watchability component for team1 (0-1). |
| `t2fun` | String | Torvik fun/watchability component for team2 (0-1). |
| `results` | String | Results blob as Torvik ships it (text). |
| `game_date` | Date | date parsed to a Date (null when it does not parse). |
| `year` | Int64 | Season ending year inferred from game_date (July onward is the next season). |

**`return_parsed=False`** — the decoded JSON response body (the positional-field array).

### Raises {#torvik_game_schedule-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoESPNDataError` | The host answered 404 -- the requested resource does not exist. Barttorvik.com returned 404 (no data file for that season). |
| `requests.exceptions.RequestException` | Connection-level failure after `dl_utils.download` exhausts its retries. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#torvik_game_schedule-example}

```python
torvik_game_schedule(year=2025)
```
