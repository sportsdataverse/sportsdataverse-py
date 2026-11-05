---
title: "NBA — additional Python functions — Analytics: nba_l2m–nba_team"
sidebar_label: "Analytics: nba_l2m–nba_team"
sidebar_position: 14
description: "NBA — additional Python functions — Analytics: nba_l2m–nba_team — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_l2m–nba_team

### nba_l2m {#nba_l2m}

`nba_l2m(game_id: 'str | int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse an NBA Last Two Minute report from official.nba.com.

Retrieves the L2M report for a given game and returns parsed tables of calls,
game metadata, and error statistics. The game_id is zero-padded to 10 digits
(e.g., 42500405 becomes "0042500405"). A report is published for any game that
is within 3 points (5 points before the 2017-18 season) at any point during the
last two minutes of the fourth quarter or overtime -- not only playoff games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str \| int` |  | NBA game ID (can be int or str). Automatically zero-padded to 10 digits. |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict. Otherwise, a dict with keys `"calls"`, `"game"`, `"stats"` mapping to DataFrames as documented in `parse_nba_l2m`.

| col_name | type | description |
|---|---|---|
| `calls.game_id` | String | 10-digit NBA game id (zero-padded), the join key back to the L2M game and stats tables. |
| `calls.period` | Int64 | Period number extracted from period_name's digits: 4 for the fourth quarter, 5+ for overtime periods (Q5 = OT1 through Q8 = OT4). |
| `calls.period_name` | String | Raw NBA.com period label for the graded play, e.g. Q4 for the fourth quarter or Q5 for the first overtime. |
| `calls.pc_time` | String | Raw game clock string from the L2M report before normalization, in MM:SS or MM:SS.t tenths-of-a-second format. |
| `calls.seconds_remaining` | Float64 | Seconds remaining in the period at the graded play, parsed out of pc_time. |
| `calls.call_type` | String | Raw "Call: Type" label from the report, e.g. "Foul: Shooting" or "Turnover:  24 Second Violation"; whitespace (including doubled spaces) is NOT cleaned here -- the cleaned, upper-cased parts are call and type. |
| `calls.call` | String | Upper-cased category before the colon in call_type, e.g. FOUL, TURNOVER, or STOPPAGE. |
| `calls.type` | String | Upper-cased detail after the colon in call_type, e.g. SHOOTING or 24 SECOND VIOLATION. |
| `calls.committing` | String | Player, team nickname, or coach responsible for the graded action. |
| `calls.disadvantaged` | String | Player or team nickname disadvantaged by the graded action, blank when not applicable. |
| `calls.decision` | String | Normalized grading decision: CC (correct call), CNC (correct non-call), IC (incorrect call), or INC (incorrect non-call); null when the report marks the play undetectable at game speed. |
| `calls.decision_raw` | String | Raw grading code from the report before normalization, including the rare NCC/NCI aliases folded into decision. |
| `calls.comment` | String | Grader's free-text explanation of the ruling, sometimes naming a player as "Last (TEAM)". |
| `calls.difficulty` | String | Grader's difficulty rating for the play, e.g. Observable, Difficult, Blatant/Hard, or Undetectable. |
| `calls.video_event_id` | String | L2M video event id from the report (source field VideolLink, sic); not a stats.nba.com play-by-play EVENTNUM, so it cannot be joined to pbp data. |
| `calls.pos_id` | Int64 | Possession id assigned by the report, rising monotonically across the game; rows sharing pos_start/pos_end belong to the same possession. |
| `calls.pos_start` | String | Game clock at the start of the possession containing this graded play. |
| `calls.pos_end` | String | Game clock at the end of the possession containing this graded play. |
| `calls.pos_team_id` | Int64 | 10-digit NBA team id of the team in possession during the graded play. |
| `game.game_id` | String | 10-digit NBA game id (zero-padded), the join key to the calls and stats tables. |
| `game.game_date` | Date | Game date parsed from the report's local tip-off timestamp (no timezone offset is applied). |
| `game.season_type` | String | Season type inferred from the third digit of game_id: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `game.home_team_id` | Int64 | 10-digit NBA team id (1610612xxx) of the home team. |
| `game.away_team_id` | Int64 | 10-digit NBA team id (1610612xxx) of the away team. |
| `game.home_team_abbr` | String | Three-letter abbreviation of the home team. |
| `game.away_team_abbr` | String | Three-letter abbreviation of the away team. |
| `game.home_team_name` | String | Home team nickname as published in the report, e.g. Thunder rather than Oklahoma City Thunder. |
| `game.away_team_name` | String | Away team nickname as published in the report. |
| `game.home_score` | Int64 | Home team's final score for the game. |
| `game.away_score` | Int64 | Away team's final score for the game. |
| `game.l2m_comments` | String | Report-level note from the league, e.g. a technical-issue disclaimer about missing video; null for almost every game. |
| `stats.game_id` | String | 10-digit NBA game id (zero-padded), the join key to the calls and game tables. |
| `stats.stat_name` | String | Name of the report-level error-rate statistic: Calls, Errors in Favor, or Possessions in Favor. |
| `stats.home` | Int64 | Value of stat_name attributed to the home team. |
| `stats.away` | Int64 | Value of stat_name attributed to the away team. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `game_id` is not one non-negative integer id -- a bool, a negative or fractional number, a string that is not all digits, or more than 10 digits (checked before any request). |
| `NoDataError` | The game has no L2M report (a 404, or S3's `AccessDenied` 403). Only games within 3 points (5 before 2017-18) at some point in the last two minutes of the fourth quarter or overtime get one, so most games have none; a game that just ended may not be graded yet. |
| `AssetFetchError` | The fetch failed (a transport error, a rate limit, an Akamai WAF block, or a 5xx that outlived the retry budget), or the 200 body is not a JSON object whose `game` is exactly one row, or its `l2m`/`stats` table is not a list of records (empty is fine) -- an error envelope or a changed schema, checked with `raw=True` too. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_l2m
result = nba_l2m("0042500405")
calls = result["calls"]
print(f"Game had {calls.height} tracked plays in the L2M window")

# Parse as pandas instead

result = nba_l2m("0042500405", return_as_pandas=True)
calls_pd = result["calls"]

# Access raw JSON

payload = nba_l2m("0042500405", raw=True)
print(payload["game"])
```

### nba_l2m_games {#nba_l2m_games}

`nba_l2m_games(season: 'int | str', *, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Fetch the list of games with Last Two Minute reports for an NBA season.

Retrieves and parses the L2M season index page from official.nba.com,
returning a table of all games for which L2M reports exist. JSON reports
exist only from 2019-01-01 onward; earlier seasons' index pages list PDFs,
which this function ignores. A release loader for historical (PDF-era)
reports is planned but does not exist yet.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int \| str` |  | The NBA season (end year) as a 4-digit int or string, e.g. 2026 for the 2025-26 season. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

A `polars.DataFrame` (or `pandas.DataFrame` when `return_as_pandas=True`) with schema `{"game_id": Utf8, "season": Int32, "season_type": Utf8, "label": Utf8}`, one row per unique game ID in page order.

| col_name | type | description |
|---|---|---|
| `game_id` | String | 10-digit NBA game id (zero-padded), parsed from the season listing page's report link. |
| `season` | Int64 | NBA season end year passed to the function (e.g. 2026 for the 2025-26 season), stamped onto every row. |
| `season_type` | String | Season type inferred from the third digit of game_id: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `label` | String | Matchup label text scraped from the listing page link, e.g. "Thunder 125, Rockets 124 (2OT)". |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season` is not a 4-digit year (an int or an all-digit string), checked before any request. |
| `NoDataError` | Official.nba.com has no listing page for that season (a 404, or S3's `AccessDenied` 403) -- e.g. a future season, or one from before the L2M program began. |
| `AssetFetchError` | The fetch failed (network error, rate limit, or Akamai WAF block), or a 200 response that is missing the expected "Last Two Minute" page marker (an Akamai interstitial, a blank body, or a redesigned page), or that links reports in a shape the parser does not read (a redesigned page) -- checked here, not in `parse_nba_l2m_games`, so the parser itself never raises. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_l2m_games
df = nba_l2m_games(2026)
print(f"Season had {df.height} games with L2M reports")

# Get playoff games only

df = nba_l2m_games(2026)
playoffs = df.filter(df["season_type"] == "playoffs")

# Convert to pandas

df = nba_l2m_games(2026, return_as_pandas=True)
```

### nba_lineup_count_rows {#nba_lineup_count_rows}

`nba_lineup_count_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame') -> 'pl.DataFrame'`

Map `nba_stats_possessions` rows onto the lineup-seasons count frame.

Each possession's counts go to the side that had the ball, its `dreb` (the defense's
defensive rebounds) to the other side, on the possession's lineup. A possession whose offense
is neither team of its `schedule` game is dropped, as in the game impact box.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_nba_stats_possessions` / `load_wnba_stats_possessions` rows (`fg2a`, `fg2m`, `fg3a`, `fg3m`, `fta`, `oreb`, `dreb`, `tov`). |
| `schedule` | `DataFrame` |  | The games to keep (`game_id`, `home_team_id`, `away_team_id`). |

**Returns**

One row per possession in `sportsdataverse._common.lineup_seasons.lineup_count_schema` with Int64 player ids.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.nba.com game id. |
| `home_team_id` | Int64 | stats.nba.com team id of the home side. |
| `away_team_id` | Int64 | stats.nba.com team id of the away side. |
| `home_1` | Int64 | stats.nba.com id of home player 1 on the floor when the possession starts. |
| `home_2` | Int64 | stats.nba.com id of home player 2 on the floor when the possession starts. |
| `home_3` | Int64 | stats.nba.com id of home player 3 on the floor when the possession starts. |
| `home_4` | Int64 | stats.nba.com id of home player 4 on the floor when the possession starts. |
| `home_5` | Int64 | stats.nba.com id of home player 5 on the floor when the possession starts. |
| `away_1` | Int64 | stats.nba.com id of away player 1 on the floor when the possession starts. |
| `away_2` | Int64 | stats.nba.com id of away player 2 on the floor when the possession starts. |
| `away_3` | Int64 | stats.nba.com id of away player 3 on the floor when the possession starts. |
| `away_4` | Int64 | stats.nba.com id of away player 4 on the floor when the possession starts. |
| `away_5` | Int64 | stats.nba.com id of away player 5 on the floor when the possession starts. |
| `home_fga` | Int64 | The home side's field-goal attempts in the possession. |
| `home_fgm` | Int64 | The home side's field goals made in the possession. |
| `home_fg3a` | Int64 | The home side's three-point attempts in the possession. |
| `home_fg3m` | Int64 | The home side's threes made in the possession. |
| `home_fta` | Int64 | The home side's free-throw attempts in the possession. |
| `home_oreb` | Int64 | The home side's offensive rebounds in the possession. |
| `home_dreb` | Int64 | The home side's defensive rebounds in the possession. |
| `home_tov` | Int64 | The home side's turnovers in the possession. |
| `away_fga` | Int64 | The away side's field-goal attempts in the possession. |
| `away_fgm` | Int64 | The away side's field goals made in the possession. |
| `away_fg3a` | Int64 | The away side's three-point attempts in the possession. |
| `away_fg3m` | Int64 | The away side's threes made in the possession. |
| `away_fta` | Int64 | The away side's free-throw attempts in the possession. |
| `away_oreb` | Int64 | The away side's offensive rebounds in the possession. |
| `away_dreb` | Int64 | The away side's defensive rebounds in the possession. |
| `away_tov` | Int64 | The away side's turnovers in the possession. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a count column holds a null. |

**Example**

```python
from sportsdataverse.nba import nba_loaders as L
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows
rows = nba_lineup_count_rows(L.load_nba_stats_possessions(2024), L.load_nba_stats_schedules(2024))
```

### nba_lineup_seasons {#nba_lineup_seasons}

`nba_lineup_seasons(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', min_poss: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Lineup seasons: 2-, 3- and 5-man units with possessions, minutes, ratings and four factors.

The definitions are in `sportsdataverse._common.lineup_seasons`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `min_poss` | `int \| None` | `None` | The rank gate, offensive plus defensive possessions. Defaults to 400 (NBA) or 200 (WNBA). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, n_players, lineup_id)`: `n_players` (2, 3 or 5), `lineup_id` (the player ids sorted and joined with `-`), `player_1..5`, `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions), the four factors for (`efg_pct`, `tov_pct`, `orb_pct`, `ft_rate`) and against (`opp_*`) as fractions, and for every rate `<m>_rank` / `<m>_pct` (within the season and unit size, among units with at least `min_poss`) and `<m>_n`. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `n_players` | Int64 | Unit size: 2, 3 or 5 players. |
| `lineup_id` | String | The unit's stats.nba.com player ids, sorted numerically and joined with '-'. |
| `player_1` | Int64 | Player 1 of the unit, in lineup_id order; null past n_players. |
| `player_2` | Int64 | Player 2 of the unit, in lineup_id order; null past n_players. |
| `player_3` | Int64 | Player 3 of the unit, in lineup_id order; null past n_players. |
| `player_4` | Int64 | Player 4 of the unit, in lineup_id order; null past n_players. |
| `player_5` | Int64 | Player 5 of the unit, in lineup_id order; null past n_players. |
| `minutes` | Float64 | Game-clock minutes the unit shared the floor over the season. |
| `poss` | Int64 | Offensive possessions with the unit on the floor (a possession counts for the five on the floor when it starts; a 2- or 3-man unit sums the fives that contain it). |
| `opp_poss` | Int64 | Defensive possessions with the unit on the floor, counted as poss is. |
| `pts` | Int64 | The team's points with the unit on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts` | Int64 | The opponent's points with the unit on the floor, credited as pts is. |
| `plus_minus` | Int64 | The unit's +/- over the season: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss. Null with a zero denominator. On 2024-25 units it runs about 5 above stats.nba.com's lineup rating, which credits a possession to every lineup on the floor during it (its counts run ~4% higher). |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss. Null with a zero denominator. On 2024-25 units it runs about 5 above stats.nba.com's lineup rating, which credits a possession to every lineup on the floor during it (its counts run ~4% higher). |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Null with a zero denominator. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Null with a zero denominator. |
| `orb_pct` | Float64 | Offensive-rebound rate: OREB / (OREB + the opponent's DREB), as a fraction. Null with a zero denominator. |
| `ft_rate` | Float64 | Free-throw rate: FTA / FGA. Null with a zero denominator. |
| `opp_efg_pct` | Float64 | The opponent's effective field-goal percentage with the unit on the floor. Null with a zero denominator. |
| `opp_tov_pct` | Float64 | The opponent's turnovers per defensive possession. Null with a zero denominator. |
| `opp_orb_pct` | Float64 | The opponent's offensive-rebound rate: its OREB / (its OREB + the unit's DREB). Null with a zero denominator. |
| `opp_ft_rate` | Float64 | The opponent's free-throw rate: FTA / FGA. Null with a zero denominator. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: the unit's field-goal attempts. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: poss. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the unit's offensive-rebound chances. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the unit's field-goal attempts. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponent's field-goal attempts. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: opp_poss. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponent's offensive-rebound chances. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponent's field-goal attempts. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, `season_type` is not one of the four above, or the possessions carry a null shot, free-throw, rebound or turnover count. |

**Example**

```python
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_seasons
units = nba_lineup_seasons(2025)
units.filter(n_players=5).sort("net_rtg_rank").head()
```

### nba_play_context {#nba_play_context}

`nba_play_context(game_id: 'str', league_id: 'str' = '00', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Fetch one game and return its possessions with the full CTG play-context surface.

Single live call to `nba_stats_playbyplayv3`, then
`add_play_context`. Works for NBA (`league_id="00"`), WNBA and the
G-League — `stats.wnba.com` ships the same play-by-play shapes, and every
threshold here is league-agnostic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | Ten-character game identifier (e.g. `"0022200001"`). |
| `league_id` | `str` | `'00'` | League identifier (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 10.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Possession frame with the play-context columns. Empty/malformed payloads return a zero-row frame — never raises.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba.nba_play_context import nba_play_context
poss = nba_play_context("0022200001")
print(poss["possession_start_type_ctg"].value_counts())

# Transition rate for the game

import polars as pl
clean = poss.filter(
    (pl.col("is_garbage_time") == False) & (pl.col("is_heave_possession") == False)
)
print(clean["is_transition"].mean())
```

### nba_player_ages {#nba_player_ages}

`nba_player_ages(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Per-player age for a season (bulk), for the DARKO aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_leaguedashplayerbiostats` replacement for offline tests. |

**Returns**

Frame `player_id:Int64, age:Float64`.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_player_ages
ages = nba_player_ages("2023-24")
print(ages.head())
```

### nba_player_identity {#nba_player_identity}

`nba_player_identity(player_logs: 'pl.DataFrame') -> 'pl.DataFrame'`

Human-readable identity for every player in a season's box logs.

Model outputs key on `player_id` alone, which makes them unusable without a
second lookup -- a leaderboard reads `1628983` instead of
`Shai Gilgeous-Alexander`. This derives the display columns from the season's
own game logs, so they are **season-accurate**: a player's team is what he
actually played for that year, not his current one (which is what a player
directory would give and would silently mislabel every historical season).

A traded player has rows for several teams. `team_*` is his **primary** team
by minutes -- the one a reader means when they say "his team that season" --
and `teams` lists every abbreviation he appeared for, in descending minutes,
so a trade is visible rather than silently collapsed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game rows from `leaguegamelog` (the `player_or_team="P"` variant), carrying `player_id`, `player_name`, `team_id`, `team_abbreviation`, `team_name` and `min`. |

**Returns**

One row per `player_id` with `PLAYER_IDENTITY_SCHEMA`. An empty input -- or one missing any required column, `min` included -- gives the zero-row frame with that schema, so callers can join unconditionally. `min` is required rather than optional: without it every team totals zero minutes and "primary team" quietly degrades to whichever `team_id` sorts first, which looks like an answer but is not one. A malformed frame yields the typed zero-row frame instead of raising, so a caller can join unconditionally.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `teams` | String | Nested list of member-team membership spans. |

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_player_identity

logs = pl.DataFrame({
    "player_id": [1628983],
    "player_name": ["Shai Gilgeous-Alexander"],
    "team_id": [1610612760],
    "team_abbreviation": ["OKC"],
    "team_name": ["Oklahoma City Thunder"],
    "min": [34.0],
})
ratings = pl.DataFrame({"player_id": [1628983], "war": [21.9]})
named = ratings.join(nba_player_identity(logs), on="player_id", how="left")
print(named.select("player_name", "team_name", "war"))
```

### nba_player_positions {#nba_player_positions}

`nba_player_positions(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Fetch league-wide listed positions for a season as numeric 1-5.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_playerindex` replacement for offline tests. |

**Returns**

Frame with columns `player_id:Int64, position_num:Float64`.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | If `curl_cffi` is not installed (required by the stats runtime). |

**Example**

```python
from sportsdataverse.nba import nba_player_positions
pos = nba_player_positions("2023-24")
print(pos.head())

# Offline / injectable fetch for testing

import polars as pl
stub = lambda **kw: pl.DataFrame({"person_id": [1], "position": ["PG"]})
pos = nba_player_positions("2023-24", fetch=stub)
```

### nba_player_props {#nba_player_props}

`nba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-player expected prop lines + team pace projection for a matchup.

Loads the season's player box logs + team ratings, keeps only games played
strictly before `as_of_date` (both inputs), computes per-minute
`player_rates`, and projects each player's line onto their mean
minutes and the matchup's pace factor (`exp_poss / avg_pace`). Only the
two teams in the matchup are returned.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024`). |
| `game_id` | `str` |  | The game id. When `as_of_date` is `None` and this game is in the season's box logs or schedule, its date becomes the cut, so the default call never sees the game's own or any later line. |
| `home_team_id` | `str` |  | Home team id. |
| `away_team_id` | `str` |  | Away team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `as_of_date` | `Union[date, None]` | `None` | Only box lines and ratings games with `game_date < as_of_date` (strict) are used. `None` = the target game's date from the box logs, else from the schedule (one extra schedule read, only when the game is missing from the logs); a game in neither gets no cut and an `~sportsdataverse.errors.AsOfCutWarning`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per player on either team: `player_id, team_id, stat_pts_exp, stat_reb_exp, stat_ast_exp, stat_fg3m_exp, pace_proj`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `team_id` | String | Unique team identifier. |
| `stat_pts_exp` | Float64 |  |
| `stat_reb_exp` | Float64 |  |
| `stat_ast_exp` | Float64 |  |
| `stat_fg3m_exp` | Float64 |  |
| `pace_proj` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | The box logs carry no `game_date` (the as-of cut cannot be applied). |

**Example**

```python
from sportsdataverse.nba.nba_player_props import nba_player_props
props = nba_player_props(2024, "401585828", "2", "6")

# Backtest as of a date

import datetime as dt
props = nba_player_props(2024, "401585828", "2", "6", as_of_date=dt.date(2024, 1, 15))
```

### nba_playtype_ratings {#nba_playtype_ratings}

`nba_playtype_ratings(season: 'str', *, league_id: 'str' = '00', off_team: 'Optional[pl.DataFrame]' = None, def_team: 'Optional[pl.DataFrame]' = None, schedule: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Season play-type-adjusted offensive/defensive team ratings.

Fetches (or uses injected) Synergy offensive/defensive team frames plus the
league schedule, computes raw per-type efficiency
(`raw_playtype_efficiency`), opponent-adjusts it
(`adjust_playtype_efficiency`), then rolls up to one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id` (Int64), `adj_off`/`adj_def`/`adj_net` (Float64) roll-ups, plus per-type wide columns `adj_off_ppp_<playtype>`/`adj_def_ppp_<playtype>`/`off_freq_<playtype>` (Float64) for each play type present in the data. `adj_off = Σ_t off_freq_t · adj_off_ppp_t · 100` (symmetric for `adj_def` off `def_freq_t`); `adj_net = adj_off - adj_def`. Returns a zero-row frame with the base roll-up schema when the upstream fetch is empty (sparse-coverage leagues never raise).

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_playtype_ratings
r = nba_playtype_ratings("2023-24")
print(r.sort("adj_off", descending=True).head(10))

# Injected offline (oracle / test) path

r = nba_playtype_ratings("2023-24", off_team=off_df, def_team=def_df, schedule=sched_df)

# Pipeline next step

r.filter(pl.col("adj_net") > 0).sort("adj_net", descending=True)
```

### nba_possession_rate_rows {#nba_possession_rate_rows}

`nba_possession_rate_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season') -> 'pl.DataFrame'`

Map `nba_stats_possessions` rows onto the team-season-rates possession frame.

Keeps the games of `schedule` whose `season_type` matches, takes the home team from the
schedule, and sets each possession's start clock to the previous possession's end clock in the
period (the period length for the first one). Points come from the made-shot counts.

`neutral_site` is always False: the stats schedule has no neutral-site or venue column, so
a neutral-site game (an NBA Cup semifinal in Las Vegas, a Mexico City or Paris game) counts
as a home game for the listed home team: at most five regular-season games a season.
ESPN's schedule `neutral_site` is not a usable substitute: it marks the 2023-24 Cup
semifinals False, and the ESPN-to-stats game id crosswalk starts at 2025-26.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_nba_stats_possessions` / `load_wnba_stats_possessions` rows. |
| `schedule` | `DataFrame` |  | `load_nba_stats_schedules` / `load_wnba_stats_schedules` rows (`game_id`, `season_type`, `home_team_id`). |
| `league_id` | `str` | `'00'` | `"00"` NBA (12-minute quarters) or `"10"` WNBA (10-minute quarters). |
| `season_type` | `str` | `'regular-season'` | The schedule `season_type` to keep: `"regular-season"` (the default, stats.nba.com's "Regular Season"; NBA Cup group and knockout games count, the Cup final does not), `"preseason"`, `"playoffs"` or `"play-in"`. |

**Returns**

One row per possession in `~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.nba.com game id. |
| `period` | Int64 | Period of the possession; overtimes follow the last regulation period. |
| `possession_number` | Int64 | Order of the possession in the game. |
| `late` | Boolean | True in the 4th quarter or an overtime: the periods the clutch window can fall in. |
| `start_clock` | Float64 | Seconds left in the period when the possession starts (when the previous possession ended). |
| `end_clock` | Float64 | Seconds left in the period when the possession ends. |
| `offense_team_id` | Int64 | stats.nba.com id of the team with the ball. |
| `defense_team_id` | Int64 | stats.nba.com id of the team on defense. |
| `offense_is_home` | Boolean | True when the offense is the (nominal) home team; it signs the running score for the clutch margin. |
| `neutral_site` | Boolean | Always False: the schedule names a home team for every NBA and WNBA game. |
| `points` | Int64 | Points the offense scored on the possession, from its made shots and free throws (2 * fg2m + 3 * fg3m + ftm). |
| `fga` | Float64 | Offense field-goal attempts on the possession. |
| `fgm` | Float64 | Offense field goals made on the possession. |
| `fg3m` | Float64 | Offense three-pointers made on the possession. |
| `fta` | Float64 | Offense free-throw attempts on the possession. |
| `oreb` | Float64 | Offense offensive rebounds on the possession. |
| `dreb` | Float64 | Defense defensive rebounds on the possession (the other half of the offensive-rebound chances). |
| `tov` | Float64 | Offense turnovers on the possession. |
| `ff_poss` | Float64 | Turnover-rate denominator: 1 per possession. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions, load_nba_stats_schedules
from sportsdataverse.nba.nba_team_season_rates import nba_possession_rate_rows
rows = nba_possession_rate_rows(load_nba_stats_possessions(2025), load_nba_stats_schedules(2025))
```

### nba_ratings_panel {#nba_ratings_panel}

`nba_ratings_panel(model: 'AnyModel', possessions: 'pl.DataFrame', dates: 'Optional[Sequence[datetime.date]]' = None, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Player-ratings-through-date long panel: one row per (player_id, date).

Refit-per-checkpoint (v1; no warm-start incrementality) — each date's row
calls `ratings_as_of` independently, so the panel is leakage-free by
construction: a possession dated after a given checkpoint can never affect
that checkpoint's row, no matter what other dates are also being computed
or what future rows exist in `possessions`. Cost is a full refit per
checkpoint date; for a season's sparse RAPM-family design this is seconds
per date, not minutes — acceptable for a nightly/daily cadence but not for
live in-game updating (out of scope; see spec non-goals).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel`. |
| `possessions` | `DataFrame` |  | A possession+lineup frame with a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `dates` | `Optional[Sequence[date]]` | `None` | Checkpoint dates to compute. `None` (default) uses every distinct `game_date` present in `possessions`, sorted ascending — a rating for every game day, matching what EPM/LEBRON publish nightly. Duplicates are deduped; input order does not matter (the output is always sorted by date). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Long frame with `RATINGS_PANEL_SCHEMA` columns (`player_id`, `date`, `o_rating`, `d_rating`, `rating`). Zero-row (that schema) when `possessions` is empty or no date yields any players.

No returns table is published for this function: no capture: its possessions input needs a game_date column, and neither nba_possessions nor the released possessions carry one.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `possessions` (non-empty) lacks a `game_date` column. |

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import nba_ratings_panel

checkpoints = [datetime.date(2023, 11, 1), datetime.date(2023, 12, 1)]
panel = nba_ratings_panel(RidgeRapmModel(), season_poss, dates=checkpoints)
print(panel.filter(pl.col("player_id") == 201939).sort("date"))

# Every game day, no explicit grid

panel = nba_ratings_panel(RidgeRapmModel(), season_poss)
```

### nba_raw_store_season_frame {#nba_raw_store_season_frame}

`nba_raw_store_season_frame(endpoint: 'str', season: 'int', variant: 'Optional[str]' = None, *, result_set: 'Optional[str]' = None, raw_store_dir: 'RawStoreDir' = None) -> "Optional['pl.DataFrame']"`

Read a committed SEASON-LEVEL capture from the raw store, parsed to a frame.

The per-game half of the store is served by the read-through per-game path;
this is the season-keyed half (`leaguegamelog`, `playerindex`,
`leaguedashplayerbiostats`, ...) that the `-raw` scraper writes as

* `{endpoint}/{season}/{variant}.json` -- parameterized captures, where
  *variant* is the slugified parameter sweep (e.g. `"regular-season"`,
  `"regular-season_totals"`), and
* `{endpoint}/{season}.json` -- unparameterized captures (e.g.
  `playerindex`), i.e. `variant=None`.

Roots may be a local checkout or an `http(s)://` base (the raw repo served
over raw.githubusercontent / a CDN), so a consumer -- notably the
`hoopR-nba-stats-data` model producer -- runs clone-free in CI against the
same committed tree the per-game compile reads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` |  | stats.nba.com endpoint slug (the store subdirectory). |
| `season` | `int` |  | Season **END** year (2024 = 2023-24) -- the key both halves of the store are filed under. |
| `variant` | `Optional[str]` | `None` | Capture variant slug, or `None` for an unparameterized capture. |
| `result_set` | `Optional[str]` | `None` | Named result set to return when the payload carries several; defaults to the first frame that parses. |
| `raw_store_dir` | `RawStoreDir` | `None` | Store root spec (dir or URL base) or per-endpoint mapping; `None` falls back to the env vars, `""` disables. |

**Returns**

The parsed `polars.DataFrame`, or `None` when the store is unset, the capture is absent, or the payload carries no usable frame -- so a caller can cleanly fall back to a live fetch.

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | Int64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |
| `video_available` | Int64 | Video available. |

**Example**

```python
from sportsdataverse.nba import nba_raw_store_season_frame
base = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
logs = nba_raw_store_season_frame("leaguegamelog", 2024, "regular-season", raw_store_dir=base)

# Fall back to a live fetch when the capture is absent

frame = nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=base)
positions = frame if frame is not None else nba_stats_playerindex(season="2023-24")
```

### nba_referee_assignments {#nba_referee_assignments}

`nba_referee_assignments(date: 'str | _dt.date', *, league: 'str' = 'nba', raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse NBA referee assignments for a given date from official.nba.com.

Retrieves one league's referee crew assignments and replay-center officials for a
date: NBA, G-League or WNBA, picked by `league` (`raw=True` returns all three
leagues' blocks). The `crew_position` column (1–4) represents the feed's slot
order; slot 1 is inferred to be the crew chief. The `season` column converts
from the feed's format to an END year: START+1 for NBA/G-League
(two-calendar-year seasons) and START unchanged for WNBA.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `str \| date` |  | The date to fetch assignments for (str in "YYYY-MM-DD" format or datetime.date). |
| `league` | `str` | `'nba'` | The league to extract ("nba", "gl", or "wnba"). Defaults to "nba". |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) with all three leagues instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict with keys "nba", "gl", "wnba". Otherwise, a dict with keys `"officials"` and `"replay_center"` mapping to DataFrames as documented in `parse_nba_referee_assignments`.

| col_name | type | description |
|---|---|---|
| `officials.league` | String | League the assignment belongs to: nba, gl (G League), or wnba. |
| `officials.game_id` | String | 10-digit game id (zero-padded) for the assigned game. |
| `officials.game_date` | Date | Game date parsed from the feed's MM/DD/YYYY format. |
| `officials.season` | Int64 | Season end year, converted from the feed's <season-type digit><start year> code: start year + 1 for NBA/G League's two-calendar-year seasons, start year unchanged for WNBA's single-year seasons. |
| `officials.season_type` | String | Season type decoded from the feed's season code first digit: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `officials.game_code` | String | League game code in YYYYMMDD/AWYHOM format, matching the away and home team abbreviations. |
| `officials.home_team_id` | Int64 | 10-digit team id of the home team. |
| `officials.home_team_abbr` | String | Three-letter abbreviation of the home team. |
| `officials.away_team_id` | Int64 | 10-digit team id of the away team. |
| `officials.away_team_abbr` | String | Three-letter abbreviation of the away team. |
| `officials.crew_position` | Int64 | Feed's official slot order (1-4); slot 1 is inferred to be the crew chief since the API does not label roles. |
| `officials.official_id` | Int64 | Numeric official id from the feed (source field official{n}_code); expected to match stats.nba.com's OFFICIAL_ID. |
| `officials.official_name` | String | Official's display name for this crew slot. |
| `officials.jersey_num` | String | Official's jersey number as a string, from the feed's official{n}_JNum field. |
| `replay_center.league` | String | League the replay-center staffing belongs to: nba, gl, or wnba. |
| `replay_center.game_date` | Date | Date the replay-center official worked; a date-level staffing record, not tied to one game. |
| `replay_center.official_id` | Int64 | Numeric replay-center official id from the feed. |
| `replay_center.official_name` | String | Replay-center official's display name for that date. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If league is not "nba", "gl", or "wnba", or date is not a valid "YYYY-MM-DD" date (checked before any request). |
| `NoDataError` | Official.nba.com answered 404, or 403 with S3's `AccessDenied` body. A date without games is not this: it comes back as a 200 with empty `rows` (see the Note). |
| `AssetFetchError` | The fetch failed (network error, rate limit, or Akamai WAF block), the 200 body is not a JSON object, or the league's `Table`/`Table1` rows are missing or malformed (a `Table` row without a 10-digit `game_id`, or a `Table1` row without a `replaycenter_official` name, included). With `raw=True` all three leagues are checked, since the whole payload is returned. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_referee_assignments
result = nba_referee_assignments("2026-06-13")
officials = result["officials"]
print(f"Found {officials.height} official slots")

# Fetch WNBA assignments for the same date

result = nba_referee_assignments("2026-06-13", league="wnba")
wnba_officials = result["officials"]
```

### nba_season_sim {#nba_season_sim}

`nba_season_sim(season: 'int', *, as_of_date: 'dt.date | None' = None, league_id: 'str' = '00', n_sims: 'int' = 10000, seed: 'int' = 20261103, chances: 'list[float] | None' = None, cup_schedule: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season sim as of a date: projected W-L, division, seed, play-in, playoff and lottery odds.

Reads the `{nba,wnba}_stats_schedules` and `_possessions` releases for the season and the
one before, fits the game model on the regular-season games played before `as_of_date`,
shrunk toward last season's final ratings (`as_of_game_model`, `preseason_prior`),
and simulates the rest (`simulate_pro_season`). Before the first game the ratings are
the prior alone. The rules applied to each simulated season
are in `sportsdataverse.nba.nba_season_rules`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2027` is 2026-27); the WNBA season is its calendar year. |
| `as_of_date` | `date \| None` | `None` | Games on or after this date are simulated even when the release has their score (a backtest). None: every game with a score counts as played. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `n_sims` | `int` | `10000` | Simulated seasons (Monte Carlo error: see `SIM_SCHEMA`'s `_se` columns). |
| `seed` | `int` | `20261103` | Seeds the generator; the same release data and seed give the same output. |
| `chances` | `list[float] \| None` | `None` | Lottery chances per slot, worst record first, one per lottery team; overrides the published table (needed for the 2027 WNBA lottery's seven teams, not published yet). |
| `cup_schedule` | `DataFrame \| None` | `None` | NBA: stats.nba.com's schedule for the season (`~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2`), which marks the Cup games. Read only while the release lists TBD Cup games; None fetches it then (a residential connection), and a failed fetch falls back to league-average opponents for the Cup slots with a warning. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team (stats.nba.com `team_id`, Int64), sorted by conference and mean seed: * `wins` / `losses` so far, `games_remaining`, and the as-of `adj_net_rtg` with its posterior sd `adj_net_rtg_sd` (capped so two teams' shift carries at most 3/4 of a game's variance; it binds only near the prior, before many games are played). * `proj_wins` / `proj_losses` (means), `proj_wins_sd`, the 10th and 90th percentiles, and `proj_wins_se` (the Monte Carlo error of the mean). * `p_division` (NBA), `p_seed_1` .. `p_seed_15` (the regular-season finish in the conference, NBA, or the league, WNBA) and `mean_seed`. * `p_top4` (first-round home court), `p_top6` (NBA, straight to the playoffs), `p_play_in` (NBA, seeds 7-10), `p_playoffs` (NBA: top 6 or through the play-in; WNBA: top 8) and its Monte Carlo `p_playoffs_se`. * `p_lottery`, `p_pick_1`, `p_drawn_pick` (a pick from the drawing: the NBA's top four, the WNBA's top two) and `mean_pick` (given a lottery finish). The pick columns are null when the league has no published chances for that many lottery teams. * `n_sims`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26); the WNBA season is its calendar year. |
| `as_of_date` | Date | The as-of date: games on or after it are simulated, the ones before it are played. Null when every game with a score counts as played. |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `conference` | String | NBA conference (East / West) from the 2004-05 alignment; "WNBA" for every WNBA team (one table). |
| `division` | String | NBA division (Atlantic, Central, Southeast, Northwest, Pacific, Southwest); "WNBA" for every WNBA team. |
| `wins` | Int64 | Regular-season wins in the games already played. |
| `losses` | Int64 | Regular-season losses in the games already played. |
| `games_remaining` | Int64 | Regular-season games on the schedule still to play (simulated). |
| `adj_net_rtg` | Float64 | As-of adjusted net rating from the possession ridge at the sim's penalty: points per 100 over an average opponent on a neutral court. |
| `adj_net_rtg_sd` | Float64 | Posterior sd of adj_net_rtg (points per 100) from the ridge, capped so two teams' shift carries at most 3/4 of a game's variance; the sd of the per-season rating shift each simulated season draws. |
| `proj_wins` | Float64 | Mean final regular-season wins over the simulated seasons (played wins plus simulated ones). |
| `proj_losses` | Float64 | Mean final regular-season losses over the simulated seasons. |
| `proj_wins_sd` | Float64 | Standard deviation of the final wins over the simulated seasons. |
| `proj_wins_p10` | Float64 | 10th percentile of the final wins over the simulated seasons. |
| `proj_wins_p90` | Float64 | 90th percentile of the final wins over the simulated seasons. |
| `proj_wins_se` | Float64 | Monte Carlo standard error of proj_wins: proj_wins_sd / sqrt(n_sims). |
| `p_division` | Float64 | Share of simulated seasons in which the team wins its division under the NBA tiebreakers; null for the WNBA. |
| `p_seed_1` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 1: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_2` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 2: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_3` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 3: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_4` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 4: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_5` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 5: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_6` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 6: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_7` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 7: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_8` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 8: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_9` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 9: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_10` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 10: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_11` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 11: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_12` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 12: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_13` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 13: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_14` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 14: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_15` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 15: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `mean_seed` | Float64 | Mean regular-season seed over the simulated seasons (1 = best). |
| `p_top4` | Float64 | Share of simulated seasons with a top-4 seed: first-round home court. |
| `p_top6` | Float64 | NBA: share of simulated seasons with a top-6 conference seed, straight to the playoffs; null for the WNBA. |
| `p_play_in` | Float64 | NBA: share of simulated seasons with a conference seed of 7 to 10, the play-in; null for the WNBA. |
| `p_playoffs` | Float64 | Share of simulated seasons in which the team makes the playoffs: NBA top 6 or through the simulated play-in; WNBA top 8. |
| `p_playoffs_se` | Float64 | Monte Carlo standard error of p_playoffs: sqrt(p (1 - p) / n_sims). |
| `p_lottery` | Float64 | Share of simulated seasons in which the team misses the playoffs and enters the draft lottery. |
| `p_pick_1` | Float64 | Share of simulated seasons in which the team's own lottery slot draws the first pick; null when the league has no published chances for that many lottery teams. |
| `p_drawn_pick` | Float64 | Share of simulated seasons in which the team's slot gets a drawn pick (NBA top 4, WNBA top 2); null as p_pick_1. |
| `mean_pick` | Float64 | Mean draft pick over the simulated seasons in which the team is in the lottery; null when it never is, or as p_pick_1. |
| `n_sims` | Int64 | Number of simulated seasons behind every share on the row. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
import datetime as dt
from sportsdataverse.nba.nba_season_sim import nba_season_sim
sim = nba_season_sim(2026, as_of_date=dt.date(2026, 2, 13))
sim.select("team_id", "wins", "proj_wins", "p_playoffs", "p_pick_1").head()

# WNBA via ``league_id``

nba_season_sim(2025, as_of_date=dt.date(2025, 7, 1), league_id="10")
```

### nba_season_standings {#nba_season_standings}

`nba_season_standings(games: 'pl.DataFrame', *, league_id: 'str' = '00', seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Standings with the league's tiebreakers from completed regular-season games.

NBA: conference seeds and division ranks under the NBA tie-break procedure (division leaders
first, postseason-eligible = a top-10 conference record). WNBA: one league table under the
WNBA procedure. The rules and their sources are in `sportsdataverse.nba.nba_season_rules`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `season`, `home_team_id`, `away_team_id` (Int64 stats.nba.com ids), `home_pts` and `away_pts`; rows without both scores are not played yet and are skipped. A `load_nba_stats_schedules` frame can be passed as is: with a `season_type` column only the `"regular-season"` rows count (the NBA Cup final, preseason, play-in and playoffs do not). |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `seed` | `int` | `0` | Seeds the drawing that ends a tie no criterion breaks. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)` that played: `conference` / `division` (NBA; the WNBA rows say `"WNBA"`), `games`, `wins`, `losses`, `win_pct`, `point_diff`, `division_rank` (1 = the division winner), `division_leader`, `seed` (rank in the conference for the NBA, in the league for the WNBA, 1 = best) and `games_back` (behind the seed-1 team). Sorted by season, conference and seed.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26); the WNBA season is its calendar year. |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `conference` | String | NBA conference (East / West); "WNBA" for every WNBA team. |
| `division` | String | NBA division; "WNBA" for every WNBA team. |
| `games` | Int64 | Regular-season games played. |
| `wins` | Int64 | Regular-season wins. |
| `losses` | Int64 | Regular-season losses. |
| `win_pct` | Float64 | Regular-season winning percentage, wins divided by games played; the first key of the seeding. |
| `point_diff` | Int64 | Points scored minus points allowed over the regular-season games. |
| `division_rank` | Int64 | Rank within the division under the NBA tiebreakers, 1 = the division winner (the WNBA rows rank the league). |
| `division_leader` | Boolean | True for the NBA division winner after the division tiebreak; False for every WNBA row. |
| `seed` | Int64 | Seed under the league's tiebreakers: rank in the conference (NBA) or the league (WNBA), 1 = best. |
| `games_back` | Float64 | Games behind the seed-1 team of the conference (NBA) or league (WNBA): ((leader wins - wins) + (losses - leader losses)) / 2. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or an NBA team id is unknown. |
| `TypeError` | If the team-id columns are not Int64. |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_stats_schedules
from sportsdataverse.nba.nba_season_rules import nba_season_standings
nba_season_standings(load_nba_stats_schedules(2025)).filter(pl.col("seed") <= 10)
```

### nba_shot_value {#nba_shot_value}

`nba_shot_value(player_ids: "'list[int]'", season: 'str', *, league_id: 'str' = '00', include_context: 'bool' = False, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

One-call shot-value spine: fetch, score, and run all five models.

Fetches each player's `shotchartdetail`, scores per-shot expected points
from the free `LeagueAverages` zone table, and returns the scored shots
plus shooter talent, selection quality, and zone-value maps (and the
defender/shot-clock context tables when `include_context=True`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_ids` | `list[int]` |  | Player ids to fetch. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `include_context` | `bool` | `False` | Also fetch + return the `playerdashptshots` defender/shot-clock context tables, once per player and team his fetched shots came from. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"shots", "talent", "selection", "zones"}` (plus `"context"` when requested). An empty fetch returns a dict of zero-row frames.

**Example**

```python
from sportsdataverse.nba import nba_shot_value
out = nba_shot_value([201939], "2022-23")
out["talent"].head()
```

### nba_shot_value_lineups {#nba_shot_value_lineups}

`nba_shot_value_lineups(group_id: 'str', season: 'str', *, team_id: 'int', league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Scored per-shot frame for one 5-man lineup (`shotchartlineupdetail`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `group_id` | `str` |  | The 5-man lineup group id (dash-joined player ids); kept `Utf8`. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `team_id` | `int` |  | The lineup's team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The lineup's shots scored by `score_shot_xpoints` (with `xpoints`). Empty fetch returns the augmented zero-row schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_shot_value_lineups
df = nba_shot_value_lineups("201939-202691-...", "2022-23", team_id=1610612744)
```

### nba_team_clutch {#nba_team_clutch}

`nba_team_clutch(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Opponent-agnostic clutch skill (shrunk clutch net-rating delta) per team.

Loads the season's clutch net rating (`nba_stats_leaguedashteamclutch`)
and full-game net baseline (`nba_stats_leaguedashteamstats`), computes
`clutch_delta`, and applies `shrink_clutch`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024` for 2023-24). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team: `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_skill_shrunk, clutch_poss`. Empty input returns that schema with zero rows.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba.nba_clutch import nba_team_clutch
skill = nba_team_clutch(2024)
skill.sort("clutch_skill_shrunk", descending=True).head()
```
