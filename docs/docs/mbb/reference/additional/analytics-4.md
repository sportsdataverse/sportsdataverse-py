---
title: "Men's college basketball — additional Python functions — Analytics: mbb_team–ncaa_mbb"
sidebar_label: "Analytics: mbb_team–ncaa_mbb"
sidebar_position: 16
description: "Men's college basketball — additional Python functions — Analytics: mbb_team–ncaa_mbb — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Analytics: mbb_team–ncaa_mbb

### mbb_team_luck {#mbb_team_luck}

`mbb_team_luck(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College luck and close-game record: Pythagorean expected wins vs actual wins, and close-game W-L.

One row per team and season. `exp_wins` is the Pythagorean expectation on the season's
final scores, `games * pts^x / (pts^x + opp_pts^x)`, with the exponent `x` fitted per
league on past seasons (`~sportsdataverse._common.team_luck.PYTHAG_EXPONENT`).
`luck_wins` is `wins - exp_wins`. A close game is one within 5 points at the start of a
possession in play in the last 5:00 of the 2nd half (4th quarter, women's, from 2016) or
overtime: one that starts there, or starts before 5:00 and ends after it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_team_luck.wbb_team_luck`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`; `team_id` is the ESPN team id (Int64). `games`, `wins`, `losses`, `pts` and `opp_pts` are from ESPN's completed games (postseason included); `win_pct`, `pythag_win_pct`, `exp_wins` and `luck_wins` (`wins - exp_wins`) follow. `pbp_games` counts the games with published NCAA possessions, and `close_games`, `close_wins`, `close_losses` and `close_win_pct` (null with no close game) are the close games among them. `pythag_win_pct`, `luck_wins` and `close_win_pct` have `<m>_rank` (1 = highest), `<m>_pct` (the Weibull percentile) and `<m>_n` (games, or close games); ranks are within the season, among teams with at least half the most games (which leaves out the non-D1 opponents; `close_win_pct` also needs possessions for half of them and five close games), and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id. Non-D1 opponents get rows of their own (a game or two) and no ranks. |
| `games` | Int64 | Games in the season's completed ESPN games, postseason included. |
| `wins` | Int64 | Wins in the season's completed ESPN games, postseason included. |
| `losses` | Int64 | Losses in the season's completed ESPN games, postseason included. |
| `pts` | Int64 | Points scored in those games (final scores). |
| `opp_pts` | Int64 | Points allowed in those games (final scores). |
| `win_pct` | Float64 | Win fraction: wins divided by games, from the final scores. |
| `pythag_win_pct` | Float64 | Pythagorean win fraction: pts^x / (pts^x + opp_pts^x), x = PYTHAG_EXPONENT['mbb'] (PYTHAG_EXPONENT['wbb'] with league='womens'), fitted on past seasons by tools/fit_team_luck_pythag.py. |
| `exp_wins` | Float64 | Expected wins: pythag_win_pct * games. |
| `luck_wins` | Float64 | Luck in wins: wins minus exp_wins. Positive means the team won more games than its scoring says it should have. |
| `pbp_games` | Int64 | Games with published possessions (ncaa_mbb_possessions); close_games is counted among these. A game whose late possessions are missing from the release counts as not close. |
| `close_games` | Int64 | Close games: at least one possession in play with 5:00 or less left (starting there, or starting before 5:00 and ending after it) in the 2nd half (the 4th quarter for league='womens' from 2016) or an overtime, the score within 5 as that possession starts (stats.nba.com's clutch window; BB-D3's close split). The margin is the running sum of the possessions' points, so it can drift from the scoreboard: NBA 2025-26 agrees with an independent ESPN play-by-play recount on 1215 of 1229 games. |
| `close_wins` | Int64 | Wins in close games. |
| `close_losses` | Int64 | Losses in close games. |
| `close_win_pct` | Float64 | close_wins / close_games; null with no close game. |
| `pythag_win_pct_rank` | Float64 | Rank of pythag_win_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers, or when every qualifier has the same value. |
| `luck_wins_rank` | Float64 | Rank of luck_wins within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers, or when every qualifier has the same value. |
| `close_win_pct_rank` | Float64 | Rank of close_win_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, have possessions for at least half their games and played at least five close games (MIN_CLOSE_GAMES); null otherwise, for a null value, or with fewer than five qualifiers, or when every qualifier has the same value. |
| `pythag_win_pct_pct` | Float64 | Percentile of pythag_win_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `luck_wins_pct` | Float64 | Percentile of luck_wins within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `close_win_pct_pct` | Float64 | Percentile of close_win_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `pythag_win_pct_n` | Int64 | Sample size of pythag_win_pct: games. |
| `luck_wins_n` | Int64 | Sample size of luck_wins: games. |
| `close_win_pct_n` | Int64 | Sample size of close_win_pct: close games. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |
| `SeasonNotFoundError` | For a season before 2010, the first with NCAA possessions. |

**Example**

```python
from sportsdataverse.mbb.mbb_team_luck import mbb_team_luck
luck = mbb_team_luck(2025)
luck.sort("luck_wins", descending=True).select("team_id", "wins", "exp_wins", "luck_wins").head()
```

### mbb_team_season_flow {#mbb_team_season_flow}

`mbb_team_season_flow(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College game flow per team and season, with ranks, percentiles and `_n`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_game_flow.wbb_team_season_flow`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)` with the columns of `sportsdataverse.nba.nba_game_flow.nba_team_season_flow`; ranks are among teams with at least half the season's most games (which leaves out the non-D1 opponents). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | ESPN team id; a non-D1 opponent has none and gets no row. |
| `games` | Int64 | Games played (completed games of the season type). |
| `wins` | Int64 | Games the team won among its completed games of the season type (pts > opp_pts). |
| `trail_pct` | Float64 | Share of the team's game clock spent behind (sum trail_sec / sum game_seconds). |
| `tied_pct` | Float64 | Share of the team's game clock spent tied. |
| `lead_changes_pg` | Float64 | Lead changes per game. |
| `times_tied_pg` | Float64 | Times tied per game. |
| `largest_lead` | Int64 | The team's biggest lead of the season. |
| `largest_comeback` | Int64 | The biggest deficit the team overcame in a win this season (0 if none). |
| `max_run` | Int64 | The team's biggest n-0 run of the season. |
| `runs_10` | Int64 | The team's runs of 10 or more unanswered points, summed over the season. |
| `opp_runs_10` | Int64 | The opponents' runs of 10 or more unanswered points against the team, summed. |
| `def_poss` | Int64 | Defensive possessions, summed. |
| `stops` | Int64 | Stops (scoreless defensive possessions), summed. |
| `kills` | Int64 | Kills (three straight stops), summed. |
| `double_kills` | Int64 | Double kills (six straight stops), summed. |
| `avg_margin` | Float64 | Game control: the score margin averaged over the season's game clock. |
| `avg_wp` | Float64 | Game control: the in-game win probability averaged over the season's game clock. |
| `lead_pct` | Float64 | Share of the team's game clock spent ahead (sum lead_sec / sum game_seconds). |
| `runs_10_pg` | Float64 | The team's 10-0 (or better) runs per game. |
| `opp_runs_10_pg` | Float64 | The opponents' 10-0 (or better) runs against the team per game. |
| `avg_longest_drought_sec` | Float64 | The team's longest scoring drought, in seconds, averaged over its games. |
| `stop_pct` | Float64 | stops / def_poss; null with no possessions. |
| `kills_pg` | Float64 | Kills per game. |
| `double_kills_pg` | Float64 | Double kills per game. |
| `kills_per_100` | Float64 | Kills per 100 defensive possessions; null with no possessions. |
| `avg_margin_rank` | Float64 | Rank of avg_margin within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_wp_rank` | Float64 | Rank of avg_wp within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `lead_pct_rank` | Float64 | Rank of lead_pct within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `runs_10_pg_rank` | Float64 | Rank of runs_10_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `opp_runs_10_pg_rank` | Float64 | Rank of opp_runs_10_pg within the season (the lowest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_longest_drought_sec_rank` | Float64 | Rank of avg_longest_drought_sec within the season (the lowest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `stop_pct_rank` | Float64 | Rank of stop_pct within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `kills_pg_rank` | Float64 | Rank of kills_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `double_kills_pg_rank` | Float64 | Rank of double_kills_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `kills_per_100_rank` | Float64 | Rank of kills_per_100 within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_margin_pct` | Float64 | Percentile of avg_margin among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_wp_pct` | Float64 | Percentile of avg_wp among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `lead_pct_pct` | Float64 | Percentile of lead_pct among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `runs_10_pg_pct` | Float64 | Percentile of runs_10_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_runs_10_pg_pct` | Float64 | Percentile of opp_runs_10_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_longest_drought_sec_pct` | Float64 | Percentile of avg_longest_drought_sec among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `stop_pct_pct` | Float64 | Percentile of stop_pct among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `kills_pg_pct` | Float64 | Percentile of kills_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `double_kills_pg_pct` | Float64 | Percentile of double_kills_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `kills_per_100_pct` | Float64 | Percentile of kills_per_100 among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_margin_n` | Int64 | Sample size of avg_margin: games. |
| `avg_wp_n` | Int64 | Sample size of avg_wp: the games with a win probability. |
| `lead_pct_n` | Int64 | Sample size of lead_pct: games. |
| `runs_10_pg_n` | Int64 | Sample size of runs_10_pg: games. |
| `opp_runs_10_pg_n` | Int64 | Sample size of opp_runs_10_pg: games. |
| `avg_longest_drought_sec_n` | Int64 | Sample size of avg_longest_drought_sec: games. |
| `stop_pct_n` | Int64 | Sample size of stop_pct: the teams defensive possessions. |
| `kills_pg_n` | Int64 | Sample size of kills_pg: the games with published possessions. |
| `double_kills_pg_n` | Int64 | Sample size of double_kills_pg: the games with published possessions. |
| `kills_per_100_n` | Int64 | Sample size of kills_per_100: the teams defensive possessions. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_game_flow import mbb_team_season_flow
mbb_team_season_flow(2026).sort("avg_wp_rank").head()
```

### mbb_team_season_rates {#mbb_team_season_rates}

`mbb_team_season_rates(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College season team rates from possessions: ORtg/DRtg/net, tempo and the four factors, with splits.

One row per team, season and split (`all`, `home`, `away`, `close`, `clutch`).
Clutch is a possession in play with 5:00 or less left (starting there, or the one live at the
5:00 mark: it starts before 5:00 and ends after it; one that ends exactly at 5:00 is out) in
the 2nd half (the 4th quarter in women's basketball from 2016) or an overtime, the score
within 5 as it starts; a close game is one with a clutch possession. The full definitions
are in `sportsdataverse._common.team_season_rates`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_team_season_rates.wbb_team_season_rates`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, split)`. `team_id` is the ESPN team id (Int64). `games`, `poss` / `opp_poss` (offensive / defensive possessions) and `pts` / `opp_pts` are the split's totals. The twelve rates are `off_rtg`, `def_rtg`, `net_rtg` (per 100 possessions), `pace` (possessions per 40 minutes), the four factors `efg_pct`, `tov_pct`, `orb_pct`, `ft_rate` and the same four against (`opp_*`), all fractions. The four factors are game-level (the NCAA team box) and null in the `clutch` split. Each rate has `<m>_rank` (1 = best; `def_rtg`, `tov_pct`, `opp_efg_pct`, `opp_orb_pct` and `opp_ft_rate` rank low first; pace ranks high first), `<m>_pct` (the Weibull percentile) and `<m>_n` (the sample: possessions for the ratings, the clock in regulation games for pace, field-goal attempts for eFG% and FT rate, the box possessions for turnover rate, rebound chances for ORB%). Ranks are within the season and split, among teams with at least half the most games (which leaves out the non-D1 opponents) whose `<m>_n` reaches the league's `~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate, and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id. Non-D1 opponents have none and get no row. |
| `split` | String | Which possessions the row covers: all; home or away (the team's home or road games; neutral-site games are in neither); close (every possession of a game that had a clutch possession); clutch (a possession in play with 5:00 or less left (starting there, or starting before 5:00 and ending after it) in the 2nd half (the 4th quarter for league='womens' from 2016) or an overtime, the score within 5 as it starts). |
| `games` | Int64 | Games with at least one possession in the split. |
| `poss` | Int64 | Offensive possessions in the split, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the split (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the split. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the split. |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 40 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Game-level, from the NCAA team box; null in the clutch split. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Game-level, from the NCAA team box; null in the clutch split. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. Game-level, from the NCAA team box; null in the clutch split. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. Game-level, from the NCAA team box; null in the clutch split. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Game-level, from the NCAA team box; null in the clutch split. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. Game-level, from the NCAA team box; null in the clutch split. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction (one minus the team's defensive rebound rate). Game-level, from the NCAA team box; null in the clutch split. |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. Game-level, from the NCAA team box; null in the clutch split. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose off_rtg_n is at least 270 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose def_rtg_n is at least 460 defensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose net_rtg_n is at least 430 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season and split (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the split, whose pace_n is at least 4 games (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose efg_pct_n is at least 500 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose tov_pct_n is at least 450 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose orb_pct_n is at least 160 offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose ft_rate_n is at least 530 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_efg_pct_n is at least 630 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_tov_pct_n is at least 340 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_orb_pct_n is at least 330 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_ft_rate_n is at least 450 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `pace_pct` | Float64 | Percentile of pace within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate within the season and split among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: the team's offensive possessions in the split. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: the team's defensive possessions in the split. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: the team's offensive plus defensive possessions in the split. |
| `pace_n` | Int64 | Sample size of pace: the game clock the split's possessions used, in regulation games (48 minutes, 40 in the WNBA and college), rounded: a full game counts 1, a clutch split's five-minute windows about 0.1 each. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the split, 0 when the rate is null. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the NCAA team box's possessions (o_poss) in the split, 0 when the rate is null. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the split, 0 when the rate is null. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the split, 0 when the rate is null. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the split, 0 when the rate is null. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the opponents' NCAA team-box possessions in the split, 0 when the rate is null. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the split, 0 when the rate is null. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the split, 0 when the rate is null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_team_season_rates import mbb_team_season_rates
rates = mbb_team_season_rates(2025)
rates.filter(split="all").sort("net_rtg_rank").head()
```

### mbb_team_strength {#mbb_team_strength}

`mbb_team_strength(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College team strength: adjusted ratings, SOS, SOV, tempo and the four factors.

One row per team and season, with two adjusted-rating methods. `adj_*_rtg` are the
published college ratings (`load_mbb_ratings` / `load_wbb_ratings`, a fixed point on ESPN
box scores). `adj_*_rtg_ridge` are the NBA / WNBA possession ridge on the stats.ncaa.org
possessions (points per 100 against an average Division I opponent on a neutral court; home
court fitted, neutral sites from ESPN, non-Division I opponents left out). SOS is the mean
opponent `adj_net_rtg` over the team's completed ESPN games (unrated opponents left out),
SOV the same over its wins. Bracketology and the season sims keep the fixed point.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_team_strength.wbb_team_strength`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`; `team_id` is the ESPN team id (Int64). `games`, `poss` / `opp_poss`, `pts` / `opp_pts` are the season totals from the NCAA possessions; `wins` / `losses` come from ESPN's completed games. The rated metrics are `adj_off_rtg`, `adj_def_rtg`, `adj_net_rtg`, `sos`, `sov`, `adj_off_rtg_ridge`, `adj_def_rtg_ridge`, `adj_net_rtg_ridge` and the BB-D3 rates (`off_rtg`, `def_rtg`, `net_rtg`, `pace`, the four factors and the four against). Each has `<m>_rank` (1 = best; `adj_def_rtg`, `adj_def_rtg_ridge`, `def_rtg`, `tov_pct` and the opponent shooting, rebounding and free-throw rates rank low first; `sos` ranks the hardest schedule first), `<m>_pct` (the Weibull percentile) and `<m>_n` (possessions for the ratings, rated games for SOS, the clock in regulation games for pace, wins over rated opponents for SOV). Ranks are within the season, among teams with at least half the most games (which leaves out the non-D1 opponents), the BB-D3 rates' also behind their `~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate, and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id. Non-D1 opponents have none and get no row. |
| `games` | Int64 | Games with at least one possession in the season (from the possession release). |
| `wins` | Int64 | Wins in the season's completed ESPN games. |
| `losses` | Int64 | Losses in the season's completed ESPN games. |
| `poss` | Int64 | Offensive possessions in the season, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the season (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the season. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the season. |
| `adj_off_rtg` | Float64 | Adjusted offensive rating: points per 100 possessions against an average defense, from the published college ratings (adj_o, the mbb_team_ratings fixed point). |
| `adj_def_rtg` | Float64 | Adjusted defensive rating: points allowed per 100 possessions against an average offense, from the published college ratings (adj_d); lower is better. |
| `adj_net_rtg` | Float64 | Adjusted net rating: adj_off_rtg minus adj_def_rtg. |
| `sos` | Float64 | Strength of schedule: the mean adj_net_rtg of the opponents of the team's completed games, one weight per game (an opponent met four times counts four times). Opponents with no rating are left out. |
| `sov` | Float64 | Strength of victory: the mean adj_net_rtg of the opponents the team beat, one weight per win; null with no win. |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 40 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Game-level, from the NCAA team box. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Game-level, from the NCAA team box. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. Game-level, from the NCAA team box. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. Game-level, from the NCAA team box. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Game-level, from the NCAA team box. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. Game-level, from the NCAA team box. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction (one minus the team's defensive rebound rate). Game-level, from the NCAA team box. |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. Game-level, from the NCAA team box. |
| `adj_off_rtg_rank` | Float64 | Rank of adj_off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_def_rtg_rank` | Float64 | Rank of adj_def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_net_rtg_rank` | Float64 | Rank of adj_net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `sos_rank` | Float64 | Rank of sos within the season (the highest value ranks 1; the hardest schedule ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `sov_rank` | Float64 | Rank of sov within the season (the highest value ranks 1; the strongest wins rank 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose off_rtg_n is at least 270 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose def_rtg_n is at least 460 defensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose net_rtg_n is at least 430 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the season, whose pace_n is at least 4 games (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose efg_pct_n is at least 500 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose tov_pct_n is at least 450 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose orb_pct_n is at least 160 offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose ft_rate_n is at least 530 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_efg_pct_n is at least 630 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_tov_pct_n is at least 340 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_orb_pct_n is at least 330 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_ft_rate_n is at least 450 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `adj_off_rtg_pct` | Float64 | Percentile of adj_off_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_def_rtg_pct` | Float64 | Percentile of adj_def_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_net_rtg_pct` | Float64 | Percentile of adj_net_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `sos_pct` | Float64 | Percentile of sos within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `sov_pct` | Float64 | Percentile of sov within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `pace_pct` | Float64 | Percentile of pace within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_off_rtg_n` | Int64 | Sample size of adj_off_rtg: the team's offensive possessions in the season. |
| `adj_def_rtg_n` | Int64 | Sample size of adj_def_rtg: the team's defensive possessions in the season. |
| `adj_net_rtg_n` | Int64 | Sample size of adj_net_rtg: the team's offensive plus defensive possessions in the season. |
| `sos_n` | Int64 | Sample size of sos: the completed games against a rated opponent. |
| `sov_n` | Int64 | Sample size of sov: the wins against a rated opponent. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: the team's offensive possessions in the season. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: the team's defensive possessions in the season. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: the team's offensive plus defensive possessions in the season. |
| `pace_n` | Int64 | Sample size of pace: the game clock the season's possessions used, in regulation games (48 minutes, 40 in the WNBA and college), rounded: about the games played, overtime adding a little. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the season, 0 when the rate is null. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the NCAA team box's possessions (o_poss) in the season, 0 when the rate is null. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the season, 0 when the rate is null. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the season, 0 when the rate is null. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the season, 0 when the rate is null. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the opponents' NCAA team-box possessions in the season, 0 when the rate is null. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the season, 0 when the rate is null. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the season, 0 when the rate is null. |
| `adj_off_rtg_ridge` | Float64 | Adjusted offensive rating, possession-ridge method: points per 100 possessions against an average Division I defense on a neutral court. The NBA / WNBA ridge (possession_ridge_ratings) on the ncaa_mbb_possessions release, penalty COLLEGE_RIDGE_LAMBDA, home court fitted and ESPN neutral sites; possessions against a non-Division I opponent left out. Published beside the fixed-point adj_off_rtg, which it does not replace. |
| `adj_def_rtg_ridge` | Float64 | Adjusted defensive rating, possession-ridge method: points allowed per 100 possessions against an average Division I offense on a neutral court; lower is better. |
| `adj_net_rtg_ridge` | Float64 | Adjusted net rating, possession-ridge method: adj_off_rtg_ridge minus adj_def_rtg_ridge. |
| `adj_off_rtg_ridge_rank` | Float64 | Rank of adj_off_rtg_ridge within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_def_rtg_ridge_rank` | Float64 | Rank of adj_def_rtg_ridge within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_net_rtg_ridge_rank` | Float64 | Rank of adj_net_rtg_ridge within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_off_rtg_ridge_pct` | Float64 | Percentile of adj_off_rtg_ridge within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_def_rtg_ridge_pct` | Float64 | Percentile of adj_def_rtg_ridge within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_net_rtg_ridge_pct` | Float64 | Percentile of adj_net_rtg_ridge within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `adj_off_rtg_ridge_n` | Int64 | Sample size of adj_off_rtg_ridge: the team's offensive possessions in the season, counting any against a non-Division I opponent that the fit leaves out. |
| `adj_def_rtg_ridge_n` | Int64 | Sample size of adj_def_rtg_ridge: the team's defensive possessions in the season, counting any against a non-Division I opponent that the fit leaves out. |
| `adj_net_rtg_ridge_n` | Int64 | Sample size of adj_net_rtg_ridge: the team's offensive plus defensive possessions in the season, counting any against a non-Division I opponent that the fit leaves out. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_team_strength import mbb_team_strength
strength = mbb_team_strength(2025)
strength.sort("adj_net_rtg_rank").select("team_id", "adj_net_rtg", "sos", "sov").head()

# The two methods side by side

strength.select("team_id", "adj_net_rtg_rank", "adj_net_rtg_ridge_rank").head()
```

### mbb_tournament_ineligible {#mbb_tournament_ineligible}

`mbb_tournament_ineligible(seasons: 'list[int] | None' = None, *, league: 'str | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Division I basketball programs that could not play in the NCAA tournament, per season.

A bundled, hand-curated list (2010-2027): programs reclassifying to Division I (four seasons
from Division II until 2025, three since the NCAA's January 2025 rule; St. Thomas came from
Division III), and APR, NCAA-infractions and self-imposed postseason bans. Every row cites
its source. The college season products (bracketology, season sim, strength of schedule) use
it by default to set `eligible`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int] \| None` | `None` | Seasons to keep, as the end year (`2024` is 2023-24). `None` keeps all. |
| `league` | `str \| None` | `None` | `"mens"`, `"womens"`, or `None` for both. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per ineligible `(season, league, team_id)`: `season, league, team_id` (ESPN team id, Utf8), `team`, `reason` (`reclassifying`, `apr_ban`, `infractions_ban`, `self_imposed_ban` or `other`), `detail`, `source_url` and `confidence` (`high` / `medium`).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, as the end year (2024 is 2023-24). |
| `league` | String | mens or womens. |
| `team_id` | String | ESPN team id (Utf8). |
| `team` | String | School name of the barred program, as ESPN spells it. |
| `reason` | String | Why the program was barred: reclassifying, apr_ban, infractions_ban, self_imposed_ban or other. |
| `detail` | String | Short note on the row, such as the reclassification year or the case behind the ban. |
| `source_url` | String | The source that documents the row. |
| `confidence` | String | How well the source documents the row: high (the NCAA, the school or the conference says so) or medium (a secondary source). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"`, `"womens"` or `None`. |

**Example**

```python
from sportsdataverse.mbb import mbb_tournament_ineligible
barred = mbb_tournament_ineligible([2023], league="mens")

# Pipeline next step (one line)

barred.filter(pl.col("reason") == "reclassifying").get_column("team").to_list()
```

### ncaa_coach_stints {#ncaa_coach_stints}

`ncaa_coach_stints(coaches: 'pl.DataFrame', records: 'pl.DataFrame') -> 'pl.DataFrame'`

The card coach of each team-season as a whole-season stint, unless the card says the season changed hands.

A card whose season record (`coach_season_records`) is short of the team's by more than
a game and more than a loss marks a mid-season change, and its figures are not the card
coach's own games: on Sports-Reference they are the other coach's (Ole Miss 2017-18's card
names Andy Kennedy with 1-4, Tony Madlock's five games; Kennedy went 11-16). Such a team-season
gets no stint, so it stays uncredited (and is counted in a log line). Otherwise -- the record
matches, is a loss off, drops vacated wins (never losses), or cannot be computed -- the card
coach has the whole season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `coaches` | `DataFrame` |  | `ncaa_team_coaches` rows, with the season before each wanted one for the record difference. |
| `records` | `DataFrame` |  | The teams' season records: `season`, `team_id`, `games`, `losses` (as `sportsdataverse._common.team_luck.season_records` gives them). |

**Returns**

Stints in `~sportsdataverse._common.coach_seasons.STINTS_SCHEMA` with no counts and `position="any"`, one per team-season with a card, an ESPN team id (cards with none are counted in a log line) and no mid-season change.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id of the school (from the card's NCAA team-season crosswalk). |
| `coach_id` | Int64 | NCAA person id of the card's head coach (stats.ncaa.org /people/{id}). |
| `coach` | String | Head coach's name on the card. |
| `games` | Int64 | Always null here; the card coach has the whole season (a season the card shows changing hands has no stint). |
| `wins` | Int64 | Always null here, as games. |
| `position` | String | Always any; one whole-season stint per team-season. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If two cards name a coach for the same (season, team_id). |

### ncaa_impact_rows {#ncaa_impact_rows}

`ncaa_impact_rows(pbp: 'pl.DataFrame', possessions: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map the stats.ncaa.org pbp and possessions onto the game-impact row frame.

One row per event (the clock since the previous event in the period and the points scored on
it, on that event's lineup; a free throw on the lineup at the last foul) and one per
possession (its lineup and which side had it, `poss_team == home`). The clock left in a
period after its last event goes to that event's lineup, so each period's clock is whole. The
possessions of a contest with no pbp events, and those whose `poss_team` is neither side,
are dropped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order, with the cumulative `game_seconds` and the `period`. |
| `possessions` | `DataFrame` |  | `load_ncaa_mbb_possessions` / `load_ncaa_wbb_possessions` rows. |
| `league` | `str` | `'mens'` | `"mens"` (20-minute halves) or `"womens"` (10-minute quarters from 2016, halves before); overtimes are 5 minutes. |

**Returns**

Rows in `sportsdataverse._common.game_impact.impact_row_schema` with String player ids.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the action or possession. |
| `game_id` | String | stats.ncaa.org contest id. |
| `home_team_id` | Int64 | Home ESPN team id (null for a non-D1 team). |
| `away_team_id` | Int64 | Away ESPN team id (null for a non-D1 team). |
| `home_1` | String | Home player 1 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_2` | String | Home player 2 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_3` | String | Home player 3 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_4` | String | Home player 4 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_5` | String | Home player 5 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_1` | String | Away player 1 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_2` | String | Away player 2 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_3` | String | Away player 3 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_4` | String | Away player 4 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_5` | String | Away player 5 on the floor (on a free throw, the player on the floor at the last foul). |
| `seconds` | Float64 | Game clock that ran into this action (0 on a possession row). |
| `home_pts` | Int64 | Points the home team scored on this action (0 on a possession row). |
| `away_pts` | Int64 | Points the away team scored on this action (0 on a possession row). |
| `home_poss` | Int64 | 1 on a possession row where the home team has the ball, else 0. |
| `away_poss` | Int64 | 1 on a possession row where the away team has the ball, else 0. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp, load_ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_game_impact import ncaa_impact_rows
rows = ncaa_impact_rows(load_ncaa_mbb_pbp(2025), load_ncaa_mbb_possessions(2025))
```

### ncaa_lineup_count_rows {#ncaa_lineup_count_rows}

`ncaa_lineup_count_rows(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Map the stats.ncaa.org pbp onto the lineup-seasons count frame.

Each shot, free throw, rebound and turnover counts for the side whose `event_team` it is,
on the lineup at the event; a free throw on the lineup at the most recent foul
(`sportsdataverse._common.game_impact.credit_free_throws_to_foul_lineup`). Events with
no count are dropped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order. |

**Returns**

One row per counted event in `sportsdataverse._common.lineup_seasons.lineup_count_schema` (with the shot zones) and String player ids.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.ncaa.org contest id. |
| `home_team_id` | Int64 | ESPN team id of the home side. |
| `away_team_id` | Int64 | ESPN team id of the away side. |
| `home_1` | String | stats.ncaa.org id of home player 1 on the floor at the event (a free throw: at the last foul). |
| `home_2` | String | stats.ncaa.org id of home player 2 on the floor at the event (a free throw: at the last foul). |
| `home_3` | String | stats.ncaa.org id of home player 3 on the floor at the event (a free throw: at the last foul). |
| `home_4` | String | stats.ncaa.org id of home player 4 on the floor at the event (a free throw: at the last foul). |
| `home_5` | String | stats.ncaa.org id of home player 5 on the floor at the event (a free throw: at the last foul). |
| `away_1` | String | stats.ncaa.org id of away player 1 on the floor at the event (a free throw: at the last foul). |
| `away_2` | String | stats.ncaa.org id of away player 2 on the floor at the event (a free throw: at the last foul). |
| `away_3` | String | stats.ncaa.org id of away player 3 on the floor at the event (a free throw: at the last foul). |
| `away_4` | String | stats.ncaa.org id of away player 4 on the floor at the event (a free throw: at the last foul). |
| `away_5` | String | stats.ncaa.org id of away player 5 on the floor at the event (a free throw: at the last foul). |
| `home_fga` | Int64 | The home side's field-goal attempts on the event (0 or 1). |
| `home_fgm` | Int64 | The home side's field goals made on the event (0 or 1). |
| `home_fg3a` | Int64 | The home side's three-point attempts on the event (0 or 1). |
| `home_fg3m` | Int64 | The home side's threes made on the event (0 or 1). |
| `home_fta` | Int64 | The home side's free-throw attempts on the event (0 or 1). |
| `home_oreb` | Int64 | The home side's offensive rebounds on the event (0 or 1). |
| `home_dreb` | Int64 | The home side's defensive rebounds on the event (0 or 1). |
| `home_tov` | Int64 | The home side's turnovers on the event (0 or 1). |
| `home_rima` | Int64 | The home side's rim attempts on the event (0 or 1). |
| `home_rimm` | Int64 | The home side's rim makes on the event (0 or 1). |
| `home_mida` | Int64 | The home side's mid-range attempts on the event (0 or 1). |
| `home_midm` | Int64 | The home side's mid-range makes on the event (0 or 1). |
| `away_fga` | Int64 | The away side's field-goal attempts on the event (0 or 1). |
| `away_fgm` | Int64 | The away side's field goals made on the event (0 or 1). |
| `away_fg3a` | Int64 | The away side's three-point attempts on the event (0 or 1). |
| `away_fg3m` | Int64 | The away side's threes made on the event (0 or 1). |
| `away_fta` | Int64 | The away side's free-throw attempts on the event (0 or 1). |
| `away_oreb` | Int64 | The away side's offensive rebounds on the event (0 or 1). |
| `away_dreb` | Int64 | The away side's defensive rebounds on the event (0 or 1). |
| `away_tov` | Int64 | The away side's turnovers on the event (0 or 1). |
| `away_rima` | Int64 | The away side's rim attempts on the event (0 or 1). |
| `away_rimm` | Int64 | The away side's rim makes on the event (0 or 1). |
| `away_mida` | Int64 | The away side's mid-range attempts on the event (0 or 1). |
| `away_midm` | Int64 | The away side's mid-range makes on the event (0 or 1). |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp
from sportsdataverse.mbb.mbb_lineup_seasons import ncaa_lineup_count_rows
rows = ncaa_lineup_count_rows(load_ncaa_mbb_pbp(2025))
```

### ncaa_mbb_lineups {#ncaa_mbb_lineups}

`ncaa_mbb_lineups(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-lineup stats.

Port of bigballR `get_lineups` (`all_functions.R:1945-2521`). Rows
with any missing on-court player and substitution rows are dropped, each
row's home/away five are byte-sorted so a lineup always occupies the same
columns, and the home + away passes are combined per `(p1..p5, team)`.
Ratios are derived from the summed counters, rounded to 3 decimals, and
NA/Inf are zeroed exactly where R does it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`parse_ncaa_bb_game_pbp` output). May span multiple games. |
| `include_transition` | `bool` | `False` | When True, append the `_trans`/`_half` split surface plus `o_trans_pct`/`d_trans_pct` (213 columns total). |
| `fix_tip_in` | `bool` | `True` | When True (default), rim stats count the scrape engine's real `"Tip In"` vocabulary. When False, reproduce R's literal `"Tip-In"` test (`all_functions.R:2012`) — tip-ins silently excluded — for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per lineup+team — 75 columns (`LINEUPS_COLUMNS`) or 213 with `include_transition=True` (`LINEUPS_TRANSITION_COLUMNS`), rows sorted by `p1..p5, team`. Empty input yields an empty frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `p1` | String |  |
| `p2` | String |  |
| `p3` | String |  |
| `p4` | String |  |
| `p5` | String |  |
| `team` | String | Team-side label or team identifier. |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_lineups
lineups = ncaa_mbb_lineups(pbp)
print(lineups.shape)

# Transition/half-court splits, pandas out

df_pd = ncaa_mbb_lineups(pbp, include_transition=True, return_as_pandas=True)

# Pipeline next step (one line)

lineups.filter(pl.col("mins") > 10).sort("netrtg", descending=True).head()
```
