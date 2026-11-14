# Women's college basketball — additional Python functions — Analytics: wbb_lineup–wbb_player

> Women's college basketball — additional Python functions — Analytics: wbb_lineup–wbb_player — function reference in sdv-py, the SportsDataverse Python package.

### wbb_lineup_seasons {#wbb_lineup_seasons}

`wbb_lineup_seasons(seasons: 'int | list[int]', *, min_poss: 'int' = 200, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college lineup seasons: 2-, 3- and 5-man units with ratings, four factors and shot zones.

Delegates to `sportsdataverse.mbb.mbb_lineup_seasons.mbb_lineup_seasons` with
`league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `min_poss` | `int` | `200` | The rank gate, offensive plus defensive possessions (default 200). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, n_players, lineup_id)`: `player_1..5`, `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus`, the ratings, the four factors and shot zones for and against, each with `_rank` / `_pct` / `_n`. See the mbb core for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id of the unit's team. |
| `n_players` | Int64 | Unit size: 2, 3 or 5 players. |
| `lineup_id` | String | The unit's stats.ncaa.org player ids, sorted as text and joined with '-'. |
| `player_1` | String | Player 1 of the unit, in lineup_id order; null past n_players. |
| `player_2` | String | Player 2 of the unit, in lineup_id order; null past n_players. |
| `player_3` | String | Player 3 of the unit, in lineup_id order; null past n_players. |
| `player_4` | String | Player 4 of the unit, in lineup_id order; null past n_players. |
| `player_5` | String | Player 5 of the unit, in lineup_id order; null past n_players. |
| `minutes` | Float64 | Game-clock minutes the unit shared the floor over the season. |
| `poss` | Int64 | Offensive possessions with the unit on the floor (a possession counts for the five on the floor when it starts; a 2- or 3-man unit sums the fives that contain it). |
| `opp_poss` | Int64 | Defensive possessions with the unit on the floor, counted as poss is. |
| `pts` | Int64 | The team's points with the unit on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts` | Int64 | The opponent's points with the unit on the floor, credited as pts is. |
| `plus_minus` | Int64 | The unit's +/- over the season: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss. Null with a zero denominator. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss. Null with a zero denominator. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `orb_pct` | Float64 | Offensive-rebound rate: OREB / (OREB + the opponent's DREB), as a fraction. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `ft_rate` | Float64 | Free-throw rate: FTA / FGA. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_efg_pct` | Float64 | The opponent's effective field-goal percentage with the unit on the floor. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_tov_pct` | Float64 | The opponent's turnovers per defensive possession. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_orb_pct` | Float64 | The opponent's offensive-rebound rate: its OREB / (its OREB + the unit's DREB). Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_ft_rate` | Float64 | The opponent's free-throw rate: FTA / FGA. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `rim_rate` | Float64 | Share of the unit's field-goal attempts at the rim (dunk, layup, hook, tip-in). Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `mid_rate` | Float64 | Share of the unit's field-goal attempts that are other twos (mid-range). Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `fg3_rate` | Float64 | Share of the unit's field-goal attempts that are threes. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `rim_pct` | Float64 | Field-goal percentage at the rim, as a fraction. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `mid_pct` | Float64 | Mid-range field-goal percentage, as a fraction. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `fg3_pct` | Float64 | Three-point percentage, as a fraction. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_rim_rate` | Float64 | Share of the opponent's field-goal attempts at the rim with the unit on the floor. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_mid_rate` | Float64 | Share of the opponent's field-goal attempts that are mid-range twos. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_fg3_rate` | Float64 | Share of the opponent's field-goal attempts that are threes. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_rim_pct` | Float64 | The opponent's field-goal percentage at the rim. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_mid_pct` | Float64 | The opponent's mid-range field-goal percentage. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `opp_fg3_pct` | Float64 | The opponent's three-point percentage. Null with a zero denominator. Each event counts for the lineup on the floor at it (a free throw: the lineup at the last foul). |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose off_rtg_n is at least 130 offensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose def_rtg_n is at least 470 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose net_rtg_n is at least 360 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose efg_pct_n is at least 230 field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose tov_pct_n is at least 210 offensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose orb_pct_n is at least 160 offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose ft_rate_n is at least 550 field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose opp_efg_pct_n is at least 600 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose opp_tov_pct_n is at least 300 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose opp_orb_pct_n is at least 240 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose opp_ft_rate_n is at least 420 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `rim_rate_rank` | Float64 | Rank of rim_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `mid_rate_rank` | Float64 | Rank of mid_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `fg3_rate_rank` | Float64 | Rank of fg3_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `rim_pct_rank` | Float64 | Rank of rim_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `mid_pct_rank` | Float64 | Rank of mid_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `fg3_pct_rank` | Float64 | Rank of fg3_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose fg3_pct_n is at least 350 three-point attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_rim_rate_rank` | Float64 | Rank of opp_rim_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_mid_rate_rank` | Float64 | Rank of opp_mid_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_fg3_rate_rank` | Float64 | Rank of opp_fg3_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_rim_pct_rank` | Float64 | Rank of opp_rim_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_mid_pct_rank` | Float64 | Rank of opp_mid_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_fg3_pct_rank` | Float64 | Rank of opp_fg3_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), and never for opp_fg3_pct: its reliability never comes near half signal on a season's sample (rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
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
| `rim_rate_pct` | Float64 | Percentile of rim_rate among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_rate_pct` | Float64 | Percentile of mid_rate among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `fg3_rate_pct` | Float64 | Percentile of fg3_rate among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_pct_pct` | Float64 | Percentile of rim_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_pct_pct` | Float64 | Percentile of mid_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `fg3_pct_pct` | Float64 | Percentile of fg3_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_rate_pct` | Float64 | Percentile of opp_rim_rate among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_rate_pct` | Float64 | Percentile of opp_mid_rate among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_fg3_rate_pct` | Float64 | Percentile of opp_fg3_rate among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_pct_pct` | Float64 | Percentile of opp_rim_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_pct_pct` | Float64 | Percentile of opp_mid_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_fg3_pct_pct` | Float64 | Percentile of opp_fg3_pct among the same qualified units: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
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
| `rim_rate_n` | Int64 | Sample size of rim_rate: the unit's field-goal attempts. |
| `mid_rate_n` | Int64 | Sample size of mid_rate: the unit's field-goal attempts. |
| `fg3_rate_n` | Int64 | Sample size of fg3_rate: the unit's field-goal attempts. |
| `rim_pct_n` | Int64 | Sample size of rim_pct: the unit's rim attempts. |
| `mid_pct_n` | Int64 | Sample size of mid_pct: the unit's mid-range attempts. |
| `fg3_pct_n` | Int64 | Sample size of fg3_pct: the unit's three-point attempts. |
| `opp_rim_rate_n` | Int64 | Sample size of opp_rim_rate: the opponent's field-goal attempts. |
| `opp_mid_rate_n` | Int64 | Sample size of opp_mid_rate: the opponent's field-goal attempts. |
| `opp_fg3_rate_n` | Int64 | Sample size of opp_fg3_rate: the opponent's field-goal attempts. |
| `opp_rim_pct_n` | Int64 | Sample size of opp_rim_pct: the opponent's rim attempts. |
| `opp_mid_pct_n` | Int64 | Sample size of opp_mid_pct: the opponent's mid-range attempts. |
| `opp_fg3_pct_n` | Int64 | Sample size of opp_fg3_pct: the opponent's three-point attempts. |
| `pts_per75` | Float64 | Per 75 possessions: 0.75 * off_rtg (BB-G8's per-75 basis). A unit has no per-game basis: it plays stretches of many games, never a game of its own. |
| `opp_pts_per75` | Float64 | Per 75 possessions: 0.75 * def_rtg (BB-G8's per-75 basis). A unit has no per-game basis: it plays stretches of many games, never a game of its own. |
| `plus_minus_per75` | Float64 | Per 75 possessions: 0.75 * net_rtg (BB-G8's per-75 basis). A unit has no per-game basis: it plays stretches of many games, never a game of its own. |
| `pts_per75_rank` | Float64 | Rank of pts_per75: off_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `opp_pts_per75_rank` | Float64 | Rank of opp_pts_per75: def_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `plus_minus_per75_rank` | Float64 | Rank of plus_minus_per75: net_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `pts_per75_pct` | Float64 | Percentile of pts_per75: off_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `opp_pts_per75_pct` | Float64 | Percentile of opp_pts_per75: def_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `plus_minus_per75_pct` | Float64 | Percentile of plus_minus_per75: net_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `pts_per75_n` | Int64 | Sample size of pts_per75: off_rtg_n, copied. |
| `opp_pts_per75_n` | Int64 | Sample size of opp_pts_per75: def_rtg_n, copied. |
| `plus_minus_per75_n` | Int64 | Sample size of plus_minus_per75: net_rtg_n, copied. |

**Example**

```python
from sportsdataverse.wbb import wbb_lineup_seasons
wbb_lineup_seasons(2025).filter(n_players=5).sort("poss").tail()
```

### wbb_official_seasons {#wbb_official_seasons}

`wbb_official_seasons(seasons: 'int | list[int]', *, season_type: 'int | None' = 2, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college official season profiles: games, fouls and FTA per game, pace and home splits.

One row per official and season over the completed games of `season_type` the official
worked. `fouls_per_game` and `fta_per_game` are both teams' totals from ESPN's team box;
`pace` is possessions (FGA - ORB + TOV + 0.44 FTA, averaged over the two teams) per 40
minutes, over the games' minutes including overtime; `home_win_pct` and
`home_minus_away_fouls` (home fouls less away fouls, per game) leave out neutral-site
games. Every rate has `<m>_n`, the games it is computed over. The release keys officials by
ESPN's `official_id`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). |
| `season_type` | `int \| None` | `2` | ESPN's season type: `2` regular season (default), `3` postseason; `None` keeps every type. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, official)`: `season`, `official_id` (Int64), `official_name`, `games`, `fouls_per_game`, `fta_per_game`, `pace`, `home_win_pct` and `home_minus_away_fouls`, each rate with its `_n`. A season without an officials release returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `official_id` | Int64 | ESPN official id where the officials release carries one (WNBA, WBB); null where it has names only (NBA, MBB). |
| `official_name` | String | Official's name, whitespace-normalised; the key when official_id is null. |
| `games` | Int64 | Completed games of the season type the officials table lists the official on. |
| `fouls_per_game` | Float64 | Both teams' fouls per game (ESPN team box fouls), over fouls_per_game_n games. |
| `fouls_per_game_n` | Int64 | Games with a two-team box behind fouls_per_game. |
| `fta_per_game` | Float64 | Both teams' free-throw attempts per game, over fta_per_game_n games. |
| `fta_per_game_n` | Int64 | Games with a two-team box behind fta_per_game. |
| `pace` | Float64 | Possessions (FGA - ORB + TOV + 0.44 FTA, averaged over the two teams) per regulation length (48 minutes NBA, 40 otherwise), over the games' minutes including overtime. |
| `pace_n` | Int64 | Games with a box possession estimate and a known last period behind pace. |
| `home_win_pct` | Float64 | Share of the official's non-neutral-site games with a recorded winner that the home team won. |
| `home_win_pct_n` | Int64 | Non-neutral-site games with a recorded home winner behind home_win_pct. |
| `home_minus_away_fouls` | Float64 | Home team's fouls minus the away team's, per non-neutral-site game. |
| `home_minus_away_fouls_n` | Int64 | Non-neutral-site games with a two-team box behind home_minus_away_fouls. |

**Example**

```python
from sportsdataverse.wbb.wbb_official_seasons import wbb_official_seasons
refs = wbb_official_seasons(2026)
refs.sort("fouls_per_game", descending=True).select("official_name", "games", "fouls_per_game").head()
```

### wbb_play_styles {#wbb_play_styles}

`wbb_play_styles(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college player play-style archetypes: box, shot and lineup-role features (BB-D20).

Delegates to `sportsdataverse.mbb.mbb_play_style.mbb_play_styles` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons (2025+). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per qualifying `(player_id, season, team_id)`: the ESPN ids, `player`, `min` and the `archetype`, `cluster`, `dist_to_center`, `second_archetype` and `margin`.

| col_name | type | description |
|---|---|---|
| `player_id` | String | ESPN athlete id of the player, as text (the box and shots releases' athlete id). |
| `season` | Int64 | Season (2026 is 2025-26). |
| `team_id` | String | ESPN team id of the team the player-season belongs to, as text. |
| `player` | String | Player's display name on ESPN. |
| `min` | Float64 | Season minutes; the qualifier is a quarter of the season's most minutes. |
| `archetype` | String | Play-style archetype: the label of the nearest frozen KMeans center to the player-season's within-season z-scored box, shot and lineup-role features. |
| `cluster` | Int64 | Index of the archetype's center in the bundled artifact (0-based). |
| `dist_to_center` | Float64 | Euclidean distance (z-space) to the archetype's center; small means a prototypical example. |
| `second_archetype` | String | The next-nearest archetype, the style a hybrid leans toward. |
| `margin` | Float64 | Distance to the second center minus distance to the nearest; small means a hybrid. |

**Example**

```python
from sportsdataverse.wbb import wbb_play_styles
wbb_play_styles(2026).group_by("archetype").len()
```

### wbb_player_game_expected_box {#wbb_player_game_expected_box}

`wbb_player_game_expected_box(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college points, rebounds and assists per player-game against his season, per minute and usage-adjusted.

For every played player-game: `<s>_base` is his season per-minute rate times the game's
minutes; `<s>_exp` scales that by the game's usage and minutes against his season's
(a Poisson model fitted on earlier seasons, scored on later held-out ones). Both use the
season without this game (over every game of the season), so a game is never in its own baseline.
`<s>_exp` is null before the model's first held-out season and where a usage is unknown.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (2026 is 2025-26). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(game_id, athlete_id)`: `season`, `season_type`, `team_id`, `minutes`, `usg_pct`, `pts` / `reb` / `ast`, the season-without-the-game `rate_games` / `rate_minutes` / `rate_usg_pct`, and `pts_base` / `reb_base` / `ast_base` / `pts_exp` / `reb_exp` / `ast_exp`. A season without a release returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `season_type` | Int64 | ESPN season type of the game (2 regular season, 3 postseason, 5 NBA play-in). |
| `game_id` | Int64 | ESPN game id, the join key to the schedule and the player box score. |
| `team_id` | Int64 | ESPN team id of the player's team in the game. |
| `athlete_id` | Int64 | ESPN athlete id of the player, the join key to the player box score. |
| `minutes` | Float64 | Minutes played in the game (above 0: a player who did not play has no row). |
| `usg_pct` | Float64 | Usage: 100 x uses x (team minutes / 5) / (minutes x team uses), uses = FGA + 0.44 FTA + TOV, the team sums over that team-game's players; null where a player of the team-game lacks a count. |
| `pts` | Int64 | Points in the game. |
| `reb` | Int64 | Rebounds in the game. |
| `ast` | Int64 | Assists in the game. |
| `rate_games` | Int64 | Games of the season without this one (the pros' regular season, every college game) that the expectations read. |
| `rate_minutes` | Float64 | Minutes of those games. |
| `rate_usg_pct` | Float64 | Season usage without this game: 100 x sum(uses) / sum(minutes x team uses / (team minutes / 5)); null where no game of it has a usage. |
| `pts_base` | Float64 | Expected points at the per-minute rate: the season-without-this-game points per minute times the game's minutes; null when that season has no minutes. |
| `reb_base` | Float64 | Expected rebounds at the per-minute rate, as pts_base. |
| `ast_base` | Float64 | Expected assists at the per-minute rate, as pts_base. |
| `pts_exp` | Float64 | Usage-adjusted expected points: pts_base x exp(a + b_usg ln(usg_pct / rate_usg_pct) + b_min ln(minutes / rate minutes per game)), a Poisson fit on earlier seasons (sportsdataverse._common.player_box_expected); null before the model's first held-out season or without both usages. |
| `reb_exp` | Float64 | Usage-adjusted expected rebounds, as pts_exp. |
| `ast_exp` | Float64 | Usage-adjusted expected assists, as pts_exp. |

**Example**

```python
import polars as pl
from sportsdataverse.wbb.wbb_player_box_expected import wbb_player_game_expected_box
games = wbb_player_game_expected_box(2026)
games.with_columns((pl.col("pts") - pl.col("pts_exp")).alias("pts_vs_exp")).sort("pts_vs_exp").tail()
```

### wbb_player_on_off {#wbb_player_on_off}

`wbb_player_on_off(seasons: 'int | list[int]', *, min_poss: 'int' = 200, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college player on/off: each player's team with her on the floor and without her.

Delegates to `sportsdataverse.mbb.mbb_player_on_off.mbb_player_on_off` with
`league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `min_poss` | `int` | `200` | The rank gate, offensive plus defensive possessions both on and off the floor (default 200). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id)`: `games`, `team_games`, the `_on` and `_off` totals and ratings, `<rating>_diff`, the ranks and the sample sizes. `poss_on + poss_off` is the team's possessions. See the mbb core for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `player_id` | String | stats.ncaa.org player id (there is no NCAA-to-ESPN player crosswalk yet). |
| `games` | Int64 | Games the player was on the floor for the team. |
| `team_games` | Int64 | Games the team played in the season (with play-by-play and possessions); games well short of it marks a player who missed time or was with the team for part of the season. |
| `minutes_on` | Float64 | Game-clock minutes of the team's season with the player on the floor. |
| `poss_on` | Int64 | The team's offensive possessions with the player on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_on` | Int64 | The team's defensive possessions with the player on the floor. |
| `pts_on` | Int64 | The team's points with the player on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_on` | Int64 | The opponent's points with the player on the floor, credited as pts_on is. |
| `plus_minus_on` | Int64 | pts_on - opp_pts_on. |
| `off_rtg_on` | Float64 | Offensive rating with the player on the floor: pts_on per 100 poss_on; null with no possessions. |
| `def_rtg_on` | Float64 | Defensive rating with the player on the floor: opp_pts_on per 100 opp_poss_on; null with no possessions. |
| `net_rtg_on` | Float64 | Net rating with the player on the floor: off_rtg_on - def_rtg_on; null when either is null. |
| `minutes_off` | Float64 | Game-clock minutes of the team's season with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left). |
| `poss_off` | Int64 | The team's offensive possessions with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left) (a possession belongs to the five on the floor when it starts). |
| `opp_poss_off` | Int64 | The team's defensive possessions with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left). |
| `pts_off` | Int64 | The team's points with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left), each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_off` | Int64 | The opponent's points with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left), credited as pts_off is. |
| `plus_minus_off` | Int64 | pts_off - opp_pts_off. |
| `off_rtg_off` | Float64 | Offensive rating with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left): pts_off per 100 poss_off; null with no possessions. |
| `def_rtg_off` | Float64 | Defensive rating with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left): opp_pts_off per 100 opp_poss_off; null with no possessions. |
| `net_rtg_off` | Float64 | Net rating with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left): off_rtg_off - def_rtg_off; null when either is null. |
| `off_rtg_diff` | Float64 | On-off offensive rating: off_rtg_on - off_rtg_off; null when either is null. |
| `def_rtg_diff` | Float64 | On-off defensive rating: def_rtg_on - def_rtg_off; null when either is null. |
| `net_rtg_diff` | Float64 | On-off net rating: net_rtg_on - net_rtg_off; null when either is null. |
| `off_rtg_on_rank` | Float64 | Rank of off_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose off_rtg_on_n is at least 240 offensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `def_rtg_on_rank` | Float64 | Rank of def_rtg_on within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose def_rtg_on_n is at least 590 defensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_on_rank` | Float64 | Rank of net_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose net_rtg_on_n is at least 600 possessions on the floor (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `off_rtg_diff_rank` | Float64 | Rank of off_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), and never for off_rtg_diff: its reliability never comes near half signal on a season's sample (rate_gates); null under that gate or below 10 qualifiers. |
| `def_rtg_diff_rank` | Float64 | Rank of def_rtg_diff within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), and never for def_rtg_diff: its reliability never comes near half signal on a season's sample (rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_diff_rank` | Float64 | Rank of net_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), and never for net_rtg_diff: its reliability never comes near half signal on a season's sample (rate_gates); null under that gate or below 10 qualifiers. |
| `off_rtg_on_pct` | Float64 | Percentile of off_rtg_on among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `def_rtg_on_pct` | Float64 | Percentile of def_rtg_on among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `net_rtg_on_pct` | Float64 | Percentile of net_rtg_on among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `off_rtg_diff_pct` | Float64 | Percentile of off_rtg_diff among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `def_rtg_diff_pct` | Float64 | Percentile of def_rtg_diff among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `net_rtg_diff_pct` | Float64 | Percentile of net_rtg_diff among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `off_rtg_on_n` | Int64 | Sample size of off_rtg_on: poss_on. |
| `def_rtg_on_n` | Int64 | Sample size of def_rtg_on: opp_poss_on. |
| `net_rtg_on_n` | Int64 | Sample size of net_rtg_on: poss_on + opp_poss_on. |
| `off_rtg_off_n` | Int64 | Sample size of off_rtg_off: poss_off. |
| `def_rtg_off_n` | Int64 | Sample size of def_rtg_off: opp_poss_off. |
| `net_rtg_off_n` | Int64 | Sample size of net_rtg_off: poss_off + opp_poss_off. |
| `off_rtg_diff_n` | Int64 | Sample size of off_rtg_diff: the smaller of off_rtg_on_n and off_rtg_off_n. |
| `def_rtg_diff_n` | Int64 | Sample size of def_rtg_diff: the smaller of def_rtg_on_n and def_rtg_off_n. |
| `net_rtg_diff_n` | Int64 | Sample size of net_rtg_diff: the smaller of net_rtg_on_n and net_rtg_off_n. |

**Example**

```python
from sportsdataverse.wbb import wbb_player_on_off
wbb_player_on_off(2025).sort("net_rtg_diff_rank").head()
```

### wbb_player_season_box_rates {#wbb_player_season_box_rates}

`wbb_player_season_box_rates(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college per-36 points, rebounds and assists per player-season, ranked, with the season usage.

Over every game of the season, from ESPN's player box. `pts_per36` / `reb_per36` /
`ast_per36` are 36 x the total / minutes, each with `_n` (the minutes) and
`_rank` / `_pct` among the season's players whose minutes reach the rate's gate (the
minutes at which it is half signal on odd / even games). `usg_pct` is the season usage
(FGA + 0.44 FTA + TOV over the team's while on the floor), not ranked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (2026 is 2025-26). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, athlete_id)` (the ESPN id): `team_id` (his last game's team), `games`, `minutes`, `pts` / `reb` / `ast`, `usg_pct` with `usg_pct_n`, and the three per-36 rates with `_n` / `_rank` / `_pct`. A season without a release returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `athlete_id` | Int64 | ESPN athlete id of the player, the join key to the player box score. |
| `team_id` | Int64 | ESPN team id of the player's last game of the season (a traded player has one row). |
| `games` | Int64 | Games played (the pros' regular season, every college game). |
| `minutes` | Float64 | Minutes played in those games. |
| `pts` | Int64 | Points in those games. |
| `reb` | Int64 | Rebounds in those games. |
| `ast` | Int64 | Assists in those games. |
| `usg_pct` | Float64 | Season usage: 100 x sum(uses) / sum(minutes x team uses / (team minutes / 5)), uses = FGA + 0.44 FTA + TOV; not ranked. |
| `usg_pct_n` | Int64 | Minutes of the games with a usage behind usg_pct. |
| `pts_per36` | Float64 | Points per 36 minutes: 36 x pts / minutes. |
| `pts_per36_n` | Int64 | Minutes behind pts_per36. |
| `reb_per36` | Float64 | Rebounds per 36 minutes: 36 x reb / minutes. |
| `reb_per36_n` | Int64 | Minutes behind reb_per36. |
| `ast_per36` | Float64 | Assists per 36 minutes: 36 x ast / minutes. |
| `ast_per36_n` | Int64 | Minutes behind ast_per36. |
| `pts_per36_rank` | Float64 | Rank of pts_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `pts_per36_pct` | Float64 | Percentile of pts_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |
| `reb_per36_rank` | Float64 | Rank of reb_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `reb_per36_pct` | Float64 | Percentile of reb_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |
| `ast_per36_rank` | Float64 | Rank of ast_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `ast_per36_pct` | Float64 | Percentile of ast_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |

**Example**

```python
from sportsdataverse.wbb.wbb_player_box_expected import wbb_player_season_box_rates
rates = wbb_player_season_box_rates(2026)
rates.sort("pts_per36_rank").select("athlete_id", "minutes", "pts_per36", "pts_per36_pct").head()
```

### wbb_player_shot_zones {#wbb_player_shot_zones}

`wbb_player_shot_zones(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3.

Delegates to `sportsdataverse.mbb.mbb_shot_zone_profiles.mbb_player_shot_zones` with
`league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id)`: `games`, `fga`, `fgm`, `unlocated_fga`, each zone's attempts, makes, assisted makes, `_fg_pct` / `_freq` / `_ast_pct` and the `_n` / `_rank` / `_pct` columns. See the mbb core for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | The team's ESPN team id (Int64). |
| `player_id` | String | stats.ncaa.org player id (text). |
| `player_name` | String | Player name as the shots release spells it. |
| `games` | Int64 | The player's games with at least one field-goal attempt for this team. |
| `fga` | Int64 | Field-goal attempts by the player (games). |
| `fgm` | Int64 | Field goals made by the player. |
| `unlocated_fga` | Int64 | Attempts by the player with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the player at the rim (within 4 ft, the shot chart's rim zone). |
| `rim_fgm` | Int64 | Makes by the player at the rim (within 4 ft, the shot chart's rim zone). |
| `rim_ast` | Int64 | Assisted makes by the player at the rim (within 4 ft, the shot chart's rim zone). |
| `mid_fga` | Int64 | Attempts by the player in the mid zone (every other two: the chart's paint and mid-range). |
| `mid_fgm` | Int64 | Makes by the player in the mid zone (every other two: the chart's paint and mid-range). |
| `mid_ast` | Int64 | Assisted makes by the player in the mid zone (every other two: the chart's paint and mid-range). |
| `corner_3_fga` | Int64 | Attempts by the player from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `corner_3_fgm` | Int64 | Makes by the player from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `corner_3_ast` | Int64 | Assisted makes by the player from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `above_break_3_fga` | Int64 | Attempts by the player from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the player from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the player from above the break (every other three). |
| `rim_fg_pct` | Float64 | rim_fgm / rim_fga; null with no attempt. |
| `rim_freq` | Float64 | rim_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `rim_ast_pct` | Float64 | Share of rim_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `mid_fg_pct` | Float64 | mid_fgm / mid_fga; null with no attempt. |
| `mid_freq` | Float64 | mid_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `mid_ast_pct` | Float64 | Share of mid_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `corner_3_fg_pct` | Float64 | corner_3_fgm / corner_3_fga; null with no attempt. |
| `corner_3_freq` | Float64 | corner_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `corner_3_ast_pct` | Float64 | Share of corner_3_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `above_break_3_fg_pct` | Float64 | above_break_3_fgm / above_break_3_fga; null with no attempt. |
| `above_break_3_freq` | Float64 | above_break_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `above_break_3_ast_pct` | Float64 | Share of above_break_3_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `rim_fg_pct_n` | Int64 | Sample size of rim_fg_pct: rim_fga. |
| `rim_freq_n` | Int64 | Sample size of rim_freq: located attempts (fga - unlocated_fga). |
| `rim_ast_pct_n` | Int64 | Sample size of rim_ast_pct: makes linked to a play-by-play row in the zone. |
| `mid_fg_pct_n` | Int64 | Sample size of mid_fg_pct: mid_fga. |
| `mid_freq_n` | Int64 | Sample size of mid_freq: located attempts (fga - unlocated_fga). |
| `mid_ast_pct_n` | Int64 | Sample size of mid_ast_pct: makes linked to a play-by-play row in the zone. |
| `corner_3_fg_pct_n` | Int64 | Sample size of corner_3_fg_pct: corner_3_fga. |
| `corner_3_freq_n` | Int64 | Sample size of corner_3_freq: located attempts (fga - unlocated_fga). |
| `corner_3_ast_pct_n` | Int64 | Sample size of corner_3_ast_pct: makes linked to a play-by-play row in the zone. |
| `above_break_3_fg_pct_n` | Int64 | Sample size of above_break_3_fg_pct: above_break_3_fga. |
| `above_break_3_freq_n` | Int64 | Sample size of above_break_3_freq: located attempts (fga - unlocated_fga). |
| `above_break_3_ast_pct_n` | Int64 | Sample size of above_break_3_ast_pct: makes linked to a play-by-play row in the zone. |
| `rim_fg_pct_rank` | Float64 | Rank of rim_fg_pct within the season (the highest ranks 1; ties average), among players whose rim_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_freq_rank` | Float64 | Rank of rim_freq within the season (the highest ranks 1; ties average), among players whose rim_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_ast_pct_rank` | Float64 | Rank of rim_ast_pct within the season (the highest ranks 1; ties average), among players whose rim_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_fg_pct_rank` | Float64 | Rank of mid_fg_pct within the season (the highest ranks 1; ties average), among players whose mid_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_freq_rank` | Float64 | Rank of mid_freq within the season (the highest ranks 1; ties average), among players whose mid_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_ast_pct_rank` | Float64 | Rank of mid_ast_pct within the season (the highest ranks 1; ties average), among players whose mid_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_fg_pct_rank` | Float64 | Rank of corner_3_fg_pct within the season (the highest ranks 1; ties average), among players whose corner_3_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_freq_rank` | Float64 | Rank of corner_3_freq within the season (the highest ranks 1; ties average), among players whose corner_3_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_ast_pct_rank` | Float64 | Rank of corner_3_ast_pct within the season (the highest ranks 1; ties average), among players whose corner_3_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_fg_pct_rank` | Float64 | Rank of above_break_3_fg_pct within the season (the highest ranks 1; ties average), among players whose above_break_3_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_freq_rank` | Float64 | Rank of above_break_3_freq within the season (the highest ranks 1; ties average), among players whose above_break_3_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_ast_pct_rank` | Float64 | Rank of above_break_3_ast_pct within the season (the highest ranks 1; ties average), among players whose above_break_3_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_fg_pct_pct` | Float64 | Percentile of rim_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_freq_pct` | Float64 | Percentile of rim_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_ast_pct_pct` | Float64 | Percentile of rim_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_fg_pct_pct` | Float64 | Percentile of mid_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_freq_pct` | Float64 | Percentile of mid_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_ast_pct_pct` | Float64 | Percentile of mid_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_fg_pct_pct` | Float64 | Percentile of corner_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_freq_pct` | Float64 | Percentile of corner_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_ast_pct_pct` | Float64 | Percentile of corner_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_fg_pct_pct` | Float64 | Percentile of above_break_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_freq_pct` | Float64 | Percentile of above_break_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_ast_pct_pct` | Float64 | Percentile of above_break_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |

**Example**

```python
from sportsdataverse.wbb import wbb_player_shot_zones
wbb_player_shot_zones(2025).sort("rim_fga", descending=True).head()
```

### wbb_player_war {#wbb_player_war}

`wbb_player_war(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college WAR and VORP per player-season, from our box BPM.

Delegates to `sportsdataverse.mbb.mbb_player_war.mbb_player_war` with
`league="womens"`. Box BPM covers every D1 team from 2017 (about 80 teams in 2014-2016).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, player_id)` of a D1 team: `player_name`, `team_id`, `teams`, `min`, `poss`, `rating`, `vorp`, `war` and the `_rank` / `_pct` / `_n` columns. See the mbb core for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `player_id` | String | The player's ESPN athlete id, as text (the box-score id the college player-value models key on). |
| `player_name` | String | Player name, from the box score. |
| `team_id` | String | ESPN team id of the Division I team he played the most possessions for. |
| `teams` | Int64 | How many Division I teams he played for. |
| `min` | Float64 | Minutes in the season's box scores. |
| `poss` | Float64 | Possessions: minutes / 40 * the team's adjusted tempo (mbb_team_ratings adj_tempo, possessions per 40 minutes). College has no published on-court possessions for every game. The minutes basis of vorp and war. |
| `rating` | Float64 | Points per 100 possessions above an average Division I player: box BPM (mbb_box_bpm, model 1 of the player-value spine), re-anchored to the team (team mean = adjusted efficiency margin minus the D1 mean, / 5) and centred so the D1 possession-weighted mean is 0. |
| `vorp` | Float64 | Value over replacement player: (rating - R) * poss / the season's possessions per D1 team, R the league's replacement level (REPLACEMENT_LEVEL; MBB -1.66, WBB -1.99 per 100). Basketball-Reference's VORP scale. |
| `war` | Float64 | Wins above replacement against an average D1 schedule: (rating - R) * poss / 100 / points per win (PTS_PER_WIN; MBB 38.91, WBB 47.10). 0 at replacement level; a replacement team wins about .35. |
| `war_lo` | Float64 | Low end of the nominal 90% interval for war (5th percentile of 1000 bootstrap replicates, each team-season seeded on its own): each team-season's games resampled with replacement (a game's player possessions, team possessions and margin move together), then his possessions, the team net rating, the anchoring and the centring recomputed. The source ratings and the replacement level are held fixed, so this is the sampling uncertainty of the team side and the playing time (in college the replicate's adjusted net rating is the adjusted rating plus the change in the raw one); box BPM's own sampling error is in no column. |
| `war_hi` | Float64 | High end of the nominal 90% interval for war (95th percentile of the same 1000 game-resampled replicates as war_lo). |
| `vorp_rank` | Float64 | Rank of vorp within the season among every player (1 = most; ties average); null under 10 players. |
| `war_rank` | Float64 | Rank of war within the season among every player (1 = most; ties average); null under 10 players. |
| `vorp_pct` | Float64 | Percentile of vorp within the season: 100 * (n + 1 - rank) / (n + 1), n the players ranked; null when the rank is null. |
| `war_pct` | Float64 | Percentile of war within the season: 100 * (n + 1 - rank) / (n + 1), n the players ranked; null when the rank is null. |
| `vorp_n` | Int64 | Sample size of vorp: on-court possessions, rounded. |
| `war_n` | Int64 | Sample size of war: on-court possessions, rounded. |
| `pts_pg` | Float64 | Points per game he played (minutes over 0), from the ESPN box rows of his Division I games. |
| `reb_pg` | Float64 | Rebounds per game he played (minutes over 0), from the ESPN box rows of his Division I games. |
| `ast_pg` | Float64 | Assists per game he played (minutes over 0), from the ESPN box rows of his Division I games. |
| `pts_pg_rank` | Float64 | Rank of pts_pg among the season's players with at least the games gate (1 game); 1 = most, ties averaged. Null under ten ranked players. |
| `reb_pg_rank` | Float64 | Rank of reb_pg among the season's players with at least the games gate (2 games); 1 = most, ties averaged. Null under ten ranked players. |
| `ast_pg_rank` | Float64 | Rank of ast_pg among the season's players with at least the games gate (2 games); 1 = most, ties averaged. Null under ten ranked players. |
| `pts_pg_pct` | Float64 | Percentile of pts_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `reb_pg_pct` | Float64 | Percentile of reb_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `ast_pg_pct` | Float64 | Percentile of ast_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `pts_pg_n` | Int64 | Games behind pts_pg: the games he played (0 with no box line). |
| `reb_pg_n` | Int64 | Games behind reb_pg: the games he played (0 with no box line). |
| `ast_pg_n` | Int64 | Games behind ast_pg: the games he played (0 with no box line). |

**Example**

```python
from sportsdataverse.wbb import wbb_player_war
wbb_player_war(2025).select("player_name", "rating", "war").head()
```
