---
title: "Men's college basketball — additional Python functions — Analytics: mbb_lineup–mbb_strength"
sidebar_label: "Analytics: mbb_lineup–mbb_strength"
sidebar_position: 15
description: "Men's college basketball — additional Python functions — Analytics: mbb_lineup–mbb_strength — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Analytics: mbb_lineup–mbb_strength

### mbb_lineup_seasons {#mbb_lineup_seasons}

`mbb_lineup_seasons(seasons: 'int | list[int]', *, league: 'str' = 'mens', min_poss: 'int' = 200, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College lineup seasons: 2-, 3- and 5-man units with ratings, four factors and shot zones.

The definitions are in `sportsdataverse._common.lineup_seasons`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_lineup_seasons.wbb_lineup_seasons`). |
| `min_poss` | `int` | `200` | The rank gate, offensive plus defensive possessions (default 200). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, n_players, lineup_id)`: `n_players` (2, 3 or 5), `lineup_id` (the NCAA player ids sorted and joined with `-`), `player_1..5`, `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions), the four factors for and against (`efg_pct`, `tov_pct`, `orb_pct`, `ft_rate`, `opp_*`), the shot zones for and against (`rim_rate`, `mid_rate`, `fg3_rate` shares of field-goal attempts and `rim_pct`, `mid_pct`, `fg3_pct` make rates, `opp_*`), and for every rate `<m>_rank` / `<m>_pct` (within the season and unit size, among units with at least `min_poss`) and `<m>_n`. `team_id` is the ESPN team id (Int64). Empty input returns that schema with zero rows.

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
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose off_rtg_n is at least 270 offensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose def_rtg_n is at least 460 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose net_rtg_n is at least 430 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose efg_pct_n is at least 500 field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose tov_pct_n is at least 450 offensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose orb_pct_n is at least 160 offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose ft_rate_n is at least 530 field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose opp_efg_pct_n is at least 630 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose opp_tov_pct_n is at least 340 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose opp_orb_pct_n is at least 330 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), whose opp_ft_rate_n is at least 450 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `rim_rate_rank` | Float64 | Rank of rim_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `mid_rate_rank` | Float64 | Rank of mid_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `fg3_rate_rank` | Float64 | Rank of fg3_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `rim_pct_rank` | Float64 | Rank of rim_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `mid_pct_rank` | Float64 | Rank of mid_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `fg3_pct_rank` | Float64 | Rank of fg3_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200), and never for fg3_pct: its reliability never comes near half signal on a season's sample (rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_seasons import mbb_lineup_seasons
units = mbb_lineup_seasons(2025)
units.filter(n_players=2).sort("net_rtg_rank").head()
```

### mbb_play_styles {#mbb_play_styles}

`mbb_play_styles(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College player play-style archetypes: box, shot and lineup-role features (BB-D20).

The features and their sources are in `sportsdataverse.mbb.mbb_play_style`; the method
(season-relative z-scores, centers fit on 2025 and frozen, applied as-of) in
`sportsdataverse._common.play_style`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons (2025+). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per qualifying `(player_id, season, team_id)`: `player_id` / `team_id` (Utf8, ESPN ids), `season`, `player`, `min` and the `archetype`, `cluster`, `dist_to_center`, `second_archetype` and `margin`. Empty input returns that schema.

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
from sportsdataverse.mbb.mbb_play_style import mbb_play_styles
styles = mbb_play_styles(2026)
styles.group_by("archetype").len()
```

### mbb_player_on_off {#mbb_player_on_off}

`mbb_player_on_off(seasons: 'int | list[int]', *, league: 'str' = 'mens', min_poss: 'int' = 200, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College player on/off: each player's team with him on the floor and without him, per team-season.

The definitions are in `sportsdataverse._common.player_on_off`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_player_on_off.wbb_player_on_off`). |
| `min_poss` | `int` | `200` | The rank gate, offensive plus defensive possessions both on and off the floor (default 200). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id)`: `games` on the floor and `team_games`, then for `_on` and `_off` the `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions); `<rating>_diff` (on minus off); `<m>_rank` / `<m>_pct` for the on and diff ratings (within the season, among players with at least `min_poss` both ways) and `<m>_n` for every rating. `poss_on + poss_off` is the team's possessions. `team_id` is the ESPN team id (Int64), `player_id` the NCAA id (String). Empty input returns that schema with zero rows.

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
| `off_rtg_on_rank` | Float64 | Rank of off_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose off_rtg_on_n is at least 530 offensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `def_rtg_on_rank` | Float64 | Rank of def_rtg_on within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose def_rtg_on_n is at least 750 defensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_on_rank` | Float64 | Rank of net_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose net_rtg_on_n is at least 1,100 possessions on the floor (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_player_on_off import mbb_player_on_off
on_off = mbb_player_on_off(2025)
on_off.sort("net_rtg_diff_rank").head()
```

### mbb_player_war {#mbb_player_war}

`mbb_player_war(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WAR and VORP per player-season, from our box BPM.

Box BPM is anchored to the team and centred on Division I, so 0 is an average D1 player.
`vorp` is points per 100 team possessions above a replacement player over one team's
season of possessions (Basketball-Reference's VORP scale); `war` is wins above replacement
against an average D1 schedule. A replacement-level player has 0, and a season's `war`
adds up to the D1 teams' wins minus those of a team of replacement players. The replacement
level and points per win are fitted per league
(`~sportsdataverse._common.player_war.REPLACEMENT_LEVEL`,
`~sportsdataverse._common.player_war.PTS_PER_WIN`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_player_war.wbb_player_war`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, player_id)` (ESPN ids, String) of a D1 team -- one with 10 or more games in `~sportsdataverse.mbb.mbb_team_ratings.mbb_team_ratings`, box BPM's own centring set, so a season is empty until its teams reach 10 games: `player_name`, `team_id` (the team he played most possessions for), `teams`, `min`, `poss` (minutes / 40 * the team's adjusted tempo), `rating` (anchored box BPM, per 100), `vorp` and `war` with `war_lo` / `war_hi` (a nominal 90% interval from a bootstrap over each team-season's games: the sampling uncertainty of the team side and the playing time, not of box BPM itself), with `_rank` (1 = most), `_pct` and `_n` (possessions) for `vorp` and `war` among every player of the season. Sorted by season and `war`. Empty input returns that schema with zero rows.

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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"` (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.mbb.mbb_player_war import mbb_player_war
war = mbb_player_war(2025)
war.select("player_name", "poss", "rating", "vorp", "war").head(10)
```

### mbb_player_wowy {#mbb_player_wowy}

`mbb_player_wowy(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College WOWY pairs: a team's season with two teammates together, apart and both off, per pair.

The definitions are in `sportsdataverse._common.player_on_off`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_player_on_off.wbb_player_wowy`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id_a, player_id_b)` (`player_id_a` sorting first as a string) for every pair of the team's players: for `_both` (together), `_a_only`, `_b_only` and `_neither` the `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and `off_rtg` / `def_rtg` / `net_rtg`. The four splits' totals add up to the team's season; the ratings do not add. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `player_id_a` | String | The first player of the pair, the smaller id as a string. |
| `player_id_b` | String | The second player of the pair, the larger id as a string. |
| `minutes_both` | Float64 | Game-clock minutes of the team's season with both A and B on the floor. |
| `poss_both` | Int64 | The team's offensive possessions with both A and B on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_both` | Int64 | The team's defensive possessions with both A and B on the floor. |
| `pts_both` | Int64 | The team's points with both A and B on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_both` | Int64 | The opponent's points with both A and B on the floor, credited as pts_both is. |
| `plus_minus_both` | Int64 | pts_both - opp_pts_both. |
| `off_rtg_both` | Float64 | Offensive rating with both A and B on the floor: pts_both per 100 poss_both; null with no possessions. |
| `def_rtg_both` | Float64 | Defensive rating with both A and B on the floor: opp_pts_both per 100 opp_poss_both; null with no possessions. |
| `net_rtg_both` | Float64 | Net rating with both A and B on the floor: off_rtg_both - def_rtg_both; null when either is null. |
| `minutes_a_only` | Float64 | Game-clock minutes of the team's season with A on the floor and B off. |
| `poss_a_only` | Int64 | The team's offensive possessions with A on the floor and B off (a possession belongs to the five on the floor when it starts). |
| `opp_poss_a_only` | Int64 | The team's defensive possessions with A on the floor and B off. |
| `pts_a_only` | Int64 | The team's points with A on the floor and B off, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_a_only` | Int64 | The opponent's points with A on the floor and B off, credited as pts_a_only is. |
| `plus_minus_a_only` | Int64 | pts_a_only - opp_pts_a_only. |
| `off_rtg_a_only` | Float64 | Offensive rating with A on the floor and B off: pts_a_only per 100 poss_a_only; null with no possessions. |
| `def_rtg_a_only` | Float64 | Defensive rating with A on the floor and B off: opp_pts_a_only per 100 opp_poss_a_only; null with no possessions. |
| `net_rtg_a_only` | Float64 | Net rating with A on the floor and B off: off_rtg_a_only - def_rtg_a_only; null when either is null. |
| `minutes_b_only` | Float64 | Game-clock minutes of the team's season with B on the floor and A off. |
| `poss_b_only` | Int64 | The team's offensive possessions with B on the floor and A off (a possession belongs to the five on the floor when it starts). |
| `opp_poss_b_only` | Int64 | The team's defensive possessions with B on the floor and A off. |
| `pts_b_only` | Int64 | The team's points with B on the floor and A off, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_b_only` | Int64 | The opponent's points with B on the floor and A off, credited as pts_b_only is. |
| `plus_minus_b_only` | Int64 | pts_b_only - opp_pts_b_only. |
| `off_rtg_b_only` | Float64 | Offensive rating with B on the floor and A off: pts_b_only per 100 poss_b_only; null with no possessions. |
| `def_rtg_b_only` | Float64 | Defensive rating with B on the floor and A off: opp_pts_b_only per 100 opp_poss_b_only; null with no possessions. |
| `net_rtg_b_only` | Float64 | Net rating with B on the floor and A off: off_rtg_b_only - def_rtg_b_only; null when either is null. |
| `minutes_neither` | Float64 | Game-clock minutes of the team's season with neither A nor B on the floor. |
| `poss_neither` | Int64 | The team's offensive possessions with neither A nor B on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_neither` | Int64 | The team's defensive possessions with neither A nor B on the floor. |
| `pts_neither` | Int64 | The team's points with neither A nor B on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_neither` | Int64 | The opponent's points with neither A nor B on the floor, credited as pts_neither is. |
| `plus_minus_neither` | Int64 | pts_neither - opp_pts_neither. |
| `off_rtg_neither` | Float64 | Offensive rating with neither A nor B on the floor: pts_neither per 100 poss_neither; null with no possessions. |
| `def_rtg_neither` | Float64 | Defensive rating with neither A nor B on the floor: opp_pts_neither per 100 opp_poss_neither; null with no possessions. |
| `net_rtg_neither` | Float64 | Net rating with neither A nor B on the floor: off_rtg_neither - def_rtg_neither; null when either is null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_player_on_off import mbb_player_wowy
pairs = mbb_player_wowy(2025)
pairs.sort("minutes_both", descending=True).head()
```

### mbb_shot_selection {#mbb_shot_selection}

`mbb_shot_selection(scored: 'pl.DataFrame', *, group: 'str' = 'shooter_id', league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per shooter/team expected points per attempt vs the league-average mix.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output (needs `xpoints, point_value, made` + the group column). |
| `group` | `str` | `'shooter_id'` | `"shooter_id"` or `"team_id"`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (interface parity; the math is league-free). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per group: `{group}:Utf8, n_shots:Int64, xppp, actual_ppp, selection_value, selection_value_total` (all value columns Float64). The attempt-weighted `selection_value` sums to zero across the league. Empty input returns the zero-row schema.

| col_name | type | description |
|---|---|---|
| `shooter_id` | String | Unique identifier for shooter. |
| `n_shots` | Int64 |  |
| `xppp` | Float64 |  |
| `actual_ppp` | Float64 |  |
| `selection_value` | Float64 |  |
| `selection_value_total` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | Unknown `group`. |

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality, mbb_shot_selection
sel = mbb_shot_selection(mbb_shot_quality(mbb_shot_data(2025)), group="team_id")

# Pipeline next step (one line)

sel.sort("selection_value", descending=True).head(10)
```

### mbb_situational_lineup {#mbb_situational_lineup}

`mbb_situational_lineup(seasons: 'int | list[int]', *, league: 'str' = 'mens', top_n: 'int' = 25, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College five-man unit splits by situation: the team splits over the possessions each unit started.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `top_n` | `int` | `25` | As `mbb_situational_team`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, lineup_id, family, split)` with the columns of `sportsdataverse.nba.nba_situational.nba_situational_lineup` (String player ids). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | ESPN team id of the team (Int64). |
| `lineup_id` | String | The five player ids of the unit on the floor as the possession starts, sorted and joined with '-' (BB-D4's id). |
| `player_1` | String | Player 1 of the unit, in lineup_id order. |
| `player_2` | String | Player 2 of the unit, in lineup_id order. |
| `player_3` | String | Player 3 of the unit, in lineup_id order. |
| `player_4` | String | Player 4 of the unit, in lineup_id order. |
| `player_5` | String | Player 5 of the unit, in lineup_id order. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent or play_type (college has no second-chance flag, so no chance family); within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot (p1, p2 for halves); h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`, or `top_n` is below 1. |

**Example**

```python
from sportsdataverse.mbb.mbb_situational import mbb_situational_lineup
mbb_situational_lineup(2026).filter(split="not_garbage_time").sort("poss", descending=True).head()
```

### mbb_situational_player {#mbb_situational_player}

`mbb_situational_player(seasons: 'int | list[int]', *, league: 'str' = 'mens', top_n: 'int' = 25, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College player on-floor splits by situation: the team splits over the possessions each player started.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `top_n` | `int` | `25` | As `mbb_situational_team`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id, family, split)` with the columns of `sportsdataverse.nba.nba_situational.nba_situational_player`; `player_id` is the stats.ncaa.org player id (String). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | ESPN team id of the team (Int64). |
| `player_id` | String | stats.ncaa.org player id (no NCAA-to-ESPN player crosswalk yet). A possession counts for the five on the floor as it starts, with all its points. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent or play_type (college has no second-chance flag, so no chance family); within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot (p1, p2 for halves); h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`, or `top_n` is below 1. |

**Example**

```python
from sportsdataverse.mbb.mbb_situational import mbb_situational_player
mbb_situational_player(2026).filter(split="clutch").sort("net_rtg_n", descending=True).head()
```

### mbb_situational_team {#mbb_situational_team}

`mbb_situational_team(seasons: 'int | list[int]', *, league: 'str' = 'mens', top_n: 'int' = 25, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College team splits by situation: clutch, period, half, garbage time, score, venue, opponent, play type.

The families and their splits are in `sportsdataverse._common.situational_splits`; within
a family the splits add up to the `all` row.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_situational.wbb_situational_team`). |
| `top_n` | `int` | `25` | How many of the season's best teams by the published `adj_em` are `vs_top` (default 25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, family, split)` with the columns of `sportsdataverse.nba.nba_situational.nba_situational_team`; `team_id` is the ESPN team id (Int64). A non-D1 opponent gets no row. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | ESPN team id of the team (Int64). |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent or play_type (college has no second-chance flag, so no chance family); within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot (p1, p2 for halves); h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`, or `top_n` is below 1. |

**Example**

```python
from sportsdataverse.mbb.mbb_situational import mbb_situational_team
mbb_situational_team(2026).filter(split="vs_top").sort("net_rtg", descending=True).head()
```

### mbb_strength_of_schedule {#mbb_strength_of_schedule}

`mbb_strength_of_schedule(seasons: 'list[int]', *, as_of_date: 'datetime.date | None' = None, league: 'str' = 'mens', ineligible: 'Iterable[str] | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The daily `strength_of_schedule` table: records, SoS, quad résumé, WAB, RPI and a NET estimate.

Loads the released ESPN schedule and team box per season and builds
`~sportsdataverse.mbb.mbb_college_season.season_resume` from the games strictly
before `as_of_date`: opponent-adjusted ratings, SoS (mean opponent AdjEM), WAB, the
NCAA Quad 1-4 record (opponents ranked by the NET estimate), the NCAA RPI
(25% WP / 50% OWP / 25% OOWP, location-weighted WP) and the NET-style estimate
(`0.8 * z(AdjEM) + 0.2 * z(WAB)`). Division I teams only; RPI, SoS, WAB and the quads
count Division I games only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to compute (e.g. `[2024]`). |
| `as_of_date` | `date \| None` | `None` | Only use games strictly before this date; `None` uses every completed game. Applies to every season in `seasons`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `ineligible` | `Iterable[str] \| None` | `None` | Team ids barred from the NCAA tournament (sets `eligible`); `None` (default) reads the bundled list for each season. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per Division I (season, team_id), best `net_estimate_rank` first. The `_rank` / `_pct` columns (1 is best, average ties, within the season) and `_n` (the Division I games behind each number; all games for `adj_em`) follow BB-D2's league-context grammar.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by its end year (2024 is 2023-24). |
| `as_of_date` | Date | The as-of date: only games strictly before it count as played. Null when every completed game is used. |
| `team_id` | String | ESPN team id (Utf8). |
| `team` | String | Team (ESPN location name). |
| `conference_id` | String | ESPN conference id, read off the schedule (Utf8). |
| `conference` | String | Conference short name. Null for independents, which get no auto-bid. |
| `eligible` | Boolean | False for a team the caller listed as barred from the NCAA tournament (reclassifying to Division I, a postseason ban). It counts in every rating and résumé but gets no bid. |
| `wins` | Int64 | Wins in played games, against every opponent (non-Division I included). |
| `losses` | Int64 | Losses in played games, against every opponent. |
| `conf_wins` | Int64 | Regular-season conference wins (conference tournaments and multi-team events excluded). |
| `conf_losses` | Int64 | Regular-season conference losses. |
| `games` | Int64 | Games behind the ratings (every played game with a usable box score). |
| `adj_o` | Float64 | Opponent-adjusted offensive efficiency: points per 100 possessions (mbb_team_ratings). |
| `adj_d` | Float64 | Opponent-adjusted defensive efficiency: points allowed per 100 possessions. |
| `adj_em` | Float64 | Adjusted efficiency margin, adj_o - adj_d. |
| `adj_em_z` | Float64 | adj_em z-scored over Division I within the season. |
| `sos` | Float64 | Strength of schedule: mean opponent adj_em over Division I games. |
| `wab` | Float64 | Wins above bubble: Division I wins minus a bubble-quality team's expected wins against the same schedule and venues. |
| `quad1_w` | Int64 | Quad 1 wins over Division I opponents, by the opponent's NET-estimate rank and the venue (NCAA ranges, men's and women's differ). |
| `quad1_l` | Int64 | Quad 1 losses to Division I opponents, by the same opponent rank and venue ranges. |
| `quad2_w` | Int64 | Quad 2 wins over Division I opponents, by the opponent's NET-estimate rank and the venue (NCAA ranges, men's and women's differ). |
| `quad2_l` | Int64 | Quad 2 losses to Division I opponents, by the same opponent rank and venue ranges. |
| `quad3_w` | Int64 | Quad 3 wins over Division I opponents, by the opponent's NET-estimate rank and the venue (NCAA ranges, men's and women's differ). |
| `quad3_l` | Int64 | Quad 3 losses to Division I opponents, by the same opponent rank and venue ranges. |
| `quad4_w` | Int64 | Quad 4 wins over Division I opponents, by the opponent's NET-estimate rank and the venue (NCAA ranges, men's and women's differ). |
| `quad4_l` | Int64 | Quad 4 losses to Division I opponents, by the same opponent rank and venue ranges. |
| `quality_wins` | Int64 | Quad 1 plus Quad 2 wins. |
| `rpi_games` | Int64 | Division I games behind the RPI. |
| `rpi_wp` | Float64 | RPI winning percentage over Division I games, location weighted: a home win 0.6, a road win 1.4, a home loss 1.4, a road loss 0.6, neutral 1.0 (men's from 2004-05, women's from 2011-12). |
| `rpi_owp` | Float64 | Opponents' winning percentage: each opponent's unweighted Division I record without its games against this team, averaged over this team's games. |
| `rpi_oowp` | Float64 | Opponents' opponents' winning percentage: the opponents' rpi_owp averaged over this team's games. |
| `rpi` | Float64 | NCAA Rating Percentage Index: 0.25 * rpi_wp + 0.50 * rpi_owp + 0.25 * rpi_oowp. |
| `net_estimate` | Float64 | NET-style score: 0.8 * z(adj_em) + 0.2 * z(wab) within the season (the NET's efficiency and results halves; weights fitted to the men's 2024 NET). Higher is better. |
| `adj_em_rank` | Float64 | Rank of adj_em within the season among Division I teams: 1 is best, average ties. |
| `adj_em_pct` | Float64 | Percentile of adj_em within the season: 100 * (n + 1 - rank) / (n + 1). |
| `adj_em_n` | Int64 | Sample size of adj_em: the games behind the ratings. |
| `sos_rank` | Float64 | Rank of sos (1 = hardest schedule) within the season among Division I teams: 1 is best, average ties. |
| `sos_pct` | Float64 | Percentile of sos within the season: 100 * (n + 1 - rank) / (n + 1). |
| `sos_n` | Int64 | Sample size of sos: the team's Division I games. |
| `wab_rank` | Float64 | Rank of wab within the season among Division I teams: 1 is best, average ties. |
| `wab_pct` | Float64 | Percentile of wab within the season: 100 * (n + 1 - rank) / (n + 1). |
| `wab_n` | Int64 | Sample size of wab: the team's Division I games. |
| `rpi_rank` | Float64 | Rank of rpi within the season among Division I teams: 1 is best, average ties. |
| `rpi_pct` | Float64 | Percentile of rpi within the season: 100 * (n + 1 - rank) / (n + 1). |
| `rpi_n` | Int64 | Sample size of rpi: the team's Division I games. |
| `net_estimate_rank` | Float64 | Rank of net_estimate (the NET-estimate ranking the quads use) within the season among Division I teams: 1 is best, average ties. |
| `net_estimate_pct` | Float64 | Percentile of net_estimate within the season: 100 * (n + 1 - rank) / (n + 1). |
| `net_estimate_n` | Int64 | Sample size of net_estimate: the team's Division I games. |

**Example**

```python
from sportsdataverse.mbb import mbb_strength_of_schedule
resume = mbb_strength_of_schedule([2024])

# Pipeline next step (one line)

resume.sort("rpi_rank").head(25)
```
