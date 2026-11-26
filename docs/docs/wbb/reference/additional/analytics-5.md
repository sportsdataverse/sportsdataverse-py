---
title: "Women's college basketball — additional Python functions — Analytics: wbb_team"
sidebar_label: "Analytics: wbb_team"
sidebar_position: 15
description: "Women's college basketball — additional Python functions — Analytics: wbb_team — function reference in sdv-py, the SportsDataverse Python package."
---
# Women's college basketball — additional Python functions — Analytics: wbb_team

### wbb_team_season_rates {#wbb_team_season_rates}

`wbb_team_season_rates(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college season team rates from possessions, with home/away/close/clutch splits.

Delegates to `sportsdataverse.mbb.mbb_team_season_rates.mbb_team_season_rates` with
`league="womens"`. Clutch is a possession in play in the last 5:00 of the 4th quarter (the
2nd half before 2016) or an overtime, the score within 5 as it starts. That is one starting
with 5:00 or less left, or the one live at the mark: a possession from 5:10 to 4:50 counts in
full, one that ends exactly at 5:00 does not.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, split)`: `games`, `poss`, `opp_poss`, `pts`, `opp_pts`, the twelve rates (`off_rtg`, `def_rtg`, `net_rtg`, `pace`, the four factors and the four against) and their `_rank` / `_pct` / `_n`. See the mbb core for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id. Non-D1 opponents have none and get no row. |
| `split` | String | Which possessions the row covers: all; home or away (the team's home or road games; neutral-site games are in neither); close (every possession of a game that had a clutch possession); clutch (a possession in play with 5:00 or less left (starting there, or starting before 5:00 and ending after it) in the 4th quarter (the 2nd half before 2016) or an overtime, the score within 5 as it starts). |
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
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose off_rtg_n is at least 130 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose def_rtg_n is at least 470 defensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose net_rtg_n is at least 360 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season and split (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the split, whose pace_n is at least 3 games (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose efg_pct_n is at least 230 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose tov_pct_n is at least 210 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose orb_pct_n is at least 160 offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose ft_rate_n is at least 550 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_efg_pct_n is at least 600 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_tov_pct_n is at least 300 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_orb_pct_n is at least 240 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_ft_rate_n is at least 420 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
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
| `lg_ft_pct` | Float64 | League free-throw percentage in the season and split: every team's FTM over every team's FTA, so free throws can be priced without a second read. From the NCAA team box; null in the clutch split. |
| `plus_minus` | Int64 | Point margin in the split: pts - opp_pts. |
| `pts_pg` | Float64 | Per game: points scored divided by games (BB-G8's per-game basis). |
| `opp_pts_pg` | Float64 | Per game: points allowed divided by games (BB-G8's per-game basis). |
| `plus_minus_pg` | Float64 | Per game: point margin (pts - opp_pts) divided by games (BB-G8's per-game basis). |
| `pts_per75` | Float64 | Per 75 possessions: 0.75 * off_rtg (BB-G8's per-75 basis). |
| `opp_pts_per75` | Float64 | Per 75 possessions: 0.75 * def_rtg (BB-G8's per-75 basis). |
| `plus_minus_per75` | Float64 | Per 75 possessions: 0.75 * net_rtg (BB-G8's per-75 basis). |
| `pts_pg_rank` | Float64 | Rank of pts_pg within the season and split (the highest value ranks 1; ties average), ranked where off_rtg is: among the teams off_rtg_rank qualifies (half the split's most games, and off_rtg_n at least off_rtg's rate_gates gate); null otherwise or with fewer than five qualifiers. Per game is not per possession, so pace reorders it against off_rtg. |
| `opp_pts_pg_rank` | Float64 | Rank of opp_pts_pg within the season and split (the lowest value ranks 1; ties average), ranked where def_rtg is: among the teams def_rtg_rank qualifies (half the split's most games, and def_rtg_n at least def_rtg's rate_gates gate); null otherwise or with fewer than five qualifiers. Per game is not per possession, so pace reorders it against def_rtg. |
| `plus_minus_pg_rank` | Float64 | Rank of plus_minus_pg within the season and split (the highest value ranks 1; ties average), ranked where net_rtg is: among the teams net_rtg_rank qualifies (half the split's most games, and net_rtg_n at least net_rtg's rate_gates gate); null otherwise or with fewer than five qualifiers. Per game is not per possession, so pace reorders it against net_rtg. |
| `pts_per75_rank` | Float64 | Rank of pts_per75: off_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `opp_pts_per75_rank` | Float64 | Rank of opp_pts_per75: def_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `plus_minus_per75_rank` | Float64 | Rank of plus_minus_per75: net_rtg_rank, copied (scaling by 0.75 keeps the order: same gate, same ties). |
| `pts_pg_pct` | Float64 | Percentile of pts_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_pts_pg_pct` | Float64 | Percentile of opp_pts_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `plus_minus_pg_pct` | Float64 | Percentile of plus_minus_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `pts_per75_pct` | Float64 | Percentile of pts_per75: off_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `opp_pts_per75_pct` | Float64 | Percentile of opp_pts_per75: def_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `plus_minus_per75_pct` | Float64 | Percentile of plus_minus_per75: net_rtg_pct, copied (scaling by 0.75 keeps the order). |
| `pts_pg_n` | Int64 | Sample size of pts_pg: off_rtg_n, the rating's own sample. |
| `opp_pts_pg_n` | Int64 | Sample size of opp_pts_pg: def_rtg_n, the rating's own sample. |
| `plus_minus_pg_n` | Int64 | Sample size of plus_minus_pg: net_rtg_n, the rating's own sample. |
| `pts_per75_n` | Int64 | Sample size of pts_per75: off_rtg_n, copied. |
| `opp_pts_per75_n` | Int64 | Sample size of opp_pts_per75: def_rtg_n, copied. |
| `plus_minus_per75_n` | Int64 | Sample size of plus_minus_per75: net_rtg_n, copied. |
| `lg_rtg` | Float64 | Points per 100 possessions of the season's league-average offense (all teams, all split), the zero the factor points are measured from. |
| `off_efg_pts` | Float64 | Offensive shooting (eFG%) points per 100 possessions against the league average: the part of off_rtg - lg_rtg the shooting (eFG%) factor explains, at fixed linear prices from the season's league counts (factor_points). The four off_*_pts add up to off_rtg - lg_rtg and, with the four def_*_pts, to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `off_tov_pts` | Float64 | Offensive turnovers points per 100 possessions against the league average: the part of off_rtg - lg_rtg the turnovers factor explains, at fixed linear prices from the season's league counts (factor_points). The four off_*_pts add up to off_rtg - lg_rtg and, with the four def_*_pts, to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `off_orb_pts` | Float64 | Offensive rebounding points per 100 possessions against the league average: the part of off_rtg - lg_rtg the offensive rebounding factor explains, at fixed linear prices from the season's league counts (factor_points). The four off_*_pts add up to off_rtg - lg_rtg and, with the four def_*_pts, to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `off_ft_pts` | Float64 | Offensive free throws points per 100 possessions against the league average: the part of off_rtg - lg_rtg the free throws factor explains, at fixed linear prices from the season's league counts (factor_points). The four off_*_pts add up to off_rtg - lg_rtg and, with the four def_*_pts, to net_rtg exactly. The free-throw part is the remainder of the other three: the free throws' price plus the possessions and points the box estimate does not explain (on the pros, the possession count's distance from the box estimate, which weights a free-throw attempt 0.44, less the league's average gap per FGA), so its sign can differ from the free-throw factor's. |
| `def_efg_pts` | Float64 | Defensive shooting (eFG%) points per 100 defensive possessions against the league average, positive is better for the team: the part of lg_rtg - def_rtg it explains (factor_points). The four def_*_pts add up to lg_rtg - def_rtg and, with the four off_*_pts, to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `def_tov_pts` | Float64 | Defensive turnovers points per 100 defensive possessions against the league average, positive is better for the team: the part of lg_rtg - def_rtg it explains (factor_points). The four def_*_pts add up to lg_rtg - def_rtg and, with the four off_*_pts, to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `def_orb_pts` | Float64 | Defensive rebounding points (the opponents' offensive rebounding) per 100 defensive possessions against the league average, positive is better for the team: the part of lg_rtg - def_rtg it explains (factor_points). The four def_*_pts add up to lg_rtg - def_rtg and, with the four off_*_pts, to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `def_ft_pts` | Float64 | Defensive free throws points per 100 defensive possessions against the league average, positive is better for the team: the part of lg_rtg - def_rtg it explains (factor_points). The four def_*_pts add up to lg_rtg - def_rtg and, with the four off_*_pts, to net_rtg exactly. The free-throw part is the remainder of the other three: the free throws' price plus the possessions and points the box estimate does not explain (on the pros, the possession count's distance from the box estimate, which weights a free-throw attempt 0.44, less the league's average gap per FGA), so its sign can differ from the free-throw factor's. |
| `efg_pts` | Float64 | Net shooting (eFG%) points per 100: off_efg_pts + def_efg_pts. The four (efg_pts, tov_pts, orb_pts, ft_pts) add up to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `tov_pts` | Float64 | Net turnovers points per 100: off_tov_pts + def_tov_pts. The four (efg_pts, tov_pts, orb_pts, ft_pts) add up to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `orb_pts` | Float64 | Net offensive rebounding points per 100: off_orb_pts + def_orb_pts. The four (efg_pts, tov_pts, orb_pts, ft_pts) add up to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |
| `ft_pts` | Float64 | Net free throws points per 100: off_ft_pts + def_ft_pts, the remainder parts, so more than the free throws (see off_ft_pts). The four (efg_pts, tov_pts, orb_pts, ft_pts) add up to net_rtg exactly. Null where the row has no four-factor counts (college clutch). |

**Example**

```python
from sportsdataverse.wbb import wbb_team_season_rates
wbb_team_season_rates(2025).filter(split="all").sort("net_rtg_rank").head()
```

### wbb_team_shot_zones {#wbb_team_shot_zones}

`wbb_team_shot_zones(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college season shot profile by zone per team, for and allowed.

Delegates to `sportsdataverse.mbb.mbb_shot_zone_profiles.mbb_team_shot_zones` with
`league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`: `games`, the team's zone profile and its opponents' (`opp_`), with the `_n` / `_rank` / `_pct` columns. See the mbb core for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | The team's ESPN team id (Int64). |
| `games` | Int64 | The team's games with a field-goal attempt by or against it. |
| `fga` | Int64 | Field-goal attempts by the team (games). |
| `fgm` | Int64 | Field goals made by the team. |
| `unlocated_fga` | Int64 | Attempts by the team with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the team at the rim (within 4 ft, the shot chart's rim zone). |
| `rim_fgm` | Int64 | Makes by the team at the rim (within 4 ft, the shot chart's rim zone). |
| `rim_ast` | Int64 | Assisted makes by the team at the rim (within 4 ft, the shot chart's rim zone). |
| `mid_fga` | Int64 | Attempts by the team in the mid zone (every other two: the chart's paint and mid-range). |
| `mid_fgm` | Int64 | Makes by the team in the mid zone (every other two: the chart's paint and mid-range). |
| `mid_ast` | Int64 | Assisted makes by the team in the mid zone (every other two: the chart's paint and mid-range). |
| `corner_3_fga` | Int64 | Attempts by the team from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `corner_3_fgm` | Int64 | Makes by the team from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `corner_3_ast` | Int64 | Assisted makes by the team from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `above_break_3_fga` | Int64 | Attempts by the team from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the team from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the team from above the break (every other three). |
| `opp_fga` | Int64 | Allowed: Field-goal attempts by the team's opponents (games). |
| `opp_fgm` | Int64 | Allowed: Field goals made by the team's opponents. |
| `opp_unlocated_fga` | Int64 | Allowed: Attempts by the team's opponents with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in opp_fga, in no zone. |
| `opp_rim_fga` | Int64 | Allowed: Attempts by the team's opponents at the rim (within 4 ft, the shot chart's rim zone). |
| `opp_rim_fgm` | Int64 | Allowed: Makes by the team's opponents at the rim (within 4 ft, the shot chart's rim zone). |
| `opp_rim_ast` | Int64 | Allowed: Assisted makes by the team's opponents at the rim (within 4 ft, the shot chart's rim zone). |
| `opp_mid_fga` | Int64 | Allowed: Attempts by the team's opponents in the mid zone (every other two: the chart's paint and mid-range). |
| `opp_mid_fgm` | Int64 | Allowed: Makes by the team's opponents in the mid zone (every other two: the chart's paint and mid-range). |
| `opp_mid_ast` | Int64 | Allowed: Assisted makes by the team's opponents in the mid zone (every other two: the chart's paint and mid-range). |
| `opp_corner_3_fga` | Int64 | Allowed: Attempts by the team's opponents from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `opp_corner_3_fgm` | Int64 | Allowed: Makes by the team's opponents from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `opp_corner_3_ast` | Int64 | Allowed: Assisted makes by the team's opponents from the corner (a three with \|x\| >= 21 ft and \|y\| <= 9 ft, the baseline band). |
| `opp_above_break_3_fga` | Int64 | Allowed: Attempts by the team's opponents from above the break (every other three). |
| `opp_above_break_3_fgm` | Int64 | Allowed: Makes by the team's opponents from above the break (every other three). |
| `opp_above_break_3_ast` | Int64 | Allowed: Assisted makes by the team's opponents from above the break (every other three). |
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
| `opp_rim_fg_pct` | Float64 | Allowed: opp_rim_fgm / opp_rim_fga; null with no attempt. |
| `opp_rim_freq` | Float64 | Allowed: opp_rim_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_rim_ast_pct` | Float64 | Allowed: Share of opp_rim_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `opp_mid_fg_pct` | Float64 | Allowed: opp_mid_fgm / opp_mid_fga; null with no attempt. |
| `opp_mid_freq` | Float64 | Allowed: opp_mid_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_mid_ast_pct` | Float64 | Allowed: Share of opp_mid_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `opp_corner_3_fg_pct` | Float64 | Allowed: opp_corner_3_fgm / opp_corner_3_fga; null with no attempt. |
| `opp_corner_3_freq` | Float64 | Allowed: opp_corner_3_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_corner_3_ast_pct` | Float64 | Allowed: Share of opp_corner_3_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
| `opp_above_break_3_fg_pct` | Float64 | Allowed: opp_above_break_3_fgm / opp_above_break_3_fga; null with no attempt. |
| `opp_above_break_3_freq` | Float64 | Allowed: opp_above_break_3_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_above_break_3_ast_pct` | Float64 | Allowed: Share of opp_above_break_3_fgm that were assisted, over the makes linked to a play-by-play row (whose assist is known); null with none. |
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
| `opp_rim_fg_pct_n` | Int64 | Sample size of opp_rim_fg_pct: opp_rim_fga. |
| `opp_rim_freq_n` | Int64 | Sample size of opp_rim_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_rim_ast_pct_n` | Int64 | Sample size of opp_rim_ast_pct: makes linked to a play-by-play row in the zone. |
| `opp_mid_fg_pct_n` | Int64 | Sample size of opp_mid_fg_pct: opp_mid_fga. |
| `opp_mid_freq_n` | Int64 | Sample size of opp_mid_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_mid_ast_pct_n` | Int64 | Sample size of opp_mid_ast_pct: makes linked to a play-by-play row in the zone. |
| `opp_corner_3_fg_pct_n` | Int64 | Sample size of opp_corner_3_fg_pct: opp_corner_3_fga. |
| `opp_corner_3_freq_n` | Int64 | Sample size of opp_corner_3_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_corner_3_ast_pct_n` | Int64 | Sample size of opp_corner_3_ast_pct: makes linked to a play-by-play row in the zone. |
| `opp_above_break_3_fg_pct_n` | Int64 | Sample size of opp_above_break_3_fg_pct: opp_above_break_3_fga. |
| `opp_above_break_3_freq_n` | Int64 | Sample size of opp_above_break_3_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_above_break_3_ast_pct_n` | Int64 | Sample size of opp_above_break_3_ast_pct: makes linked to a play-by-play row in the zone. |
| `rim_fg_pct_rank` | Float64 | Rank of rim_fg_pct within the season (the highest ranks 1; ties average), among teams whose rim_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `rim_freq_rank` | Float64 | Rank of rim_freq within the season (the highest ranks 1; ties average), among teams whose rim_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `rim_ast_pct_rank` | Float64 | Rank of rim_ast_pct within the season (the highest ranks 1; ties average), among teams whose rim_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_fg_pct_rank` | Float64 | Rank of mid_fg_pct within the season (the highest ranks 1; ties average), among teams whose mid_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_freq_rank` | Float64 | Rank of mid_freq within the season (the highest ranks 1; ties average), among teams whose mid_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `mid_ast_pct_rank` | Float64 | Rank of mid_ast_pct within the season (the highest ranks 1; ties average), among teams whose mid_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_fg_pct_rank` | Float64 | Rank of corner_3_fg_pct within the season (the highest ranks 1; ties average), among teams whose corner_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_freq_rank` | Float64 | Rank of corner_3_freq within the season (the highest ranks 1; ties average), among teams whose corner_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `corner_3_ast_pct_rank` | Float64 | Rank of corner_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose corner_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_fg_pct_rank` | Float64 | Rank of above_break_3_fg_pct within the season (the highest ranks 1; ties average), among teams whose above_break_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_freq_rank` | Float64 | Rank of above_break_3_freq within the season (the highest ranks 1; ties average), among teams whose above_break_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `above_break_3_ast_pct_rank` | Float64 | Rank of above_break_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose above_break_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_fg_pct_rank` | Float64 | Rank of opp_rim_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_rim_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_freq_rank` | Float64 | Rank of opp_rim_freq within the season (the highest ranks 1; ties average), among teams whose opp_rim_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_rim_ast_pct_rank` | Float64 | Rank of opp_rim_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_rim_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_fg_pct_rank` | Float64 | Rank of opp_mid_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_mid_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_freq_rank` | Float64 | Rank of opp_mid_freq within the season (the highest ranks 1; ties average), among teams whose opp_mid_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_mid_ast_pct_rank` | Float64 | Rank of opp_mid_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_mid_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_fg_pct_rank` | Float64 | Rank of opp_corner_3_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_corner_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_freq_rank` | Float64 | Rank of opp_corner_3_freq within the season (the highest ranks 1; ties average), among teams whose opp_corner_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_corner_3_ast_pct_rank` | Float64 | Rank of opp_corner_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_corner_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_fg_pct_rank` | Float64 | Rank of opp_above_break_3_fg_pct within the season (the lowest ranks 1, the stingiest defense; ties average), among teams whose opp_above_break_3_fg_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_freq_rank` | Float64 | Rank of opp_above_break_3_freq within the season (the highest ranks 1; ties average), among teams whose opp_above_break_3_freq_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
| `opp_above_break_3_ast_pct_rank` | Float64 | Rank of opp_above_break_3_ast_pct within the season (the highest ranks 1; ties average), among teams whose opp_above_break_3_ast_pct_n reaches the rate's split-half gate (TEAM_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than five teams ranked. |
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
| `opp_rim_fg_pct_pct` | Float64 | Percentile of opp_rim_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_freq_pct` | Float64 | Percentile of opp_rim_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_rim_ast_pct_pct` | Float64 | Percentile of opp_rim_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_fg_pct_pct` | Float64 | Percentile of opp_mid_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_freq_pct` | Float64 | Percentile of opp_mid_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_mid_ast_pct_pct` | Float64 | Percentile of opp_mid_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_fg_pct_pct` | Float64 | Percentile of opp_corner_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_freq_pct` | Float64 | Percentile of opp_corner_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_corner_3_ast_pct_pct` | Float64 | Percentile of opp_corner_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_fg_pct_pct` | Float64 | Percentile of opp_above_break_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_freq_pct` | Float64 | Percentile of opp_above_break_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_above_break_3_ast_pct_pct` | Float64 | Percentile of opp_above_break_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |

**Example**

```python
from sportsdataverse.wbb import wbb_team_shot_zones
wbb_team_shot_zones(2025).sort("opp_rim_freq").head()
```

### wbb_team_strength {#wbb_team_strength}

`wbb_team_strength(seasons: 'int | list[int]', *, prior: 'str | None' = 'last_season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college team strength: adjusted ratings, SOS, SOV, tempo and the four factors.

Delegates to `sportsdataverse.mbb.mbb_team_strength.mbb_team_strength` with
`league="womens"`. `adj_*_rtg` are the published `wbb_ratings`; `adj_*_rtg_ridge`
the possession ridge on the `ncaa_wbb_possessions` release.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `prior` | `str \| None` | `'last_season'` | The possession ridge's prior, `"last_season"` (default) or None (see the mbb core). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`: `games`, `wins`, `losses`, `poss`, `opp_poss`, `pts`, `opp_pts`, both sets of adjusted ratings, `sos`, `sov`, the BB-D3 rates and their `_rank` / `_pct` / `_n`. See the mbb core for the definitions.

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
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose off_rtg_n is at least 130 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose def_rtg_n is at least 470 defensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose net_rtg_n is at least 360 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the season, whose pace_n is at least 3 games (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose efg_pct_n is at least 230 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose tov_pct_n is at least 210 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose orb_pct_n is at least 160 offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose ft_rate_n is at least 550 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_efg_pct_n is at least 600 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_tov_pct_n is at least 300 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_orb_pct_n is at least 240 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_ft_rate_n is at least 420 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
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
| `adj_off_rtg_ridge` | Float64 | Adjusted offensive rating, possession-ridge method: points per 100 possessions against an average Division I defense on a neutral court. The NBA / WNBA ridge (possession_ridge_ratings) on the ncaa_wbb_possessions release, penalty COLLEGE_RIDGE_LAMBDA, home court fitted and ESPN neutral sites; possessions against a non-Division I opponent left out. Published beside the fixed-point adj_off_rtg, which it does not replace. |
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
| `ValueError` | If `prior` is not `"last_season"` or None. |

**Example**

```python
from sportsdataverse.wbb import wbb_team_strength
wbb_team_strength(2025).sort("adj_net_rtg_rank").head()
```
