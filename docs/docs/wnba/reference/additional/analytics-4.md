---
title: "WNBA — additional Python functions — Analytics: wnba_team–wnba_tracking"
sidebar_label: "Analytics: wnba_team–wnba_tracking"
sidebar_position: 10
description: "WNBA — additional Python functions — Analytics: wnba_team–wnba_tracking — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Analytics: wnba_team–wnba_tracking

### wnba_team_season_rates {#wnba_team_season_rates}

`wnba_team_season_rates(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA season team rates from possessions (league_id='10'). See sportsdataverse.nba.nba_team_season_rates.nba_team_season_rates.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, split)`. `team_id` is the stats.nba.com team id (Int64). `games`, `poss` / `opp_poss` (offensive / defensive possessions) and `pts` / `opp_pts` are the split's totals. The twelve rates are `off_rtg`, `def_rtg`, `net_rtg` (per 100 possessions), `pace` (possessions per 48 minutes, 40 in the WNBA), the four factors `efg_pct`, `tov_pct`, `orb_pct`, `ft_rate` and the same four against (`opp_*`), all fractions. Each rate has `<m>_rank` (1 = best; `def_rtg`, `tov_pct`, `opp_efg_pct`, `opp_orb_pct` and `opp_ft_rate` rank low first; pace ranks high first), `<m>_pct` (the Weibull percentile) and `<m>_n` (the sample: possessions for the ratings, the clock in regulation games for pace, field-goal attempts for eFG% and FT rate, possessions for turnover rate, rebound chances for ORB%). Ranks are within the season and split, among teams with at least half the most games whose `<m>_n` reaches the league's `~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate (a clutch split reaches none), and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | WNBA season (its calendar year). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `split` | String | Which possessions the row covers: all; home or away (the team's home or road games; neutral-site games are in neither: the curated nba_neutral_site_games list, plus every game dated inside the 2020 bubble (from the schedule's game_date)); close (every possession of a game that had a clutch possession); clutch (a possession in play with 5:00 or less left (starting there, or starting before 5:00 and ending after it) in the 4th quarter (the 2nd half through 2005, when the WNBA played two 20-minute halves) or an overtime, the score within 5 as it starts). |
| `games` | Int64 | Games with at least one possession in the split. |
| `poss` | Int64 | Offensive possessions in the split, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the split (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the split. Counted from each possession's made shots and free throws. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the split. |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 40 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction (one minus the team's defensive rebound rate). |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose off_rtg_n is at least 660 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose def_rtg_n is at least 810 defensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose net_rtg_n is at least 840 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season and split (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the split, whose pace_n is at least 7 games (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. An owner-accepted exception (2026-11-09), as on the unseen 2023-2025 seasons the 7-game gate is 0.33 signal, under a third, as measured by the gate's proxy (ESPN box possessions per game over whole regular seasons, not per split); this table's own pace scores 0.44 there. Pace stays ranked, as no data or source defect was found (rate_gates). |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose efg_pct_n is at least 750 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose tov_pct_n is at least 1,100 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose orb_pct_n is at least 220 offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose ft_rate_n is at least 720 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_efg_pct_n is at least 1,300 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_tov_pct_n is at least 350 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_orb_pct_n is at least 860 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split, whose opp_ft_rate_n is at least 880 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
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
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the split. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the team's offensive possessions in the split. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the split. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the split. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the split. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the team's defensive possessions in the split. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the split. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the split. |
| `lg_ft_pct` | Float64 | League free-throw percentage in the season and split: every team's FTM over every team's FTA, so free throws can be priced without a second read. |
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

### wnba_team_shot_zones {#wnba_team_shot_zones}

`wnba_team_shot_zones(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA season shot profile by zone per team, for and allowed (league_id='10'). See sportsdataverse.nba.nba_shot_zone_profiles.nba_team_shot_zones.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id)` (the stats.nba.com team id, Int64) with `games` and the columns of `nba_player_shot_zones` for the team and again with an `opp_` prefix for its opponents (`opp_fga`, `opp_rim_fg_pct`, ...), each rate with `<m>_n`, `<m>_rank` and `<m>_pct`, ranked within the season among teams whose `<m>_n` reaches the gate and null under five teams. `<m>_rank` is 1 = highest, except the allowed `opp_{z}_fg_pct`, where 1 is the lowest (the stingiest defense). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season: the WNBA season is its calendar year. |
| `team_id` | Int64 | stats.wnba.com team id. |
| `games` | Int64 | The team's regular-season games with a field-goal attempt by or against it. |
| `fga` | Int64 | Field-goal attempts by the team (regular-season games). |
| `fgm` | Int64 | Field goals made by the team. |
| `unlocated_fga` | Int64 | Attempts by the team with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the team at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_fgm` | Int64 | Makes by the team at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_ast` | Int64 | Assisted makes by the team at the rim (the restricted area: shot_distance under 4 ft). |
| `mid_fga` | Int64 | Attempts by the team in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_fgm` | Int64 | Makes by the team in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_ast` | Int64 | Assisted makes by the team in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `corner_3_fga` | Int64 | Attempts by the team from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_fgm` | Int64 | Makes by the team from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_ast` | Int64 | Assisted makes by the team from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `above_break_3_fga` | Int64 | Attempts by the team from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the team from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the team from above the break (every other three). |
| `opp_fga` | Int64 | Allowed: Field-goal attempts by the team's opponents (regular-season games). |
| `opp_fgm` | Int64 | Allowed: Field goals made by the team's opponents. |
| `opp_unlocated_fga` | Int64 | Allowed: Attempts by the team's opponents with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in opp_fga, in no zone. |
| `opp_rim_fga` | Int64 | Allowed: Attempts by the team's opponents at the rim (the restricted area: shot_distance under 4 ft). |
| `opp_rim_fgm` | Int64 | Allowed: Makes by the team's opponents at the rim (the restricted area: shot_distance under 4 ft). |
| `opp_rim_ast` | Int64 | Allowed: Assisted makes by the team's opponents at the rim (the restricted area: shot_distance under 4 ft). |
| `opp_mid_fga` | Int64 | Allowed: Attempts by the team's opponents in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `opp_mid_fgm` | Int64 | Allowed: Makes by the team's opponents in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `opp_mid_ast` | Int64 | Allowed: Assisted makes by the team's opponents in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `opp_corner_3_fga` | Int64 | Allowed: Attempts by the team's opponents from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `opp_corner_3_fgm` | Int64 | Allowed: Makes by the team's opponents from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `opp_corner_3_ast` | Int64 | Allowed: Assisted makes by the team's opponents from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `opp_above_break_3_fga` | Int64 | Allowed: Attempts by the team's opponents from above the break (every other three). |
| `opp_above_break_3_fgm` | Int64 | Allowed: Makes by the team's opponents from above the break (every other three). |
| `opp_above_break_3_ast` | Int64 | Allowed: Assisted makes by the team's opponents from above the break (every other three). |
| `rim_fg_pct` | Float64 | rim_fgm / rim_fga; null with no attempt. |
| `rim_freq` | Float64 | rim_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `rim_ast_pct` | Float64 | Share of rim_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `mid_fg_pct` | Float64 | mid_fgm / mid_fga; null with no attempt. |
| `mid_freq` | Float64 | mid_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `mid_ast_pct` | Float64 | Share of mid_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `corner_3_fg_pct` | Float64 | corner_3_fgm / corner_3_fga; null with no attempt. |
| `corner_3_freq` | Float64 | corner_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `corner_3_ast_pct` | Float64 | Share of corner_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `above_break_3_fg_pct` | Float64 | above_break_3_fgm / above_break_3_fga; null with no attempt. |
| `above_break_3_freq` | Float64 | above_break_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `above_break_3_ast_pct` | Float64 | Share of above_break_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `opp_rim_fg_pct` | Float64 | Allowed: opp_rim_fgm / opp_rim_fga; null with no attempt. |
| `opp_rim_freq` | Float64 | Allowed: opp_rim_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_rim_ast_pct` | Float64 | Allowed: Share of opp_rim_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `opp_mid_fg_pct` | Float64 | Allowed: opp_mid_fgm / opp_mid_fga; null with no attempt. |
| `opp_mid_freq` | Float64 | Allowed: opp_mid_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_mid_ast_pct` | Float64 | Allowed: Share of opp_mid_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `opp_corner_3_fg_pct` | Float64 | Allowed: opp_corner_3_fgm / opp_corner_3_fga; null with no attempt. |
| `opp_corner_3_freq` | Float64 | Allowed: opp_corner_3_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_corner_3_ast_pct` | Float64 | Allowed: Share of opp_corner_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `opp_above_break_3_fg_pct` | Float64 | Allowed: opp_above_break_3_fgm / opp_above_break_3_fga; null with no attempt. |
| `opp_above_break_3_freq` | Float64 | Allowed: opp_above_break_3_fga / (opp_fga - opp_unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `opp_above_break_3_ast_pct` | Float64 | Allowed: Share of opp_above_break_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `rim_fg_pct_n` | Int64 | Sample size of rim_fg_pct: rim_fga. |
| `rim_freq_n` | Int64 | Sample size of rim_freq: located attempts (fga - unlocated_fga). |
| `rim_ast_pct_n` | Int64 | Sample size of rim_ast_pct: makes in the zone. |
| `mid_fg_pct_n` | Int64 | Sample size of mid_fg_pct: mid_fga. |
| `mid_freq_n` | Int64 | Sample size of mid_freq: located attempts (fga - unlocated_fga). |
| `mid_ast_pct_n` | Int64 | Sample size of mid_ast_pct: makes in the zone. |
| `corner_3_fg_pct_n` | Int64 | Sample size of corner_3_fg_pct: corner_3_fga. |
| `corner_3_freq_n` | Int64 | Sample size of corner_3_freq: located attempts (fga - unlocated_fga). |
| `corner_3_ast_pct_n` | Int64 | Sample size of corner_3_ast_pct: makes in the zone. |
| `above_break_3_fg_pct_n` | Int64 | Sample size of above_break_3_fg_pct: above_break_3_fga. |
| `above_break_3_freq_n` | Int64 | Sample size of above_break_3_freq: located attempts (fga - unlocated_fga). |
| `above_break_3_ast_pct_n` | Int64 | Sample size of above_break_3_ast_pct: makes in the zone. |
| `opp_rim_fg_pct_n` | Int64 | Sample size of opp_rim_fg_pct: opp_rim_fga. |
| `opp_rim_freq_n` | Int64 | Sample size of opp_rim_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_rim_ast_pct_n` | Int64 | Sample size of opp_rim_ast_pct: makes in the zone. |
| `opp_mid_fg_pct_n` | Int64 | Sample size of opp_mid_fg_pct: opp_mid_fga. |
| `opp_mid_freq_n` | Int64 | Sample size of opp_mid_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_mid_ast_pct_n` | Int64 | Sample size of opp_mid_ast_pct: makes in the zone. |
| `opp_corner_3_fg_pct_n` | Int64 | Sample size of opp_corner_3_fg_pct: opp_corner_3_fga. |
| `opp_corner_3_freq_n` | Int64 | Sample size of opp_corner_3_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_corner_3_ast_pct_n` | Int64 | Sample size of opp_corner_3_ast_pct: makes in the zone. |
| `opp_above_break_3_fg_pct_n` | Int64 | Sample size of opp_above_break_3_fg_pct: opp_above_break_3_fga. |
| `opp_above_break_3_freq_n` | Int64 | Sample size of opp_above_break_3_freq: located attempts (opp_fga - opp_unlocated_fga). |
| `opp_above_break_3_ast_pct_n` | Int64 | Sample size of opp_above_break_3_ast_pct: makes in the zone. |
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

### wnba_team_strength {#wnba_team_strength}

`wnba_team_strength(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA team strength: adjusted ratings, SOS, SOV, pace and four factors (league_id='10'). See sportsdataverse.nba.nba_team_strength.nba_team_strength.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id)`; `team_id` is the stats.nba.com team id (Int64). `games`, `poss` / `opp_poss`, `pts` / `opp_pts` are the season totals from the possessions; `wins` / `losses` come from the schedule's final scores. The rated metrics are `adj_off_rtg`, `adj_def_rtg`, `adj_net_rtg`, `sos`, `sov` and the BB-D3 rates (`off_rtg`, `def_rtg`, `net_rtg`, `pace`, the four factors and the four against). Each has `<m>_rank` (1 = best; `adj_def_rtg`, `def_rtg`, `tov_pct` and the opponent shooting, rebounding and free-throw rates rank low first; `sos` ranks the hardest schedule first), `<m>_pct` (the Weibull percentile) and `<m>_n` (possessions for the ratings, completed games against a rated opponent for SOS, the clock in regulation games for pace, wins over a rated opponent for SOV). Ranks are within the season, among teams with at least half the most games, the BB-D3 rates' also behind their `~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate, and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | WNBA season (its calendar year). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `games` | Int64 | Games with at least one possession in the season (from the possession release). |
| `wins` | Int64 | Wins in the season type's completed games (the schedule's final scores). |
| `losses` | Int64 | Losses in the season type's completed games. |
| `poss` | Int64 | Offensive possessions in the season, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the season (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the season. Counted from each possession's made shots and free throws. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the season. |
| `adj_off_rtg` | Float64 | Adjusted offensive rating: points per 100 possessions against an average defense on a neutral court. The ridge intercept plus the team's offense coefficient (points per 100 on each possession on offense, defense and home terms; penalty RIDGE_LAMBDA). |
| `adj_def_rtg` | Float64 | Adjusted defensive rating: points allowed per 100 possessions against an average offense on a neutral court. The ridge intercept plus the team's defense coefficient; lower is better. |
| `adj_net_rtg` | Float64 | Adjusted net rating: adj_off_rtg minus adj_def_rtg. |
| `sos` | Float64 | Strength of schedule: the mean adj_net_rtg of the opponents of the team's completed games, one weight per game (an opponent met four times counts four times). |
| `sov` | Float64 | Strength of victory: the mean adj_net_rtg of the opponents the team beat, one weight per win; null with no win. |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 40 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction (one minus the team's defensive rebound rate). |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. |
| `adj_off_rtg_rank` | Float64 | Rank of adj_off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_def_rtg_rank` | Float64 | Rank of adj_def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `adj_net_rtg_rank` | Float64 | Rank of adj_net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `sos_rank` | Float64 | Rank of sos within the season (the highest value ranks 1; the hardest schedule ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `sov_rank` | Float64 | Rank of sov within the season (the highest value ranks 1; the strongest wins rank 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose off_rtg_n is at least 660 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose def_rtg_n is at least 810 defensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose net_rtg_n is at least 840 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the season, whose pace_n is at least 7 games (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. An owner-accepted exception (2026-11-09), as on the unseen 2023-2025 seasons the 7-game gate is 0.33 signal, under a third, as measured by the gate's proxy (ESPN box possessions per game over whole regular seasons, not per split); this table's own pace scores 0.44 there. Pace stays ranked, as no data or source defect was found (rate_gates). |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose efg_pct_n is at least 750 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose tov_pct_n is at least 1,100 offensive possessions (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose orb_pct_n is at least 220 offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose ft_rate_n is at least 720 field-goal attempts (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_efg_pct_n is at least 1,300 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_tov_pct_n is at least 350 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_orb_pct_n is at least 860 opponent offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, whose opp_ft_rate_n is at least 880 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate, a null rate, or fewer than five qualifiers. |
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
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the season. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the team's offensive possessions in the season. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the season. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the season. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the season. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the team's defensive possessions in the season. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the season. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the season. |

### wnba_tracking_drive_value {#wnba_tracking_drive_value}

`wnba_tracking_drive_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA drive value + rim-pressure (`league_id="10"` by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_drive_value`
for the full recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, drives:Float64, drive_pts:Float64, drive_baseline_rate:Float64, drive_expected:Float64, drive_pts_oe:Float64, drive_pts_oe_per_36:Float64, drive_fta:Float64, rim_pressure:Float64, drive_ast:Float64, drive_tov:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com (WNBA league id 10), which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_drive_value
df = wnba_tracking_drive_value(2024)
print(df.sort("drive_pts_oe", descending=True).head())
```

### wnba_tracking_pass_value {#wnba_tracking_pass_value}

`wnba_tracking_pass_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, fetch_potential_assists: 'bool' = False, max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _pass_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA expected-assists / passer value (`league_id="10"` by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_pass_value`
for the full recipe (Passing-measure proxy + optional `playerdashptpass`
enrichment).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `fetch_potential_assists` | `bool` | `False` | Enrich the top passers with `playerdashptpass` potential-assist counts. |
| `max_players` | `int` | `0` | Cap on per-player enrichment fetches; `0` disables enrichment regardless of `fetch_potential_assists`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |
| `_pass_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_playerdashptpass`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, ast:Float64, passes:Float64, ast_baseline_rate:Float64, ast_expected:Float64, ast_oe:Float64, ast_oe_per_36:Float64, ast_pts_created:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com (WNBA league id 10), which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_pass_value
df = wnba_tracking_pass_value(2024)
print(df.sort("ast_oe", descending=True).head())
```

### wnba_tracking_reb_oe {#wnba_tracking_reb_oe}

`wnba_tracking_reb_oe(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA rebounding-over-expected (`league_id="10"` by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_reb_oe`
for the full recipe (contest-difficulty-adjusted expected rebounds,
role-bucket baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, reb:Float64, reb_chances:Float64, reb_baseline_rate:Float64, reb_expected:Float64, reb_oe:Float64, reb_oe_per_36:Float64, oreb_oe:Float64, dreb_oe:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com (WNBA league id 10), which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_reb_oe
df = wnba_tracking_reb_oe(2024)
print(df.sort("reb_oe", descending=True).head())
```

### wnba_tracking_rim_protect_value {#wnba_tracking_rim_protect_value}

`wnba_tracking_rim_protect_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, source: 'str' = 'leaguedash', max_players: 'int' = 0, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None, _defend_get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA rim-protection / shot-defend points-saved (`league_id="10"`

by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_rim_protect_value`
for the full recipe (bucket-mean defended-rate baseline; optional
`playerdashptshotdefend` rim-band enrichment).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to the fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute the baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `source` | `str` | `'leaguedash'` | `"leaguedash"` (default) or `"shotdefend"`. |
| `max_players` | `int` | `0` | Cap on per-player `shotdefend` enrichment fetches; ignored unless `source="shotdefend"`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |
| `_defend_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_playerdashptshotdefend`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, gp:Int64, min:Float64, d_fga:Float64, d_fgm:Float64, d_fg_pct:Float64, normal_fg_pct:Float64, rim_protect_pts_saved:Float64, rim_protect_pts_saved_per_36:Float64, source:Utf8, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com (WNBA league id 10), which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_rim_protect_value
df = wnba_tracking_rim_protect_value(2024)
print(df.sort("rim_protect_pts_saved", descending=True).head())
```

### wnba_tracking_shot_diet_value {#wnba_tracking_shot_diet_value}

`wnba_tracking_shot_diet_value(seasons: "'int | str | list'", *, league_id: 'str' = '10', per_mode: 'str' = 'Totals', by_position: 'bool' = True, positions: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, _get_fn: 'Optional[Callable[..., dict]]' = None) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA catch-&-shoot vs pull-up points-over-expected (`league_id="10"`

by-reference shim).

See `sportsdataverse.nba.nba_tracking_value.nba_tracking_shot_diet_value`
for the full recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| str \| list` |  | A single season or list of seasons. |
| `league_id` | `str` | `'10'` | Defaults to `"10"` (WNBA); pass `"20"` here for G-League. |
| `per_mode` | `str` | `'Totals'` | `per_mode_simple` passed to each fetch (default `"Totals"`). |
| `by_position` | `bool` | `True` | Compute each measure's baseline within role buckets (default); `False` forces one league-wide bucket. |
| `positions` | `Optional[DataFrame]` | `None` | Optional pre-fetched positions frame. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `_get_fn` | `Optional[Callable[..., dict]]` | `None` | Injectable replacement for `nba_stats_leaguedashptstats`. |

**Returns**

One row per player-season: `season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8, position_bucket:Utf8, cs_fga:Float64, cs_pts:Float64, cs_pts_oe:Float64, pu_fga:Float64, pu_pts:Float64, pu_pts_oe:Float64, shot_diet_delta:Float64, league_id:Utf8`. Empty/malformed input returns a zero-row frame with this schema.

No returns table is published for this function: no capture: it reads stats.nba.com (WNBA league id 10), which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.wnba import wnba_tracking_shot_diet_value
df = wnba_tracking_shot_diet_value(2024)
print(df.sort("cs_pts_oe", descending=True).head())
```
