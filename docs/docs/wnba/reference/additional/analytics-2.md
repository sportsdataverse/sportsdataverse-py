---
title: "WNBA — additional Python functions — Analytics: wnba_lineup–wnba_shot"
sidebar_label: "Analytics: wnba_lineup–wnba_shot"
sidebar_position: 8
description: "WNBA — additional Python functions — Analytics: wnba_lineup–wnba_shot — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Analytics: wnba_lineup–wnba_shot

### wnba_lineup_seasons {#wnba_lineup_seasons}

`wnba_lineup_seasons(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', min_poss: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA lineup seasons (league_id='10'). See sportsdataverse.nba.nba_lineup_seasons.nba_lineup_seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `min_poss` | `int \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

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
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss. Null with a zero denominator. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss. Null with a zero denominator. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `orb_pct` | Float64 | Offensive-rebound rate: OREB / (OREB + the opponent's DREB), as a fraction. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `ft_rate` | Float64 | Free-throw rate: FTA / FGA. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `opp_efg_pct` | Float64 | The opponent's effective field-goal percentage with the unit on the floor. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `opp_tov_pct` | Float64 | The opponent's turnovers per defensive possession. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `opp_orb_pct` | Float64 | The opponent's offensive-rebound rate: its OREB / (its OREB + the unit's DREB). Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `opp_ft_rate` | Float64 | The opponent's free-throw rate: FTA / FGA. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
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

### wnba_matchup_drapm {#wnba_matchup_drapm}

`wnba_matchup_drapm(season: 'str', *, matchups: "'Optional[pl.DataFrame]'" = None, config: "'Optional[PlaytypeConfig]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA matchup defensive RAPM (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm` to the
women's league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `matchups` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leagueseasonmatchups`-shaped frame. |
| `config` | `Optional[PlaytypeConfig]` | `None` | `~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig` override. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm`.

No returns table is published for this function: no capture: it reads stats.nba.com (WNBA league id 10), which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.wnba import wnba_matchup_drapm
d = wnba_matchup_drapm("2024")
print(d.sort("matchup_drapm", descending=True).head())
```

### wnba_player_factor_rapm {#wnba_player_factor_rapm}

`wnba_player_factor_rapm(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', min_poss: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA four-factor RAPM split (league_id='10'). See sportsdataverse.nba.nba_player_factor_rapm.nba_player_factor_rapm.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `min_poss` | `int \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, player_id)`: `off_poss` / `def_poss`, `rapm` / `o_rapm` / `d_rapm`, the ten components (the four factors and the remainder, each way; per 100 possessions, higher is better), `<m>_rank` / `<m>_pct` within the season among players with at least `min_poss` possessions, `<m>_se` (clustered by game), `<m>_n`, and the `pts_per_fga` and `ridge_alpha` of the fit. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `player_id` | Int64 | stats.nba.com player id. |
| `off_poss` | Int64 | Offensive possessions the player was on the floor for (with all ten lineup slots and counts known). |
| `def_poss` | Int64 | Defensive possessions the player was on the floor for (with all ten lineup slots and counts known). |
| `rapm` | Float64 | Plain RAPM per 100 possessions: o_rapm + d_rapm. |
| `o_rapm` | Float64 | Offensive plain RAPM per 100 possessions (nba_rapm's single-season ridge, shrunk to zero); equals o_efg_rapm + o_tov_rapm + o_orb_rapm + o_ftr_rapm + o_other_rapm. Equals wnba_player_impact's (2024, 2025: to 1e-11). |
| `d_rapm` | Float64 | Defensive plain RAPM per 100 possessions, sign-flipped so higher is better; equals d_efg_rapm + d_tov_rapm + d_drb_rapm + d_ftr_rapm + d_other_rapm. Equals wnba_player_impact's (2024, 2025: to 1e-11). |
| `o_efg_rapm` | Float64 | Offensive shooting (eFG) component of o_rapm: the ridge fit to field-goal points less pts_per_fga per attempt, per 100 offensive possessions. |
| `o_tov_rapm` | Float64 | Offensive turnover component of o_rapm: the ridge fit to -pts_per_fga per turnover, per 100 offensive possessions (positive = fewer turnovers). |
| `o_orb_rapm` | Float64 | Offensive rebounding component of o_rapm: the ridge fit to +pts_per_fga per offensive rebound, per 100 offensive possessions. |
| `o_ftr_rapm` | Float64 | Offensive free-throw component of o_rapm, on possessions with a free-throw attempt: the ridge fit to free-throw points less pts_per_fga per chance that ended in neither a shot nor a turnover (FT rate and FT%), per 100 offensive possessions. |
| `o_other_rapm` | Float64 | Offensive remainder of o_rapm (bookkeeping, not a skill): the ridge fit to what the four factors leave of points - pts_per_fga, i.e. -pts_per_fga per chance with no shot, turnover or free throw (a period running out) and +pts_per_fga per shot or turnover beyond the recorded chances (an unrecorded offensive rebound); non-zero on about 1% of possessions. |
| `d_efg_rapm` | Float64 | Defensive shooting (opponent eFG) component of d_rapm, per 100 possessions, sign-flipped so higher is better: positive = the opponent shoots worse. |
| `d_tov_rapm` | Float64 | Defensive turnover component of d_rapm, per 100 possessions, sign-flipped so higher is better: positive = forces more turnovers. |
| `d_drb_rapm` | Float64 | Defensive rebounding component of d_rapm, per 100 possessions, sign-flipped so higher is better: positive = fewer opponent offensive rebounds. |
| `d_ftr_rapm` | Float64 | Defensive free-throw component of d_rapm, per 100 possessions, sign-flipped so higher is better: positive = fewer opponent free-throw points. |
| `d_other_rapm` | Float64 | Defensive remainder of d_rapm (bookkeeping, not a skill), per 100 possessions, sign-flipped so higher is better: the opponent's o_other part. |
| `rapm_rank` | Float64 | Rank of rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) rapm repeats at Spearman 0.374, so single-season ranks are noisy. |
| `rapm_pct` | Float64 | Percentile of rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) rapm repeats at Spearman 0.374, so single-season ranks are noisy. |
| `o_rapm_rank` | Float64 | Rank of o_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_rapm repeats at Spearman 0.352, so single-season ranks are noisy. |
| `o_rapm_pct` | Float64 | Percentile of o_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_rapm repeats at Spearman 0.352, so single-season ranks are noisy. |
| `d_rapm_rank` | Float64 | Rank of d_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_rapm repeats at Spearman 0.163, so single-season ranks are noisy. |
| `d_rapm_pct` | Float64 | Percentile of d_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_rapm repeats at Spearman 0.163, so single-season ranks are noisy. |
| `o_efg_rapm_rank` | Float64 | Rank of o_efg_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_efg_rapm repeats at Spearman 0.304, so single-season ranks are noisy. |
| `o_efg_rapm_pct` | Float64 | Percentile of o_efg_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_efg_rapm repeats at Spearman 0.304, so single-season ranks are noisy. |
| `o_tov_rapm_rank` | Float64 | Rank of o_tov_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_tov_rapm repeats at Spearman 0.357, so single-season ranks are noisy. |
| `o_tov_rapm_pct` | Float64 | Percentile of o_tov_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_tov_rapm repeats at Spearman 0.357, so single-season ranks are noisy. |
| `o_orb_rapm_rank` | Float64 | Rank of o_orb_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_orb_rapm repeats at Spearman 0.307, so single-season ranks are noisy. |
| `o_orb_rapm_pct` | Float64 | Percentile of o_orb_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_orb_rapm repeats at Spearman 0.307, so single-season ranks are noisy. |
| `o_ftr_rapm_rank` | Float64 | Rank of o_ftr_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_ftr_rapm repeats at Spearman 0.391, so single-season ranks are noisy. |
| `o_ftr_rapm_pct` | Float64 | Percentile of o_ftr_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_ftr_rapm repeats at Spearman 0.391, so single-season ranks are noisy. |
| `o_other_rapm_rank` | Float64 | Rank of o_other_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_other_rapm repeats at Spearman 0.157, so single-season ranks are noisy. |
| `o_other_rapm_pct` | Float64 | Percentile of o_other_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) o_other_rapm repeats at Spearman 0.157, so single-season ranks are noisy. |
| `d_efg_rapm_rank` | Float64 | Rank of d_efg_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_efg_rapm repeats at Spearman 0.128, so single-season ranks are noisy. |
| `d_efg_rapm_pct` | Float64 | Percentile of d_efg_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_efg_rapm repeats at Spearman 0.128, so single-season ranks are noisy. |
| `d_tov_rapm_rank` | Float64 | Rank of d_tov_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_tov_rapm repeats at Spearman 0.337, so single-season ranks are noisy. |
| `d_tov_rapm_pct` | Float64 | Percentile of d_tov_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_tov_rapm repeats at Spearman 0.337, so single-season ranks are noisy. |
| `d_drb_rapm_rank` | Float64 | Rank of d_drb_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_drb_rapm repeats at Spearman 0.128, so single-season ranks are noisy. |
| `d_drb_rapm_pct` | Float64 | Percentile of d_drb_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_drb_rapm repeats at Spearman 0.128, so single-season ranks are noisy. |
| `d_ftr_rapm_rank` | Float64 | Rank of d_ftr_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_ftr_rapm repeats at Spearman 0.306, so single-season ranks are noisy. |
| `d_ftr_rapm_pct` | Float64 | Percentile of d_ftr_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_ftr_rapm repeats at Spearman 0.306, so single-season ranks are noisy. |
| `d_other_rapm_rank` | Float64 | Rank of d_other_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 200); null below 10 qualifiers. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_other_rapm repeats at Spearman 0.343, so single-season ranks are noisy. |
| `d_other_rapm_pct` | Float64 | Percentile of d_other_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024 to 2025, 92 players over 1,000 possessions) d_other_rapm repeats at Spearman 0.343, so single-season ranks are noisy. |
| `rapm_se` | Float64 | Standard error of rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_rapm_se` | Float64 | Standard error of o_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_rapm_se` | Float64 | Standard error of d_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_efg_rapm_se` | Float64 | Standard error of o_efg_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_tov_rapm_se` | Float64 | Standard error of o_tov_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_orb_rapm_se` | Float64 | Standard error of o_orb_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_ftr_rapm_se` | Float64 | Standard error of o_ftr_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_other_rapm_se` | Float64 | Standard error of o_other_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_efg_rapm_se` | Float64 | Standard error of d_efg_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_tov_rapm_se` | Float64 | Standard error of d_tov_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_drb_rapm_se` | Float64 | Standard error of d_drb_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_ftr_rapm_se` | Float64 | Standard error of d_ftr_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_other_rapm_se` | Float64 | Standard error of d_other_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `rapm_n` | Int64 | Sample size of rapm: off_poss + def_poss. |
| `o_rapm_n` | Int64 | Sample size of o_rapm: off_poss. |
| `d_rapm_n` | Int64 | Sample size of d_rapm: def_poss. |
| `o_efg_rapm_n` | Int64 | Sample size of o_efg_rapm: off_poss. |
| `o_tov_rapm_n` | Int64 | Sample size of o_tov_rapm: off_poss. |
| `o_orb_rapm_n` | Int64 | Sample size of o_orb_rapm: off_poss. |
| `o_ftr_rapm_n` | Int64 | Sample size of o_ftr_rapm: off_poss. |
| `o_other_rapm_n` | Int64 | Sample size of o_other_rapm: off_poss. |
| `d_efg_rapm_n` | Int64 | Sample size of d_efg_rapm: def_poss. |
| `d_tov_rapm_n` | Int64 | Sample size of d_tov_rapm: def_poss. |
| `d_drb_rapm_n` | Int64 | Sample size of d_drb_rapm: def_poss. |
| `d_ftr_rapm_n` | Int64 | Sample size of d_ftr_rapm: def_poss. |
| `d_other_rapm_n` | Int64 | Sample size of d_other_rapm: def_poss. |
| `pts_per_fga` | Float64 | The season's points per field-goal attempt, the value of one shot used in the split (pts_per_fga per turnover, offensive rebound and free-throw chance). |
| `ridge_alpha` | Float64 | The ridge penalty of the fit, shared by every component so they add up: plain RAPM's fixed league value (NBA 2,683, WNBA 1,931; sportsdataverse.nba.nba_rapm.RAPM_PENALTY), the same in every season. Rows fit before November 2026 carry RidgeCV's per-season leave-one-out pick instead, on a scale that changed with it. |

### wnba_player_on_off {#wnba_player_on_off}

`wnba_player_on_off(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', min_poss: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA player on/off (league_id='10'). See sportsdataverse.nba.nba_player_on_off.nba_player_on_off.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `min_poss` | `int \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, player_id)`: `games` on the floor and `team_games`, then for `_on` and `_off` the `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions); `<rating>_diff` (on minus off); `<m>_rank` / `<m>_pct` for the on and diff ratings (within the season, among players with at least `min_poss` both ways) and `<m>_n` for every rating. `poss_on + poss_off` is the team's possessions. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id` | Int64 | stats.nba.com player (person) id. |
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
| `off_rtg_on_rank` | Float64 | Rank of off_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200); null below 10 qualifiers. |
| `def_rtg_on_rank` | Float64 | Rank of def_rtg_on within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200); null below 10 qualifiers. |
| `net_rtg_on_rank` | Float64 | Rank of net_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200); null below 10 qualifiers. |
| `off_rtg_diff_rank` | Float64 | Rank of off_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200); null below 10 qualifiers. |
| `def_rtg_diff_rank` | Float64 | Rank of def_rtg_diff within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200); null below 10 qualifiers. |
| `net_rtg_diff_rank` | Float64 | Rank of net_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200); null below 10 qualifiers. |
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

### wnba_player_props {#wnba_player_props}

`wnba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '10', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

WNBA player props (league_id='10'). See sportsdataverse.nba.nba_player_props.nba_player_props.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  |  |
| `game_id` | `str` |  |  |
| `home_team_id` | `str` |  |  |
| `away_team_id` | `str` |  |  |
| `league_id` | `str` | `'10'` |  |
| `as_of_date` | `Union[date, None]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

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

### wnba_player_war {#wnba_player_war}

`wnba_player_war(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA WAR and VORP per player-season from our BPM, RAPM and BOBAN (league_id='10'). See sportsdataverse.nba.nba_player_war.nba_player_war.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, player_id)` (stats.nba.com ids, Int64): `player_name`, `team_id` (the team he played most possessions for), `teams` (how many he played for), `min`, `poss` (on-court possessions, offensive plus defensive over 2), `rating` and `rating_bpm` / `rating_rapm` / `rating_boban` (anchored, per 100), `vorp`, `war` with `war_lo` / `war_hi` (a nominal 90% interval from a bootstrap over each team-season's games: the sampling uncertainty of the team side and the playing time) and `war_bpm` / `war_rapm` / `war_boban` (the source spread), with `_rank` (1 = most), `_pct` and `_n` (possessions) for `vorp` and `war` among every player of the season. Sorted by season and `war`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26; the WNBA season is its calendar year). |
| `player_id` | Int64 | stats.nba.com player id. |
| `player_name` | String | Player name, from the player impact release. |
| `team_id` | Int64 | stats.nba.com team id of the team he played the most possessions for. |
| `teams` | Int64 | How many teams he played for in the regular season. |
| `min` | Float64 | Regular-season minutes (the player impact release's). |
| `poss` | Float64 | On-court possessions, offensive plus defensive over 2, counted from the stats.nba.com possessions (every possession row, BB-D3's convention). The minutes basis of vorp and war. |
| `rating` | Float64 | Points per 100 possessions above a league-average player: the mean of rating_bpm, rating_rapm and rating_boban. |
| `rating_bpm` | Float64 | Our BPM per 100, team-anchored and centred: the release's bpm with its team constant repaired (minute-weighted team mean = net rating / 5, BPM 2.0's), minus the player's team mean, plus the team's net rating / 5, centred so the league's possession-weighted mean is 0. |
| `rating_rapm` | Float64 | Our prior-informed RAPM (adj_rapm) per 100, team-anchored and centred like rating_bpm. Only the net adj_rapm is read: the o_adj_rapm / d_adj_rapm split carries a +11 / -11 offset. |
| `rating_boban` | Float64 | Our BOBAN (boban_filtered_skill) per 100, team-anchored and centred like rating_bpm. |
| `vorp` | Float64 | Value over replacement player: (rating - R) * poss / the season's possessions per team, R the league's replacement level (REPLACEMENT_LEVEL; NBA -1.98, WNBA -3.18 per 100). Basketball-Reference's VORP scale: a season of possessions at R + 1 is 1. |
| `war` | Float64 | Wins above replacement: (rating - R) * poss / 100 / points per win (PTS_PER_WIN; NBA 31.56, WNBA 31.31). 0 at replacement level; a season's war adds up to the league's wins minus a team of replacement players' (.189 NBA 2024-25, .104 WNBA 2025). |
| `war_lo` | Float64 | Low end of the nominal 90% interval for war (5th percentile of 1000 bootstrap replicates, each team-season seeded on its own): each team-season's games resampled with replacement (a game's player possessions, team possessions and margin move together), then his possessions, the team net rating, the anchoring and the centring recomputed. The source ratings and the replacement level are held fixed, so this is the sampling uncertainty of the team side and the playing time; the war_bpm / war_rapm / war_boban spread is the source uncertainty. |
| `war_hi` | Float64 | High end of the nominal 90% interval for war (95th percentile of the same 1000 game-resampled replicates as war_lo). |
| `war_bpm` | Float64 | war from rating_bpm alone. Every source shares the league's replacement level, so war_bpm, war_rapm and war_boban add up to the same league total. |
| `war_rapm` | Float64 | war from rating_rapm alone. |
| `war_boban` | Float64 | war from rating_boban alone. BOBAN shrinks low-minute players toward average, so the replacement pool carries more war_boban than war_bpm (the replacement level is a box-score level). |
| `vorp_rank` | Float64 | Rank of vorp within the season among every player (1 = most; ties average); null under 10 players. |
| `war_rank` | Float64 | Rank of war within the season among every player (1 = most; ties average); null under 10 players. |
| `vorp_pct` | Float64 | Percentile of vorp within the season: 100 * (n + 1 - rank) / (n + 1), n the players ranked; null when the rank is null. |
| `war_pct` | Float64 | Percentile of war within the season: 100 * (n + 1 - rank) / (n + 1), n the players ranked; null when the rank is null. |
| `vorp_n` | Int64 | Sample size of vorp: on-court possessions, rounded. |
| `war_n` | Int64 | Sample size of war: on-court possessions, rounded. |

### wnba_player_wowy {#wnba_player_wowy}

`wnba_player_wowy(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA WOWY pairs (league_id='10'). See sportsdataverse.nba.nba_player_on_off.nba_player_wowy.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, player_id_a, player_id_b)` (`player_id_a` the smaller id) for every pair of the team's players: for `_both` (together), `_a_only`, `_b_only` and `_neither` the `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and `off_rtg` / `def_rtg` / `net_rtg`. The four splits' totals add up to the team's season; the ratings do not add. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id_a` | Int64 | The first player of the pair, the smaller id. |
| `player_id_b` | Int64 | The second player of the pair, the larger id. |
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

### wnba_playtype_ratings {#wnba_playtype_ratings}

`wnba_playtype_ratings(season: 'str', *, off_team: "'Optional[pl.DataFrame]'" = None, def_team: "'Optional[pl.DataFrame]'" = None, schedule: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

WNBA Synergy play-type-adjusted offense/defense (`league_id="10"`).

Thin wrapper binding
`sportsdataverse.nba.nba_playtype.nba_playtype_ratings` to the
women's league. Synergy coverage is sparse for the WNBA; an empty upstream
fetch degrades to a zero-row frame (never raises).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Same schema as `sportsdataverse.nba.nba_playtype.nba_playtype_ratings`.

No returns table is published for this function: no capture: it reads stats.nba.com (WNBA league id 10), which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.wnba import wnba_playtype_ratings
r = wnba_playtype_ratings("2024")
print(r.sort("adj_off", descending=True).head())
```

### wnba_referee_assignments {#wnba_referee_assignments}

`wnba_referee_assignments(date: 'str | _dt.date', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse WNBA referee assignments for a given date from official.nba.com.

Retrieves the referee crew assignments and replay center officials for all WNBA
games on a given date. The `crew_position` column (1–4) represents the feed's
slot order; slot 1 is inferred to be the crew chief. The `season` column is
the WNBA single-year season (feed year converted as-is). This is a thin shim
over `sportsdataverse.nba.nba_officiating.nba_referee_assignments` that
sets `league="wnba"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `str \| date` |  | The date to fetch assignments for (str in "YYYY-MM-DD" format or datetime.date). |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) with all three leagues instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

A dict with keys `"officials"` and `"replay_center"` mapping to DataFrames. If `raw=True`, returns the full three-league JSON payload instead.

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
| `ValueError` | `date` is not a valid "YYYY-MM-DD" date (checked before any request). |
| `NoDataError` | Official.nba.com answered 404, or 403 with S3's `AccessDenied` body. A date without WNBA games is not this (see the Note). |
| `AssetFetchError` | The fetch failed (network error, rate limit, or Akamai WAF block), the 200 body is not a JSON object, or the `wnba` block's `Table`/`Table1` rows are missing or malformed (a row without `game_id` included). With `raw=True` all three leagues (nba, gl and wnba) are checked, since the whole payload is returned. |

**Example**

```python
from sportsdataverse.wnba.wnba_officiating import wnba_referee_assignments
result = wnba_referee_assignments("2026-06-13")
officials = result["officials"]
print(f"Found {officials.height} official slots")
```

### wnba_season_sim {#wnba_season_sim}

`wnba_season_sim(season: 'int', *, as_of_date: 'dt.date | None' = None, league_id: 'str' = '10', n_sims: 'int' = 10000, seed: 'int' = 20261103, chances: 'list[float] | None' = None, cup_schedule: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA season sim: projected W-L, seed, playoff and lottery odds (league_id='10'). See sportsdataverse.nba.nba_season_sim.nba_season_sim.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  |  |
| `as_of_date` | `date \| None` | `None` |  |
| `league_id` | `str` | `'10'` |  |
| `n_sims` | `int` | `10000` |  |
| `seed` | `int` | `20261103` |  |
| `chances` | `list[float] \| None` | `None` |  |
| `cup_schedule` | `DataFrame \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

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

### wnba_season_standings {#wnba_season_standings}

`wnba_season_standings(games: 'pl.DataFrame', *, league_id: 'str' = '10', seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA standings with the WNBA tiebreakers (league_id='10'). See sportsdataverse.nba.nba_season_rules.nba_season_standings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  |  |
| `league_id` | `str` | `'10'` |  |
| `seed` | `int` | `0` |  |
| `return_as_pandas` | `bool` | `False` |  |

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

### wnba_shot_value {#wnba_shot_value}

`wnba_shot_value(player_ids: "'list[int]'", season: 'str', *, include_context: 'bool' = False, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

WNBA one-call shot-value spine (`league_id="10"`).

Thin wrapper binding `sportsdataverse.nba.nba_shot_value.nba_shot_value`
to the women's league; fetches each player's `shotchartdetail`, scores
per-shot expected points from the free `LeagueAverages` zone table, and
returns the scored shots plus shooter talent, selection quality, and
zone-value maps (and the defender/shot-clock context tables when
`include_context=True`). Women's court geometry + shrinkage constant are
keyed `"10"` in `nba_shot_value_constants`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_ids` | `list[int]` |  | Player ids to fetch. |
| `season` | `str` |  | Season string, e.g. `"2024"`. |
| `include_context` | `bool` | `False` | Also fetch + return the `playerdashptshots` defender/shot-clock context tables. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"shots", "talent", "selection", "zones"}` (plus `"context"` when requested). An empty fetch returns a dict of zero-row frames.

**Example**

```python
from sportsdataverse.wnba import wnba_shot_value
out = wnba_shot_value([1628886], "2024")
out["talent"].head()
```
