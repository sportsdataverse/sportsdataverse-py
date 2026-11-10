---
title: "WNBA — additional Python functions — Analytics: wnba_game–wnba_player"
sidebar_label: "Analytics: wnba_game–wnba_player"
sidebar_position: 8
description: "WNBA — additional Python functions — Analytics: wnba_game–wnba_player — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Analytics: wnba_game–wnba_player

### wnba_game_units {#wnba_game_units}

`wnba_game_units(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA in-game units (league_id='10'). See sportsdataverse.nba.nba_game_stints.nba_game_units.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, game_id, team_id, n_players, lineup_id)`: `opp_team_id`, `n_players` (1 to 5), `lineup_id` (the player ids sorted and joined with `-`), `player_1..5`, `minutes` / `seconds`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions), the four factors for (`efg_pct`, `tov_pct`, `orb_pct`, `ft_rate`) and against (`opp_*`), the shot zones for and against (`rim_rate`, `mid_rate`, `fg3_rate` shares of field-goal attempts, `rim_pct`, `mid_pct`, `fg3_pct` make rates, `opp_*`) as fractions, and every rate's `<m>_n`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `team_id` | Int64 | stats.nba.com team id. |
| `opp_team_id` | Int64 | The opponent's team id. |
| `n_players` | Int64 | Unit size: 1 to 5 players. |
| `lineup_id` | String | The unit's player ids, sorted and joined with '-'. |
| `player_1` | Int64 | Player 1 of the unit, in lineup_id order; null past n_players. |
| `player_2` | Int64 | Player 2 of the unit, in lineup_id order; null past n_players. |
| `player_3` | Int64 | Player 3 of the unit, in lineup_id order; null past n_players. |
| `player_4` | Int64 | Player 4 of the unit, in lineup_id order; null past n_players. |
| `player_5` | Int64 | Player 5 of the unit, in lineup_id order; null past n_players. |
| `minutes` | Float64 | Game-clock minutes the unit's players were all on the floor together (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds the unit's players were all on the floor together. |
| `poss` | Int64 | Offensive possessions with the unit on the floor (a possession counts for the five on the floor when it starts). |
| `opp_poss` | Int64 | Defensive possessions with the unit on the floor, counted as poss is. |
| `pts` | Int64 | The team's points with the unit on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts` | Int64 | The opponent's points with the unit on the floor, credited as pts is. |
| `plus_minus` | Int64 | The unit's +/- in the game: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss. Null with a zero denominator. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss. Null with a zero denominator. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Null with a zero denominator. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Null with a zero denominator. |
| `orb_pct` | Float64 | Offensive-rebound rate: own offensive rebounds over own offensive rebounds plus the opponent's defensive rebounds. Null with a zero denominator. |
| `ft_rate` | Float64 | Free-throw rate: FTA / FGA. Null with a zero denominator. |
| `opp_efg_pct` | Float64 | The opponent's effective field-goal percentage with the unit on the floor. Null with a zero denominator. |
| `opp_tov_pct` | Float64 | The opponent's turnovers per defensive possession, as a fraction. Null with a zero denominator. |
| `opp_orb_pct` | Float64 | The opponent's offensive-rebound rate with the unit on the floor. Null with a zero denominator. |
| `opp_ft_rate` | Float64 | The opponent's free-throw rate: FTA / FGA. Null with a zero denominator. |
| `rim_rate` | Float64 | Share of the unit's field-goal attempts at the rim (a two under 4 feet). Null with a zero denominator. |
| `mid_rate` | Float64 | Share of the unit's field-goal attempts that are other twos (mid-range). Null with a zero denominator. |
| `fg3_rate` | Float64 | Share of the unit's field-goal attempts that are threes. Null with a zero denominator. |
| `rim_pct` | Float64 | Field-goal percentage at the rim, as a fraction. Null with a zero denominator. |
| `mid_pct` | Float64 | Mid-range field-goal percentage, as a fraction. Null with a zero denominator. |
| `fg3_pct` | Float64 | Three-point percentage, as a fraction. Null with a zero denominator. |
| `opp_rim_rate` | Float64 | Share of the opponent's field-goal attempts at the rim with the unit on the floor. Null with a zero denominator. |
| `opp_mid_rate` | Float64 | Share of the opponent's field-goal attempts that are mid-range twos. Null with a zero denominator. |
| `opp_fg3_rate` | Float64 | Share of the opponent's field-goal attempts that are threes. Null with a zero denominator. |
| `opp_rim_pct` | Float64 | The opponent's field-goal percentage at the rim. Null with a zero denominator. |
| `opp_mid_pct` | Float64 | The opponent's mid-range field-goal percentage with the unit on the floor. Null with a zero denominator. |
| `opp_fg3_pct` | Float64 | The opponent's three-point percentage with the unit on the floor. Null with a zero denominator. |
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
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose off_rtg_n is at least 660 offensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose def_rtg_n is at least 810 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose net_rtg_n is at least 840 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose efg_pct_n is at least 750 field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose tov_pct_n is at least 1,100 offensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose orb_pct_n is at least 220 offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose ft_rate_n is at least 720 field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose opp_efg_pct_n is at least 1,300 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose opp_tov_pct_n is at least 350 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose opp_orb_pct_n is at least 860 opponent offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose opp_ft_rate_n is at least 880 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
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
| `off_rtg_on_rank` | Float64 | Rank of off_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose off_rtg_on_n is at least 610 offensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. An owner-accepted exception (2026-11-09), as on the unseen 2023-2025 seasons the 610-possession gate is 0.27 signal, under a third, measured on halves that include playoff games (0.35 on regular-season games alone); the rank stays published, as no data or source defect was found (rate_gates). |
| `def_rtg_on_rank` | Float64 | Rank of def_rtg_on within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose def_rtg_on_n is at least 1,200 defensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_on_rank` | Float64 | Rank of net_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose net_rtg_on_n is at least 1,600 possessions on the floor (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `off_rtg_diff_rank` | Float64 | Rank of off_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose off_rtg_diff_n is at least 720 offensive possessions on the thinner side (on or off the floor) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. An owner-accepted exception (2026-11-09), as on the unseen 2023-2025 seasons the 720-possession gate is 0.31 signal, under a third, measured on halves that include playoff games (on regular-season games alone its curve never reaches half signal); the rank stays published, as no data or source defect was found (rate_gates). |
| `def_rtg_diff_rank` | Float64 | Rank of def_rtg_diff within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), and never for def_rtg_diff: its reliability never comes near half signal on a season's sample (rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_diff_rank` | Float64 | Rank of net_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 200), whose net_rtg_diff_n is at least 2,600 possessions on the thinner side (offense plus defense, on or off the floor) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
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

### wnba_player_shot_zones {#wnba_player_shot_zones}

`wnba_player_shot_zones(seasons: 'int | list[int]', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3 (league_id='10'). See sportsdataverse.nba.nba_shot_zone_profiles.nba_player_shot_zones.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, player_id)` (stats.nba.com ids, Int64) with `player_name`, `games`, `fga`, `fgm`, `unlocated_fga` and, for each zone `z` (`rim`, `mid`, `corner_3`, `above_break_3`), `{z}_fga`, `{z}_fgm`, `{z}_ast` (assisted makes), `{z}_fg_pct`, `{z}_freq` (share of the located attempts) and `{z}_ast_pct` (share of the makes assisted). The four `{z}_fga` plus `unlocated_fga` add up to `fga`. Every rate has `<m>_n` (its denominator), `<m>_rank` (1 = highest) and `<m>_pct` (the Weibull percentile), ranked within the season among players whose `<m>_n` reaches the rate's gate and null under ten players. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season: the WNBA season is its calendar year. |
| `team_id` | Int64 | stats.wnba.com team id. |
| `player_id` | Int64 | stats.wnba.com player (person) id. |
| `player_name` | String | Player name as the shots release spells it. |
| `games` | Int64 | The player's regular-season games with at least one field-goal attempt for this team. |
| `fga` | Int64 | Field-goal attempts by the player (regular-season games). |
| `fgm` | Int64 | Field goals made by the player. |
| `unlocated_fga` | Int64 | Attempts by the player with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_fgm` | Int64 | Makes by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_ast` | Int64 | Assisted makes by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `mid_fga` | Int64 | Attempts by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_fgm` | Int64 | Makes by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_ast` | Int64 | Assisted makes by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `corner_3_fga` | Int64 | Attempts by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_fgm` | Int64 | Makes by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_ast` | Int64 | Assisted makes by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `above_break_3_fga` | Int64 | Attempts by the player from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the player from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the player from above the break (every other three). |
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
