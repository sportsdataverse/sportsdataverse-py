---
title: "Men's college basketball — additional Python functions — Analytics: mbb_lineup–ncaa_lineup"
sidebar_label: "Analytics: mbb_lineup–ncaa_lineup"
sidebar_position: 14
description: "Men's college basketball — additional Python functions — Analytics: mbb_lineup–ncaa_lineup — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Analytics: mbb_lineup–ncaa_lineup

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

One row per `(season, team_id, n_players, lineup_id)`: `n_players` (2, 3 or 5), `lineup_id` (the NCAA player ids sorted and joined with `-`), `player_1..5`, `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions), the four factors for and against (`efg_pct`, `tov_pct`, `orb_pct`, `ft_rate`, `opp_*`), the shot zones for and against (`rim_rate`, `mid_rate`, `fg3_rate` shares of field-goal attempts and `rim_pct`, `mid_pct`, `fg3_pct` make rates, `opp_*`), and for every rate `<m>_rank` / `<m>_pct` (within the season and unit size, among units over `min_poss`) and `<m>_n`. `team_id` is the ESPN team id (Int64). Empty input returns that schema with zero rows.

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
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Null with a zero denominator. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Null with a zero denominator. |
| `orb_pct` | Float64 | Offensive-rebound rate: OREB / (OREB + the opponent's DREB), as a fraction. Null with a zero denominator. |
| `ft_rate` | Float64 | Free-throw rate: FTA / FGA. Null with a zero denominator. |
| `opp_efg_pct` | Float64 | The opponent's effective field-goal percentage with the unit on the floor. Null with a zero denominator. |
| `opp_tov_pct` | Float64 | The opponent's turnovers per defensive possession. Null with a zero denominator. |
| `opp_orb_pct` | Float64 | The opponent's offensive-rebound rate: its OREB / (its OREB + the unit's DREB). Null with a zero denominator. |
| `opp_ft_rate` | Float64 | The opponent's free-throw rate: FTA / FGA. Null with a zero denominator. |
| `rim_rate` | Float64 | Share of the unit's field-goal attempts at the rim (dunk, layup, hook, tip-in). Null with a zero denominator. |
| `mid_rate` | Float64 | Share of the unit's field-goal attempts that are other twos (mid-range). Null with a zero denominator. |
| `fg3_rate` | Float64 | Share of the unit's field-goal attempts that are threes. Null with a zero denominator. |
| `rim_pct` | Float64 | Field-goal percentage at the rim, as a fraction. Null with a zero denominator. |
| `mid_pct` | Float64 | Mid-range field-goal percentage, as a fraction. Null with a zero denominator. |
| `fg3_pct` | Float64 | Three-point percentage, as a fraction. Null with a zero denominator. |
| `opp_rim_rate` | Float64 | Share of the opponent's field-goal attempts at the rim with the unit on the floor. Null with a zero denominator. |
| `opp_mid_rate` | Float64 | Share of the opponent's field-goal attempts that are mid-range twos. Null with a zero denominator. |
| `opp_fg3_rate` | Float64 | Share of the opponent's field-goal attempts that are threes. Null with a zero denominator. |
| `opp_rim_pct` | Float64 | The opponent's field-goal percentage at the rim. Null with a zero denominator. |
| `opp_mid_pct` | Float64 | The opponent's mid-range field-goal percentage. Null with a zero denominator. |
| `opp_fg3_pct` | Float64 | The opponent's three-point percentage. Null with a zero denominator. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `rim_rate_rank` | Float64 | Rank of rim_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `mid_rate_rank` | Float64 | Rank of mid_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `fg3_rate_rank` | Float64 | Rank of fg3_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `rim_pct_rank` | Float64 | Rank of rim_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `mid_pct_rank` | Float64 | Rank of mid_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `fg3_pct_rank` | Float64 | Rank of fg3_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_rim_rate_rank` | Float64 | Rank of opp_rim_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_mid_rate_rank` | Float64 | Rank of opp_mid_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_fg3_rate_rank` | Float64 | Rank of opp_fg3_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_rim_pct_rank` | Float64 | Rank of opp_rim_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_mid_pct_rank` | Float64 | Rank of opp_mid_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
| `opp_fg3_pct_rank` | Float64 | Rank of opp_fg3_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least 200 possessions (poss + opp_poss); null for a non-qualifier, a null rate, or fewer than ten qualifiers. |
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

### mbb_strength_of_schedule {#mbb_strength_of_schedule}

`mbb_strength_of_schedule(seasons: 'list[int]', *, as_of_date: 'datetime.date | None' = None, league: 'str' = 'mens', ineligible: 'Iterable[str]' = (), return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

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
| `ineligible` | `Iterable[str]` | `()` | Team ids barred from the NCAA tournament (sets `eligible`). |
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

### mbb_team_luck {#mbb_team_luck}

`mbb_team_luck(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College luck and close-game record: Pythagorean expected wins vs actual wins, and close-game W-L.

One row per team and season. `exp_wins` is the Pythagorean expectation on the season's
final scores, `games * pts^x / (pts^x + opp_pts^x)`, with the exponent `x` fitted per
league on past seasons (`~sportsdataverse._common.team_luck.PYTHAG_EXPONENT`).
`luck_wins` is `wins - exp_wins`. A close game is one within 5 points at the start of a
possession in the last 5:00 of the 2nd half (4th quarter, women's, from 2016) or overtime.

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
| `close_games` | Int64 | Close games: at least one possession started with 5:00 or less left in the 2nd half or an overtime, the score within 5 (stats.nba.com's clutch window; BB-D3's close split). The margin is the running sum of the possessions' points, so it can drift from the scoreboard: NBA 2025-26 agrees with an independent ESPN play-by-play recount on 1210 of 1229 games. |
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

### mbb_team_season_rates {#mbb_team_season_rates}

`mbb_team_season_rates(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College season team rates from possessions: ORtg/DRtg/net, tempo and the four factors, with splits.

One row per team, season and split (`all`, `home`, `away`, `close`, `clutch`).
Clutch is a possession that starts with 5:00 or less left in the 2nd half (the 4th quarter
in women's basketball from 2016) or an overtime, the score within 5; a close game is one with
a clutch possession. The full definitions are in `sportsdataverse._common.team_season_rates`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_team_season_rates.wbb_team_season_rates`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, split)`. `team_id` is the ESPN team id (Int64). `games`, `poss` / `opp_poss` (offensive / defensive possessions) and `pts` / `opp_pts` are the split's totals. The twelve rates are `off_rtg`, `def_rtg`, `net_rtg` (per 100 possessions), `pace` (possessions per 40 minutes), the four factors `efg_pct`, `tov_pct`, `orb_pct`, `ft_rate` and the same four against (`opp_*`), all fractions. The four factors are game-level (the NCAA team box) and null in the `clutch` split. Each rate has `<m>_rank` (1 = best; `def_rtg`, `tov_pct`, `opp_efg_pct`, `opp_orb_pct` and `opp_ft_rate` rank low first; pace ranks high first), `<m>_pct` (the Weibull percentile) and `<m>_n` (the sample: possessions for the ratings, games for pace, field-goal attempts for eFG% and FT rate, the box possessions for turnover rate, rebound chances for ORB%). Ranks are within the season and split, among teams with at least half the most games (which leaves out the non-D1 opponents), and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `team_id` | Int64 | ESPN team id. Non-D1 opponents have none and get no row. |
| `split` | String | Which possessions the row covers: all; home or away (the team's home or road games; neutral-site games are in neither); close (every possession of a game that had a clutch possession); clutch (a possession that starts with 5:00 or less left in the 2nd half or an overtime, the score within 5 as it starts). |
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
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season and split (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and split (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and split (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the split; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
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
| `pace_n` | Int64 | Sample size of pace: games in the split. |
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

One row per team and season. The adjusted ratings are the published college ratings
(`load_mbb_ratings` / `load_wbb_ratings`). SOS is the mean opponent `adj_net_rtg` over
the team's completed ESPN games (unrated opponents left out), SOV the same over its wins.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_team_strength.wbb_team_strength`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`; `team_id` is the ESPN team id (Int64). `games`, `poss` / `opp_poss`, `pts` / `opp_pts` are the season totals from the NCAA possessions; `wins` / `losses` come from ESPN's completed games. The rated metrics are `adj_off_rtg`, `adj_def_rtg`, `adj_net_rtg`, `sos`, `sov` and the BB-D3 rates (`off_rtg`, `def_rtg`, `net_rtg`, `pace`, the four factors and the four against). Each has `<m>_rank` (1 = best; `adj_def_rtg`, `def_rtg`, `tov_pct` and the opponent shooting, rebounding and free-throw rates rank low first; `sos` ranks the hardest schedule first), `<m>_pct` (the Weibull percentile) and `<m>_n` (possessions for the ratings, rated games for SOS, games for pace, wins over rated opponents for SOV). Ranks are within the season, among teams with at least half the most games (which leaves out the non-D1 opponents), and null under five teams. Empty input returns that schema with zero rows.

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
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `pace_rank` | Float64 | Rank of pace within the season (the highest value ranks 1; pace has no better direction; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season (the lowest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null rate, or fewer than five qualifiers. |
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
| `pace_n` | Int64 | Sample size of pace: games in the season. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: the team's field-goal attempts in the season, 0 when the rate is null. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: the NCAA team box's possessions (o_poss) in the season, 0 when the rate is null. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: the team's offensive-rebound chances in the season, 0 when the rate is null. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: the team's field-goal attempts in the season, 0 when the rate is null. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: the opponents' field-goal attempts in the season, 0 when the rate is null. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: the opponents' NCAA team-box possessions in the season, 0 when the rate is null. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: the opponents' offensive-rebound chances in the season, 0 when the rate is null. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: the opponents' field-goal attempts in the season, 0 when the rate is null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_team_strength import mbb_team_strength
strength = mbb_team_strength(2025)
strength.sort("adj_net_rtg_rank").select("team_id", "adj_net_rtg", "sos", "sov").head()
```

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
