---
title: "Women's college basketball — additional Python functions — Analytics: wbb_bracketology–wbb_lineup"
sidebar_label: "Analytics: wbb_bracketology–wbb_lineup"
sidebar_position: 11
description: "Women's college basketball — additional Python functions — Analytics: wbb_bracketology–wbb_lineup — function reference in sdv-py, the SportsDataverse Python package."
---
# Women's college basketball — additional Python functions — Analytics: wbb_bracketology–wbb_lineup

### wbb_bracketology {#wbb_bracketology}

`wbb_bracketology(season: 'int', *, as_of_date: 'datetime.date | None' = None, ineligible: 'Iterable[str]' = (), return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's daily `bracketology` table: the projected field, seeds and bubble.

Delegates to `sportsdataverse.mbb.mbb_bracketology.mbb_bracketology` with `league="womens"` (WBB loaders + women's constants).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season to project (e.g. `2024`). |
| `as_of_date` | `date \| None` | `None` | Only use games strictly before this date; `None` uses every completed game. |
| `ineligible` | `Iterable[str]` | `()` | Team ids barred from the NCAA tournament (in 2024, Southern Indiana won the OVC final while reclassifying; UT Martin took the bid). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per Division I team -- see the mbb core for the full contract.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by its end year (2024 is 2023-24). |
| `as_of_date` | Date | The as-of date: only games strictly before it count as played. Null when every completed game is used. |
| `team_id` | String | ESPN team id (Utf8). |
| `team` | String | Team (ESPN location name). |
| `conference` | String | Conference short name. Null for independents, which get no auto-bid. |
| `eligible` | Boolean | False for a team the caller listed as barred from the NCAA tournament (reclassifying to Division I, a postseason ban). It counts in every rating and résumé but gets no bid. |
| `wins` | Int64 | Wins in played games, against every opponent (non-Division I included). |
| `losses` | Int64 | Losses in played games, against every opponent. |
| `net_estimate_rank` | Float64 | NET-estimate rank (strength_of_schedule). |
| `rpi_rank` | Float64 | RPI rank (strength_of_schedule). |
| `wab` | Float64 | Wins above bubble: Division I wins minus a bubble-quality team's expected wins against the same schedule and venues. |
| `quad1_w` | Int64 | Quad 1 wins over Division I opponents (strength_of_schedule). |
| `quad1_l` | Int64 | Quad 1 losses to Division I opponents (strength_of_schedule). |
| `resume_score` | Float64 | Committee-style résumé: 0.35 * adj_em_z + 0.20 * z(sos) + 0.35 * z(wab) + 0.10 * z(quad1_w), over eligible teams. Null for ineligible teams. |
| `auto_bid` | Boolean | Holds the conference's auto-bid: the conference-tournament champion, or before the final the conference leader still alive. |
| `conf_champion` | Boolean | True when the conference-tournament final has been played and this team holds the auto-bid (the runner-up when the champion is ineligible). |
| `at_large_rank` | Int64 | Order among eligible teams without an auto-bid, by resume_score (1 = best). Null for auto-bids and ineligible teams. |
| `bid` | Boolean | In the projected 68-team field. |
| `projected_seed` | Int64 | Projected seed line, 1-16. The last four at-large teams and the four worst auto-bids play the First Four in pairs, so every line has four slots. Null outside the field. |
| `at_large_prob` | Float64 | At-large probability: a logistic in resume_score centred midway between the last at-large team in and the first out. |
| `bubble` | String | last_four_byes (at-large ranks 29-32 with 36 at-large slots), last_four_in (33-36), first_four_out (37-40), next_four_out (41-44); null otherwise. |

**Example**

```python
from sportsdataverse.wbb import wbb_bracketology
field = wbb_bracketology(2024, ineligible={"88"})
```

### wbb_coach_careers {#wbb_coach_careers}

`wbb_coach_careers(seasons: 'int | list[int]', coaches: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college head-coach careers over `seasons`.

Delegates to `sportsdataverse.mbb.mbb_coach_seasons.mbb_coach_careers` with
`league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | The seasons the careers cover. |
| `coaches` | `DataFrame` |  | `~sportsdataverse.mbb.mbb_coach_seasons.ncaa_team_coaches` rows for them. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `coach_id`, most games first; see the mbb core.

| col_name | type | description |
|---|---|---|
| `coach_id` | Int64 | NCAA person id of the head coach (stats.ncaa.org /people/{id}). |
| `coach` | String | Head coach's name (the stats.ncaa.org coach card). |
| `role` | String | HC: head coach (the only role the table carries). |
| `teams` | String | Teams coached, first to last, comma-joined (schools in college, abbreviations in the pros). |
| `seasons` | Int64 | Distinct seasons in the career (only the seasons passed in). |
| `first_season` | Int64 | First season in the career. |
| `last_season` | Int64 | Last season in the career. |
| `games` | Int64 | Games credited to the coach over the seasons passed in. |
| `wins` | Int64 | Wins in the coach's games (final scores). |
| `losses` | Int64 | Losses in the coach's games (final scores). |
| `win_pct` | Float64 | Win fraction: wins divided by games, from the final scores. |
| `pts` | Int64 | Points scored in the coach's games (final scores). |
| `opp_pts` | Int64 | Points allowed in the coach's games (final scores). |
| `pbp_games` | Int64 | The coach's games with published possessions; the ratings and four factors cover these. |
| `poss` | Int64 | Offensive possessions in the coach's games. |
| `opp_poss` | Int64 | Defensive possessions in the coach's games. |
| `poss_pts` | Int64 | Points scored on those possessions (the off_rtg numerator; can differ from pts by a few points a game). |
| `opp_poss_pts` | Int64 | Points allowed on those possessions (the def_rtg numerator). |
| `seconds` | Float64 | Game clock, in seconds, that the coach's possessions used (the pace denominator). |
| `o_fga` | Float64 | The team's field-goal attempts in the coach's games (a four-factor count; null when the source has none). |
| `d_fga` | Float64 | Opponents' field-goal attempts in the coach's games (a four-factor count). |
| `o_fgm` | Float64 | The team's field goals made in the coach's games (a four-factor count; null when the source has none). |
| `d_fgm` | Float64 | Opponents' field goals made in the coach's games (a four-factor count). |
| `o_fg3m` | Float64 | The team's three-pointers made in the coach's games (a four-factor count; null when the source has none). |
| `d_fg3m` | Float64 | Opponents' three-pointers made in the coach's games (a four-factor count). |
| `o_fta` | Float64 | The team's free-throw attempts in the coach's games (a four-factor count; null when the source has none). |
| `d_fta` | Float64 | Opponents' free-throw attempts in the coach's games (a four-factor count). |
| `o_oreb` | Float64 | The team's offensive rebounds in the coach's games (a four-factor count; null when the source has none). |
| `d_oreb` | Float64 | Opponents' offensive rebounds in the coach's games (a four-factor count). |
| `o_dreb` | Float64 | The team's defensive rebounds in the coach's games (a four-factor count; null when the source has none). |
| `d_dreb` | Float64 | Opponents' defensive rebounds in the coach's games (a four-factor count). |
| `o_tov` | Float64 | The team's turnovers in the coach's games (a four-factor count; null when the source has none). |
| `d_tov` | Float64 | Opponents' turnovers in the coach's games (a four-factor count). |
| `o_ff_poss` | Float64 | The team's possessions counted for the turnover rate (the box possessions in college) in the coach's games (a four-factor count; null when the source has none). |
| `d_ff_poss` | Float64 | Opponents' possessions counted for the turnover rate (the box possessions in college) in the coach's games (a four-factor count). |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions, over the coach's games. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions, over the coach's games. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 40 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `tov_pct` | Float64 | Turnovers per possession, as a fraction. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, as a fraction. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction. |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. |
| `win_pct_rank` | Float64 | Rank of win_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `off_rtg_rank` | Float64 | Rank of off_rtg among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `def_rtg_rank` | Float64 | Rank of def_rtg among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `net_rtg_rank` | Float64 | Rank of net_rtg among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `pace_rank` | Float64 | Rank of pace among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `efg_pct_rank` | Float64 | Rank of efg_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `tov_pct_rank` | Float64 | Rank of tov_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `orb_pct_rank` | Float64 | Rank of orb_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `ft_rate_rank` | Float64 | Rank of ft_rate among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `win_pct_pct` | Float64 | Percentile of win_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `pace_pct` | Float64 | Percentile of pace among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `win_pct_n` | Int64 | Sample size of win_pct: games. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: offensive possessions. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: defensive possessions. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: offensive plus defensive possessions. |
| `pace_n` | Int64 | Sample size of pace: the games with possessions (pbp_games). |
| `efg_pct_n` | Int64 | Sample size of efg_pct: field-goal attempts. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: turnover-rate possessions. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: offensive-rebound chances. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: field-goal attempts. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: opponents' field-goal attempts. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: opponents' turnover-rate possessions. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: opponents' offensive-rebound chances. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: opponents' field-goal attempts. |

**Example**

```python
from sportsdataverse.wbb import wbb_coach_careers
wbb_coach_careers([2024, 2025], coaches).head()
```

### wbb_coach_seasons {#wbb_coach_seasons}

`wbb_coach_seasons(seasons: 'int | list[int]', coaches: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college head-coach seasons: W-L, ratings and four factors of each coach's team-seasons.

Delegates to `sportsdataverse.mbb.mbb_coach_seasons.mbb_coach_seasons` with
`league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `coaches` | `DataFrame` |  | `~sportsdataverse.mbb.mbb_coach_seasons.ncaa_team_coaches` rows (`league="womens"`) for those seasons and the season before each. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, coach_id)`; see the mbb core for the columns.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26; the WNBA season is its calendar year). |
| `team_id` | Int64 | ESPN team id of the school (ESPN's schedule and the crosswalk of the NCAA team-season id). |
| `team` | String | School (ESPN location). |
| `coach_id` | Int64 | NCAA person id of the head coach (stats.ncaa.org /people/{id}). |
| `coach` | String | Head coach's name (the stats.ncaa.org coach card). |
| `role` | String | HC: head coach (the only role the table carries). |
| `team_games` | Int64 | The team's completed games in the season (and season type); games < team_games means the coach shared the season. |
| `first_game_date` | Date | Date of the coach's first credited game that season. |
| `last_game_date` | Date | Date of the coach's last credited game that season. |
| `games` | Int64 | Games credited to the coach: each completed game goes to the coach of record for it. |
| `wins` | Int64 | Wins in the coach's games (final scores). |
| `losses` | Int64 | Losses in the coach's games (final scores). |
| `win_pct` | Float64 | Win fraction: wins divided by games, from the final scores. |
| `pts` | Int64 | Points scored in the coach's games (final scores). |
| `opp_pts` | Int64 | Points allowed in the coach's games (final scores). |
| `pbp_games` | Int64 | The coach's games with published possessions; the ratings and four factors cover these. |
| `poss` | Int64 | Offensive possessions in the coach's games. |
| `opp_poss` | Int64 | Defensive possessions in the coach's games. |
| `poss_pts` | Int64 | Points scored on those possessions (the off_rtg numerator; can differ from pts by a few points a game). |
| `opp_poss_pts` | Int64 | Points allowed on those possessions (the def_rtg numerator). |
| `seconds` | Float64 | Game clock, in seconds, that the coach's possessions used (the pace denominator). |
| `o_fga` | Float64 | The team's field-goal attempts in the coach's games (a four-factor count; null when the source has none). |
| `d_fga` | Float64 | Opponents' field-goal attempts in the coach's games (a four-factor count). |
| `o_fgm` | Float64 | The team's field goals made in the coach's games (a four-factor count; null when the source has none). |
| `d_fgm` | Float64 | Opponents' field goals made in the coach's games (a four-factor count). |
| `o_fg3m` | Float64 | The team's three-pointers made in the coach's games (a four-factor count; null when the source has none). |
| `d_fg3m` | Float64 | Opponents' three-pointers made in the coach's games (a four-factor count). |
| `o_fta` | Float64 | The team's free-throw attempts in the coach's games (a four-factor count; null when the source has none). |
| `d_fta` | Float64 | Opponents' free-throw attempts in the coach's games (a four-factor count). |
| `o_oreb` | Float64 | The team's offensive rebounds in the coach's games (a four-factor count; null when the source has none). |
| `d_oreb` | Float64 | Opponents' offensive rebounds in the coach's games (a four-factor count). |
| `o_dreb` | Float64 | The team's defensive rebounds in the coach's games (a four-factor count; null when the source has none). |
| `d_dreb` | Float64 | Opponents' defensive rebounds in the coach's games (a four-factor count). |
| `o_tov` | Float64 | The team's turnovers in the coach's games (a four-factor count; null when the source has none). |
| `d_tov` | Float64 | Opponents' turnovers in the coach's games (a four-factor count). |
| `o_ff_poss` | Float64 | The team's possessions counted for the turnover rate (the box possessions in college) in the coach's games (a four-factor count; null when the source has none). |
| `d_ff_poss` | Float64 | Opponents' possessions counted for the turnover rate (the box possessions in college) in the coach's games (a four-factor count). |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions, over the coach's games. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions, over the coach's games. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 40 minutes, the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `tov_pct` | Float64 | Turnovers per possession, as a fraction. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, as a fraction. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction. |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. |
| `win_pct_rank` | Float64 | Rank of win_pct within the season among coaches with at least half the most games any team played (null under five coaches) (the highest value ranks 1; ties average). |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season among coaches with at least half the most games any team played (null under five coaches) (the highest value ranks 1; ties average). |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season among coaches with at least half the most games any team played (null under five coaches) (the lowest value ranks 1; ties average). |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season among coaches with at least half the most games any team played (null under five coaches) (the highest value ranks 1; ties average). |
| `pace_rank` | Float64 | Rank of pace within the season among coaches with at least half the most games any team played (null under five coaches) (the highest value ranks 1; ties average). |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season among coaches with at least half the most games any team played (null under five coaches) (the highest value ranks 1; ties average). |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season among coaches with at least half the most games any team played (null under five coaches) (the lowest value ranks 1; ties average). |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season among coaches with at least half the most games any team played (null under five coaches) (the highest value ranks 1; ties average). |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season among coaches with at least half the most games any team played (null under five coaches) (the highest value ranks 1; ties average). |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season among coaches with at least half the most games any team played (null under five coaches) (the lowest value ranks 1; ties average). |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season among coaches with at least half the most games any team played (null under five coaches) (the highest value ranks 1; ties average). |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season among coaches with at least half the most games any team played (null under five coaches) (the lowest value ranks 1; ties average). |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season among coaches with at least half the most games any team played (null under five coaches) (the lowest value ranks 1; ties average). |
| `win_pct_pct` | Float64 | Percentile of win_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `pace_pct` | Float64 | Percentile of pace among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `win_pct_n` | Int64 | Sample size of win_pct: games. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: offensive possessions. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: defensive possessions. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: offensive plus defensive possessions. |
| `pace_n` | Int64 | Sample size of pace: the games with possessions (pbp_games). |
| `efg_pct_n` | Int64 | Sample size of efg_pct: field-goal attempts. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: turnover-rate possessions. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: offensive-rebound chances. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: field-goal attempts. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: opponents' field-goal attempts. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: opponents' turnover-rate possessions. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: opponents' offensive-rebound chances. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: opponents' field-goal attempts. |

**Example**

```python
from sportsdataverse.wbb import wbb_coach_seasons
wbb_coach_seasons(2025, coaches).sort("net_rtg_rank").head()
```

### wbb_epv_lineup_games {#wbb_epv_lineup_games}

`wbb_epv_lineup_games(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per women's college five-woman unit and game: the EPV added with the unit on the floor.

Delegates to `sportsdataverse.mbb.mbb_epv.mbb_epv_lineup_games` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

The columns of `sportsdataverse.mbb.mbb_epv.mbb_epv_lineup_games`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id of the unit's team. |
| `lineup_id` | String | The five stats.ncaa.org player ids, sorted and joined with '-'. |
| `off_epv` | Float64 | EPV added by the unit's team with the ball while the unit was on. |
| `def_epv` | Float64 | EPV added by the opponent with the ball while the unit was on. |
| `net_epv` | Float64 | off_epv - def_epv. |
| `plays` | Int64 | Play-by-play events with the unit on the floor (the ten on the event's row; the possession's starting ten where the row's is incomplete). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a season is before the first one served. |

**Example**

```python
from sportsdataverse.wbb import wbb_epv_lineup_games
wbb_epv_lineup_games(2026).sort("net_epv", descending=True).head()
```

### wbb_epv_player_games {#wbb_epv_player_games}

`wbb_epv_player_games(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per women's college player and game: EPV added on her own actions and on-court EPV +/-.

Delegates to `sportsdataverse.mbb.mbb_epv.mbb_epv_player_games` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

The columns of `sportsdataverse.mbb.mbb_epv.mbb_epv_player_games`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id of the player's team. |
| `player_id` | String | stats.ncaa.org player id. |
| `epv_own` | Float64 | EPV credited to the player's own events: as the offensive actor the play's epv_added, as a defensive actor its negative, a steal or block the negative of the turnover or miss it ended. |
| `own_actions` | Int64 | How many of the player's own events were credited. |
| `off_epv_on` | Float64 | EPV added by the player's team with the ball while the player was on the floor (the ten on each event's row; the possession's starting ten where the row's is incomplete). |
| `def_epv_on` | Float64 | EPV added by the opponent with the ball while the player was on the floor. |
| `epv_on` | Float64 | On-court EPV +/-: off_epv_on - def_epv_on. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a season is before the first one served. |

**Example**

```python
from sportsdataverse.wbb import wbb_epv_player_games
wbb_epv_player_games(2026).sort("epv_on", descending=True).head()
```

### wbb_epv_plays {#wbb_epv_plays}

`wbb_epv_plays(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

EPV and EPV added on every women's college play-by-play event inside a possession.

Delegates to `sportsdataverse.mbb.mbb_epv.mbb_epv_plays` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

The columns of `sportsdataverse.mbb.mbb_epv.mbb_epv_plays`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.ncaa.org contest id. |
| `possession_number` | Int64 | The possession's number in the game, from 1 (the release's, split where the other side shoots). |
| `step` | Int64 | The event's place in its possession, from 1. |
| `order_index` | Int64 | The row's order in the game's release play-by-play, from 0. |
| `action_number` | Int64 | The row's order in the game's release play-by-play (the key of its ten on the floor). |
| `offense_team_id` | Int64 | ESPN team id of the offense (null for a non-D1 team). |
| `defense_team_id` | Int64 | ESPN team id of the defense (null for a non-D1 team). |
| `home_team_id` | Int64 | ESPN team id of the home team (null for a non-D1 team). |
| `team_id` | Int64 | ESPN team id of the team on the event (null for a non-D1 team). |
| `person_id` | String | stats.ncaa.org player id on the event; null on a team event. |
| `event_type` | String | The event in the NBA engine's vocabulary (made_shot, missed_shot, rebound, foul, free_throw, turnover, timeout, jump_ball, substitution, other). |
| `sub_type` | String | The free-throw trip (Free Throw n of m) or the foul kind (Shooting, Offensive, Technical, Personal). |
| `description` | String | The play-by-play text of the event (a steal or block is prefixed STEAL / BLOCK). |
| `off_player_1` | String | stats.ncaa.org player id of offensive player 1 of 5 when the possession started. |
| `off_player_2` | String | stats.ncaa.org player id of offensive player 2 of 5 when the possession started. |
| `off_player_3` | String | stats.ncaa.org player id of offensive player 3 of 5 when the possession started. |
| `off_player_4` | String | stats.ncaa.org player id of offensive player 4 of 5 when the possession started. |
| `off_player_5` | String | stats.ncaa.org player id of offensive player 5 of 5 when the possession started. |
| `def_player_1` | String | stats.ncaa.org player id of defensive player 1 of 5 when the possession started. |
| `def_player_2` | String | stats.ncaa.org player id of defensive player 2 of 5 when the possession started. |
| `def_player_3` | String | stats.ncaa.org player id of defensive player 3 of 5 when the possession started. |
| `def_player_4` | String | stats.ncaa.org player id of defensive player 4 of 5 when the possession started. |
| `def_player_5` | String | stats.ncaa.org player id of defensive player 5 of 5 when the possession started. |
| `start_type` | Int64 | State: how the possession started, 0 dead ball, 1 timeout, 2 made shot, 3 missed shot, 4 live-ball turnover. |
| `period` | Int64 | State: the period (halves for men, quarters for women), overtimes folded to 5. |
| `seconds_remaining` | Float64 | State: seconds left in the period after the event. |
| `start_margin` | Float64 | State: the offense's lead when the possession started (before its first event). |
| `transition` | Int64 | State: 0 half court, 1 transition window open (live start, no shot or dead ball yet, within 10 s), 2 a transition possession after its first shot. |
| `last_event` | Int64 | State: the event just processed, coded by its index in nba_epv.LAST_EVENTS (11 the start). |
| `ft_pending` | Int64 | State: free throws still to come in the trip (from the release's ft_number of ft_attempts). |
| `shot_zone` | Int64 | State: zone of the possession's latest shot, 0 none, 1 paint two, 3 other two, 4 three (2 is unused: the feed has no distance). |
| `points` | Int64 | The offense's points on the event. |
| `remaining_points` | Int64 | The offense's points after the event to the end of the possession (the model's label). |
| `is_last` | Boolean | Whether the event is its possession's last. |
| `next_in_period` | Boolean | Whether another possession follows in the same period. |
| `epv` | Float64 | Expected points for the offense to the end of the possession, after the event (scaled by the game's as-of scoring level). |
| `epv_start` | Float64 | EPV at the possession's start. |
| `epv_before` | Float64 | EPV before the event (the previous event's, or the start's). |
| `epv_after` | Float64 | EPV after the event; when it ends the possession, minus the opponent's next start over an average start (0 at a period's end). |
| `epv_added` | Float64 | EPV added: points + epv_after - epv_before, the offense's (the defense's is its negative). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a season is before the first one served. |

**Example**

```python
from sportsdataverse.wbb import wbb_epv_plays
wbb_epv_plays(2026).sort("epv_added", descending=True).head()
```

### wbb_epv_team_games {#wbb_epv_team_games}

`wbb_epv_team_games(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per women's college team and game: EPV added on offense and allowed on defense.

Delegates to `sportsdataverse.mbb.mbb_epv.mbb_epv_team_games` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

The columns of `sportsdataverse.mbb.mbb_epv.mbb_epv_team_games`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id of the team (a non-D1 team gets no row). |
| `opp_team_id` | Int64 | The opponent's ESPN team id (null for a non-D1 opponent). |
| `off_epv` | Float64 | The team's EPV added with the ball over the game. |
| `poss` | Int64 | The team's possessions with at least one event. |
| `def_epv` | Float64 | The opponent's EPV added with the ball over the game. |
| `opp_poss` | Int64 | The opponent's possessions with at least one event. |
| `net_epv` | Float64 | off_epv - def_epv. |
| `off_epv_per_poss` | Float64 | off_epv per possession. |
| `def_epv_per_poss` | Float64 | def_epv per possession. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a season is before the first one served. |

**Example**

```python
import polars as pl
from sportsdataverse.wbb import wbb_epv_team_games
wbb_epv_team_games(2026).group_by("team_id").agg(pl.col("off_epv_per_poss").mean())
```

### wbb_game_flow {#wbb_game_flow}

`wbb_game_flow(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college game flow per team and game: runs, droughts, kills, lead changes, time leading, game control.

Delegates to `sportsdataverse.mbb.mbb_game_flow.mbb_game_flow` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id)` with the columns of `sportsdataverse.nba.nba_game_flow.nba_game_flow`; `game_id` is the NCAA contest id, `team_id` the ESPN team id. See `sportsdataverse._common.game_flow` for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 opponent has none and gets no row. |
| `opp_team_id` | Int64 | The opponent's team id. |
| `is_home` | Boolean | True for the home side. |
| `pts` | Int64 | The team's final points: its running score after the last play. |
| `opp_pts` | Int64 | The opponent's final points. |
| `won` | Boolean | True when pts > opp_pts. |
| `game_seconds` | Float64 | Game length in seconds: regulation plus the overtimes played. |
| `lead_sec` | Float64 | Game-clock seconds with the team ahead; the score after a play holds until the next play. |
| `trail_sec` | Float64 | Game-clock seconds with the team behind. |
| `tied_sec` | Float64 | Game-clock seconds with the score level, the clock before the first play included. |
| `lead_pct` | Float64 | lead_sec / game_seconds. |
| `lead_changes` | Int64 | How often the leader flipped (a tie is not a lead, and the first lead is not a change); the same on both rows of a game. |
| `times_tied` | Int64 | How often the score became level, the 0-0 start not counted; the same on both rows of a game. |
| `largest_lead` | Int64 | The team's biggest lead in points (0 if never ahead). |
| `largest_deficit` | Int64 | The team's biggest deficit in points (0 if never behind). |
| `comeback` | Int64 | largest_deficit for a game the team won, else 0. |
| `max_run` | Int64 | The most points the team scored in a row with the opponent scoring none (an n-0 run); a score correction comes off the run of the side it lowers. 0 if the team never scored. |
| `runs_10` | Int64 | The team's runs of 10 or more unanswered points. |
| `opp_runs_10` | Int64 | The opponent's runs of 10 or more unanswered points (a non-D1 opponent's included). |
| `longest_drought_sec` | Float64 | The longest stretch of game clock between two of the team's scores, counted from the tip to its first score and from its last score to the end. |
| `avg_margin` | Float64 | Game control: the team's score margin averaged over the game clock. |
| `avg_wp` | Float64 | Game control: the team's in-game win probability averaged over the game clock (the league's in-game model with a home-court-only prior, 0.5 at a neutral site). |
| `def_poss` | Int64 | The team's defensive possessions (every row of the possessions release); null for a game with no published possessions. |
| `stops` | Int64 | Defensive possessions on which the opponent scored no points. |
| `kills` | Int64 | Kills: each run of n straight stops adds n // 3 (a kill is three straight stops; a run of stops goes on across period breaks). |
| `double_kills` | Int64 | Double kills: each run of n straight stops adds n // 6 (six straight stops are two kills and one double kill). |

**Example**

```python
from sportsdataverse.wbb import wbb_game_flow
wbb_game_flow(2026).sort("max_run").tail()
```

### wbb_game_lineups_used {#wbb_game_lineups_used}

`wbb_game_lineups_used(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college lineups used: per five-man unit and game, minutes, possessions, +/- and ratings.

Delegates to `sportsdataverse.mbb.mbb_game_impact.mbb_game_lineups_used` with
`league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, lineup_id)`: `player_1..5` and the same totals and ratings as `wbb_game_player_impact`. See the mbb core for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `opp_team_id` | Int64 | The opponent's team id. |
| `lineup_id` | String | The five player ids of the unit, sorted and joined with '-'. |
| `player_1` | String | Player 1 of the unit, in lineup_id order. |
| `player_2` | String | Player 2 of the unit, in lineup_id order. |
| `player_3` | String | Player 3 of the unit, in lineup_id order. |
| `player_4` | String | Player 4 of the unit, in lineup_id order. |
| `player_5` | String | Player 5 of the unit, in lineup_id order. |
| `minutes` | Float64 | Game-clock minutes on the floor (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds on the floor: the clock that ran into each action, credited to the lineup at that action. |
| `poss` | Int64 | Offensive possessions on the floor (a possession counts for the five on the floor when it starts). |
| `opp_poss` | Int64 | Defensive possessions on the floor. |
| `pts` | Int64 | The team's points while on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul, the official scorer's rule). |
| `opp_pts` | Int64 | The opponent's points while on the floor, credited as pts is. |
| `plus_minus` | Int64 | On-court +/-: pts - opp_pts. |
| `off_rtg` | Float64 | On-court offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | On-court defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | On-court net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Example**

```python
from sportsdataverse.wbb import wbb_game_lineups_used
wbb_game_lineups_used(2025).head()
```

### wbb_game_player_impact {#wbb_game_player_impact}

`wbb_game_player_impact(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's college game impact box: per player and game, minutes, possessions, +/- and ratings.

Delegates to `sportsdataverse.mbb.mbb_game_impact.mbb_game_player_impact` with
`league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, player_id)`: `opp_team_id`, `minutes`, `seconds`, `poss`, `opp_poss`, `pts`, `opp_pts`, `plus_minus`, `off_rtg`, `def_rtg`, `net_rtg` and their `_n`. See the mbb core for the definitions.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `opp_team_id` | Int64 | The opponent's team id. |
| `player_id` | String | stats.ncaa.org player id (there is no NCAA-to-ESPN player crosswalk yet). |
| `minutes` | Float64 | Game-clock minutes on the floor (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds on the floor: the clock that ran into each action, credited to the lineup at that action. |
| `poss` | Int64 | Offensive possessions on the floor (a possession counts for the five on the floor when it starts). |
| `opp_poss` | Int64 | Defensive possessions on the floor. |
| `pts` | Int64 | The team's points while on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul, the official scorer's rule). |
| `opp_pts` | Int64 | The opponent's points while on the floor, credited as pts is. |
| `plus_minus` | Int64 | On-court +/-: pts - opp_pts. |
| `off_rtg` | Float64 | On-court offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | On-court defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | On-court net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Example**

```python
from sportsdataverse.wbb import wbb_game_player_impact
wbb_game_player_impact(2025).sort("plus_minus").tail()
```

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
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `rim_rate_rank` | Float64 | Rank of rim_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `mid_rate_rank` | Float64 | Rank of mid_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `fg3_rate_rank` | Float64 | Rank of fg3_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `rim_pct_rank` | Float64 | Rank of rim_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `mid_pct_rank` | Float64 | Rank of mid_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `fg3_pct_rank` | Float64 | Rank of fg3_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_rim_rate_rank` | Float64 | Rank of opp_rim_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_mid_rate_rank` | Float64 | Rank of opp_mid_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_fg3_rate_rank` | Float64 | Rank of opp_fg3_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_rim_pct_rank` | Float64 | Rank of opp_rim_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_mid_pct_rank` | Float64 | Rank of opp_mid_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_fg3_pct_rank` | Float64 | Rank of opp_fg3_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 200); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
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

**Example**

```python
from sportsdataverse.wbb import wbb_lineup_seasons
wbb_lineup_seasons(2025).filter(n_players=5).sort("poss").tail()
```
