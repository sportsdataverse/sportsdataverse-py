---
title: "Men's college basketball — additional Python functions — Analytics: mbb_coach–ncaa_impact"
sidebar_label: "Analytics: mbb_coach–ncaa_impact"
sidebar_position: 14
description: "Men's college basketball — additional Python functions — Analytics: mbb_coach–ncaa_impact — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Analytics: mbb_coach–ncaa_impact

### mbb_coach_seasons {#mbb_coach_seasons}

`mbb_coach_seasons(seasons: 'int | list[int]', coaches: 'pl.DataFrame', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College head-coach seasons: W-L, ratings and four factors of the team-seasons each coached.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). NCAA possessions start in 2010; earlier seasons get null ratings. |
| `coaches` | `DataFrame` |  | `ncaa_team_coaches` rows for those seasons and the season before each (the card record check, `ncaa_coach_stints`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_coach_seasons.wbb_coach_seasons`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, coach_id)`: `coach_id` is the NCAA person id, `team_id` the ESPN team id, `team` the school. The columns are those of `sportsdataverse.nba.nba_coach_seasons.nba_coach_seasons`; `pace` is per 40 minutes. Ranks are within the season among coaches with half the most games (which leaves out the non-D1 opponents, who have no coach). Empty input returns that schema with zero rows.

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
| `pace_n` | Int64 | Sample size of pace: games. |
| `efg_pct_n` | Int64 | Sample size of efg_pct: field-goal attempts. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: turnover-rate possessions. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: offensive-rebound chances. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: field-goal attempts. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: opponents' field-goal attempts. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: opponents' turnover-rate possessions. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: opponents' offensive-rebound chances. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: opponents' field-goal attempts. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_coach_seasons import mbb_coach_seasons, ncaa_team_coaches
coaches = ncaa_team_coaches("ncaa-mbb-hoops-raw", 2025)
mbb_coach_seasons(2025, coaches).sort("net_rtg_rank").select("coach", "team", "wins", "net_rtg").head()
```

### mbb_game_lineups_used {#mbb_game_lineups_used}

`mbb_game_lineups_used(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The college lineups used: per five-man unit and game, minutes, possessions, +/- and ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_game_impact.wbb_game_lineups_used`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, lineup_id)`: `lineup_id` (the five NCAA player ids sorted and joined with `-`), `player_1..5`, and the same totals and ratings as `mbb_game_player_impact`, sorted by minutes within each team and game. Empty input returns that schema with zero rows.

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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_game_impact import mbb_game_lineups_used
units = mbb_game_lineups_used(2025)
```

### mbb_game_player_impact {#mbb_game_player_impact}

`mbb_game_player_impact(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The college game impact box: per player and game, minutes, on-court possessions, +/- and ratings.

The definitions are in `sportsdataverse._common.game_impact`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_game_impact.wbb_game_player_impact`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, player_id)`: `opp_team_id`, `minutes` and `seconds` on the floor, `poss` / `opp_poss` (offensive / defensive possessions on the floor), `pts` / `opp_pts` (points scored / allowed while on), `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions) and their `_n` sample sizes. `game_id` is the NCAA contest id, `team_id` the ESPN team id (Int64), `player_id` the NCAA player id (String). Empty input returns that schema with zero rows.

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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_game_impact import mbb_game_player_impact
box = mbb_game_player_impact(2025)
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
