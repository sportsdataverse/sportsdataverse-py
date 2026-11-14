---
title: "Men's college basketball — additional Python functions — Analytics: mbb_coach–mbb_game"
sidebar_label: "Analytics: mbb_coach–mbb_game"
sidebar_position: 14
description: "Men's college basketball — additional Python functions — Analytics: mbb_coach–mbb_game — function reference in sdv-py, the SportsDataverse Python package."
---
# Men's college basketball — additional Python functions — Analytics: mbb_coach–mbb_game

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
| `pace_n` | Int64 | Sample size of pace: the games with possessions (pbp_games). |
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
coaches = ncaa_team_coaches("ncaa-mbb-hoops-raw", [2024, 2025])
mbb_coach_seasons(2025, coaches).sort("net_rtg_rank").select("coach", "team", "wins", "net_rtg").head()
```

### mbb_epv_lineup_games {#mbb_epv_lineup_games}

`mbb_epv_lineup_games(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per college five-man unit and game: the EPV added with the unit on the floor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_epv.wbb_epv_lineup_games`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

`sportsdataverse.nba.nba_epv.epv_lineup_games` of the seasons' plays: one row per `(season, game_id, team_id, lineup_id)` with `off_epv`, `def_epv`, `net_epv` and `plays`. `lineup_id` joins the five stats.ncaa.org player ids with `-`; a non-D1 team's units get no row.

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
| `ValueError` | If `league` is not `"mens"` or `"womens"`, or a season is before the first one served. |

**Example**

```python
from sportsdataverse.mbb.mbb_epv import mbb_epv_lineup_games
units = mbb_epv_lineup_games(2026)
units.sort("net_epv", descending=True).head()
```

### mbb_epv_player_games {#mbb_epv_player_games}

`mbb_epv_player_games(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per college player and game: EPV added on the player's own actions and on-court EPV +/-.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_epv.wbb_epv_player_games`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

`sportsdataverse.nba.nba_epv.epv_player_games` of the seasons' plays, on-court from the ten the release puts on each row: one row per `(season, game_id, team_id, player_id)` with `epv_own`, `own_actions`, `off_epv_on`, `def_epv_on` and `epv_on`. `team_id` is the ESPN id (Int64), `player_id` the stats.ncaa.org id (String); a non-D1 team's players get no row.

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
| `ValueError` | If `league` is not `"mens"` or `"womens"`, or a season is before the first one served. |

**Example**

```python
from sportsdataverse.mbb.mbb_epv import mbb_epv_player_games
box = mbb_epv_player_games(2026)
box.sort("epv_on", descending=True).head()
```

### mbb_epv_plays {#mbb_epv_plays}

`mbb_epv_plays(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

EPV and EPV added on every college play-by-play event inside a possession.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_epv.wbb_epv_plays`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

The columns of `sportsdataverse.nba.nba_epv.nba_epv_plays`, one row per event in game order. `game_id` is the NCAA contest id; the team ids are ESPN ids (Int64, null for a non-D1 team); `person_id` is the stats.ncaa.org player id on the event (String, null on a team event) and the slots the possession's starting ten (String, null only where the release has no player); `event_type` is the NBA engine's vocabulary.

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
| `ValueError` | If `league` is not `"mens"` or `"womens"`, or a season is before the first one served. |

**Example**

```python
from sportsdataverse.mbb.mbb_epv import mbb_epv_plays
plays = mbb_epv_plays(2026)
plays.sort("epv_added", descending=True).head(5)
```

### mbb_epv_team_games {#mbb_epv_team_games}

`mbb_epv_team_games(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per college team and game: EPV added on offense and allowed on defense.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_epv.wbb_epv_team_games`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

`sportsdataverse.nba.nba_epv.epv_team_games` of the seasons' plays: one row per `(season, game_id, team_id)` with `opp_team_id`, `off_epv`, `def_epv`, `net_epv`, `poss`, `opp_poss`, `off_epv_per_poss` and `def_epv_per_poss`. ESPN team ids; a non-D1 team gets no row (`opp_team_id` is null against one).

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
| `ValueError` | If `league` is not `"mens"` or `"womens"`, or a season is before the first one served. |

**Example**

```python
import polars as pl
from sportsdataverse.mbb.mbb_epv import mbb_epv_team_games
teams = mbb_epv_team_games(2026)
teams.group_by("team_id").agg(pl.col("off_epv_per_poss").mean()).sort("off_epv_per_poss")
```

### mbb_game_flow {#mbb_game_flow}

`mbb_game_flow(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College game flow per team and game: runs, droughts, kills, lead changes, time leading, game control.

The definitions are in `sportsdataverse._common.game_flow`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_game_flow.wbb_game_flow`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id)` with the columns of `sportsdataverse.nba.nba_game_flow.nba_game_flow`. `game_id` is the NCAA contest id, `team_id` the ESPN team id (Int64). Empty input returns that schema with zero rows.

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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_game_flow import mbb_game_flow
flow = mbb_game_flow(2026)
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

### mbb_game_on_off {#mbb_game_on_off}

`mbb_game_on_off(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College in-game on/off: per player and game, the team with him on the floor and the rest of its game.

The definitions are in `sportsdataverse._common.game_stints`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_game_stints.wbb_game_on_off`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, player_id)`: `opp_team_id`, for each side (`_on`, `_off`) `minutes`, `poss`, `opp_poss`, `pts`, `opp_pts`, `plus_minus`, `off_rtg`, `def_rtg` and `net_rtg`; `<rating>_diff` (on minus off) and every rating's `_n`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `opp_team_id` | Int64 | The opponent's ESPN team id (null for a non-D1 opponent). |
| `player_id` | String | stats.ncaa.org player id. |
| `minutes_on` | Float64 | Game-clock minutes of the team's game with the player on the floor. |
| `poss_on` | Int64 | The team's offensive possessions with the player on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_on` | Int64 | The team's defensive possessions with the player on the floor. |
| `pts_on` | Int64 | The team's points with the player on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_on` | Int64 | The opponent's points with the player on the floor, credited as pts_on is. |
| `plus_minus_on` | Int64 | pts_on - opp_pts_on. |
| `off_rtg_on` | Float64 | Offensive rating with the player on the floor: pts_on per 100 poss_on; null with no possessions. |
| `def_rtg_on` | Float64 | Defensive rating with the player on the floor: opp_pts_on per 100 opp_poss_on; null with no possessions. |
| `net_rtg_on` | Float64 | Net rating with the player on the floor: off_rtg_on - def_rtg_on; null when either is null. |
| `minutes_off` | Float64 | Game-clock minutes of the team's game with the player off the floor (the rest of the team's game). |
| `poss_off` | Int64 | The team's offensive possessions with the player off the floor (the rest of the team's game) (a possession belongs to the five on the floor when it starts). |
| `opp_poss_off` | Int64 | The team's defensive possessions with the player off the floor (the rest of the team's game). |
| `pts_off` | Int64 | The team's points with the player off the floor (the rest of the team's game), each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_off` | Int64 | The opponent's points with the player off the floor (the rest of the team's game), credited as pts_off is. |
| `plus_minus_off` | Int64 | pts_off - opp_pts_off. |
| `off_rtg_off` | Float64 | Offensive rating with the player off the floor (the rest of the team's game): pts_off per 100 poss_off; null with no possessions. |
| `def_rtg_off` | Float64 | Defensive rating with the player off the floor (the rest of the team's game): opp_pts_off per 100 opp_poss_off; null with no possessions. |
| `net_rtg_off` | Float64 | Net rating with the player off the floor (the rest of the team's game): off_rtg_off - def_rtg_off; null when either is null. |
| `off_rtg_diff` | Float64 | On-off offensive rating: off_rtg_on - off_rtg_off; null when either is null. |
| `def_rtg_diff` | Float64 | On-off defensive rating: def_rtg_on - def_rtg_off; null when either is null. |
| `net_rtg_diff` | Float64 | On-off net rating: net_rtg_on - net_rtg_off; null when either is null. |
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
from sportsdataverse.mbb.mbb_game_stints import mbb_game_on_off
mbb_game_on_off(2025)
```

### mbb_game_player_box_splits {#mbb_game_player_box_splits}

`mbb_game_player_box_splits(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College player box by game, period and garbage time: minutes and counts from the play-by-play.

The definitions are in `sportsdataverse._common.game_stints`
(`~sportsdataverse._common.game_stints.game_player_box_splits`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_game_stints.wbb_game_player_box_splits`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

Per `(season, game_id, team_id, player_id)`: the `family="all"` row, one `family="period"` row per period played (`split` `p1`, `p2` for the men's halves, `p1` .. `p4` for the women's quarters from 2016, later periods the overtimes; `period`; `on_floor_at_end`) and the two `family="garbage"` rows (`garbage_time`, `not_garbage_time`), each with the columns of `sportsdataverse.nba.nba_game_stints.nba_game_player_box_splits`. Ids as in `mbb_game_stints`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `player_id` | String | stats.ncaa.org player id. |
| `family` | String | Split family: all (the game), period (one row per period he played) or garbage (garbage time in and out). |
| `split` | String | The split: all; p1, p2, ... for a period (periods past regulation are the overtimes); garbage_time or not_garbage_time (his box with garbage time removed). |
| `period` | Int64 | The period of a period row; null on the other families. |
| `seconds` | Float64 | Game-clock seconds he was on the floor in the split (an action's clock goes to the lineup at it). |
| `minutes` | Float64 | Game-clock minutes on the floor in the split (seconds / 60). |
| `points` | Int64 | Points the player scored: 2 or 3 per made field goal and 1 per made free throw. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goals attempted. |
| `fg3m` | Int64 | Three-pointers made. |
| `fg3a` | Int64 | Three-pointers attempted. |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throws attempted. |
| `oreb` | Int64 | Offensive rebounds (a team rebound is no player's). |
| `dreb` | Int64 | Defensive rebounds (a team rebound is no player's). |
| `reb` | Int64 | Total rebounds: oreb + dreb. |
| `ast` | Int64 | Assists: made field goals the play-by-play credits him as the passer on. |
| `stl` | Int64 | Steals he is credited with. |
| `blk` | Int64 | Shots he blocked. |
| `tov` | Int64 | Turnovers (a team turnover is no player's). |
| `pf` | Int64 | Personal fouls (technicals are not counted). |
| `on_floor_at_end` | Boolean | On a period row, whether he was in his side's lineup at the period's last action; null on the other families. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_game_stints import mbb_game_player_box_splits
mbb_game_player_box_splits(2025).filter(family="garbage", split="not_garbage_time").head()
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

### mbb_game_stints {#mbb_game_stints}

`mbb_game_stints(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College game stints: per player and game, every stretch on the floor with its clock, points and possessions.

The definitions are in `sportsdataverse._common.game_stints`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_game_stints.wbb_game_stints`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, player_id, stint_number)`: `opp_team_id`, `stint_number` (1 = the first in the game), `period`, `clock_in` / `clock_out` (the period clock, seconds left, when the stint began and ended), `seconds` / `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and the player's own box in the stint (`ncaa_box_events`; the columns of `sportsdataverse.nba.nba_game_stints.nba_game_stints`). `game_id` is the NCAA contest id, `team_id` the ESPN team id (Int64), `player_id` the NCAA player id (String). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `opp_team_id` | Int64 | The opponent's ESPN team id (null for a non-D1 opponent). |
| `player_id` | String | stats.ncaa.org player id. |
| `stint_number` | Int64 | The stint's place among the player's stints in the game, from 1 in game order. A stint is a run of consecutive actions of one period with him on the floor. |
| `period` | Int64 | Period of the stint (a stint never spans a period end). |
| `clock_in` | Float64 | Period clock (seconds left) when the stint began: the clock at the action before his first. |
| `clock_out` | Float64 | Period clock (seconds left) when the stint ended: the clock at his last action. |
| `seconds` | Float64 | Game-clock seconds of the stint: the clock that ran into each of its actions. |
| `minutes` | Float64 | Game-clock minutes of the stint (seconds / 60). |
| `poss` | Int64 | Offensive possessions that started during the stint (each counted on the lineup at the action that starts it). |
| `opp_poss` | Int64 | Defensive possessions that started during the stint, counted as poss is. |
| `pts` | Int64 | The team's points during the stint, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts` | Int64 | The opponent's points during the stint, credited as pts is. |
| `plus_minus` | Int64 | The stint's +/-: pts - opp_pts. |
| `points` | Int64 | Points the player scored: 2 or 3 per made field goal and 1 per made free throw, his own, on the stint's actions. |
| `fgm` | Int64 | Field goals made, his own, on the stint's actions. |
| `fga` | Int64 | Field goals attempted, his own, on the stint's actions. |
| `fg3m` | Int64 | Three-pointers made, his own, on the stint's actions. |
| `fg3a` | Int64 | Three-pointers attempted, his own, on the stint's actions. |
| `ftm` | Int64 | Free throws made, his own, on the stint's actions. |
| `fta` | Int64 | Free throws attempted, his own, on the stint's actions. |
| `oreb` | Int64 | Offensive rebounds (a team rebound is no player's), his own, on the stint's actions. |
| `dreb` | Int64 | Defensive rebounds (a team rebound is no player's), his own, on the stint's actions. |
| `reb` | Int64 | Total rebounds in the stint: oreb + dreb. |
| `ast` | Int64 | Assists: made field goals the play-by-play credits him as the passer on, his own, on the stint's actions. |
| `stl` | Int64 | Steals, his own, on the stint's actions. |
| `blk` | Int64 | Blocked shots, his own, on the stint's actions. |
| `tov` | Int64 | Turnovers (a team turnover is no player's), his own, on the stint's actions. |
| `pf` | Int64 | Personal fouls (technicals are not counted), his own, on the stint's actions. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_game_stints import mbb_game_stints
stints = mbb_game_stints(2025)
```

### mbb_game_units {#mbb_game_units}

`mbb_game_units(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

College in-game units: per 1- to 5-man set of teammates and game, ratings, four factors and shot zones.

The definitions are in `sportsdataverse._common.game_stints`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_game_stints.wbb_game_units`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, n_players, lineup_id)`: `opp_team_id`, `n_players` (1 to 5), `lineup_id` (the NCAA player ids sorted and joined with `-`), `player_1..5`, `minutes` / `seconds`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg`, the four factors and the shot zones for and against (as in `sportsdataverse.nba.nba_game_stints.nba_game_units`) and every rate's `<m>_n`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `opp_team_id` | Int64 | The opponent's ESPN team id (null for a non-D1 opponent). |
| `n_players` | Int64 | Unit size: 1 to 5 players. |
| `lineup_id` | String | The unit's player ids, sorted and joined with '-'. |
| `player_1` | String | Player 1 of the unit, in lineup_id order; null past n_players. |
| `player_2` | String | Player 2 of the unit, in lineup_id order; null past n_players. |
| `player_3` | String | Player 3 of the unit, in lineup_id order; null past n_players. |
| `player_4` | String | Player 4 of the unit, in lineup_id order; null past n_players. |
| `player_5` | String | Player 5 of the unit, in lineup_id order; null past n_players. |
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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_game_stints import mbb_game_units
units = mbb_game_units(2025)
```
