---
title: "NBA — additional Python functions — Analytics: nba_coach–nba_lineup"
sidebar_label: "Analytics: nba_coach–nba_lineup"
sidebar_position: 14
description: "NBA — additional Python functions — Analytics: nba_coach–nba_lineup — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_coach–nba_lineup

### nba_coach_seasons {#nba_coach_seasons}

`nba_coach_seasons(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', espn: 'bool' = True, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Head-coach seasons: W-L, ratings and four factors over the games each coach was on the bench for.

One row per season, team and head coach. A coach who took over mid-season gets his games
only. Who coached each game: see `sportsdataverse.nba.nba_coach_seasons`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"playoffs"` or `"play-in"`; the postseason games go to the coach of the team's last regular-season game. |
| `espn` | `bool` | `True` | Read ESPN's per-coach records (2002 on) to split a team-season that changed coach (about 90 requests a season). `False` credits only the seasons one coach had throughout. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, coach_id)`: `coach_id` is the stats.nba.com coach id, `team` the team abbreviation, `role` `"HC"`. `team_games` is the team's completed games of `season_type`, `games` / `wins` / `losses` / `win_pct` / `pts` / `opp_pts` the coach's (final scores), `first_game_date` / `last_game_date` his span. `pbp_games`, `poss`, `opp_poss`, `poss_pts`, `opp_poss_pts`, `seconds` and the `o_*` / `d_*` four-factor counts come from his games' possessions, and give `off_rtg`, `def_rtg`, `net_rtg`, `pace` and `efg_pct` .. `opp_ft_rate`. `win_pct` and the twelve rates have `<m>_rank` (1 = best), `<m>_pct` and `<m>_n`, within the season among coaches with half the most games; null under five coaches. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26; the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `team` | String | Team abbreviation (the stats schedule's). |
| `coach_id` | Int64 | stats.nba.com coach id of the head coach (nba_stats_coaches / wnba_stats_coaches). |
| `coach` | String | Head coach's name (stats.nba.com's staff list). |
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
| `pace` | Float64 | Pace: possessions per 48 minutes (40 with league_id="10", the WNBA), the mean of poss and opp_poss over the game clock those possessions used. |
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
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the three above (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.nba.nba_coach_seasons import nba_coach_seasons
coaches = nba_coach_seasons(2026)
coaches.sort("net_rtg", descending=True).select("coach", "team", "games", "wins", "net_rtg").head()

# Without ESPN (only the coaches who had the whole season)

nba_coach_seasons(2026, espn=False)
```

### nba_cup_sim {#nba_cup_sim}

`nba_cup_sim(season: 'int', *, as_of_date: 'dt.date | None' = None, n_sims: 'int' = 10000, seed: 'int' = 20261103, cup_schedule: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NBA Cup sim as of a date: odds to win the group, take a wild card, reach each round and win.

Runs the season sim (`sportsdataverse.nba.nba_season_sim.nba_season_sim`: the as-of
possession ridge with the preseason prior, one rating shift per team per simulated season, the
same seed and draws) with the Cup inside it, ranks every simulated group with the Cup
tiebreakers (`nba_cup_standings`), and plays the knockout: seed 1 hosts the wild card
and seed 2 seed 3 in each conference, the semifinals neutral through 2025-26 and at the
higher seed from 2026-27, the final neutral. A knockout game already played keeps its result.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2027` is 2026-27); from 2024. |
| `as_of_date` | `date \| None` | `None` | Games on or after this date are simulated even when they have a score (a backtest). None: every final game counts as played. |
| `n_sims` | `int` | `10000` | Simulated Cups (Monte Carlo error: the `_se` columns). |
| `seed` | `int` | `20261103` | Seeds the generator; the same data and seed give the same output. |
| `cup_schedule` | `DataFrame \| None` | `None` | A `~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2` frame; None fetches it from stats.nba.com (residential connection). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team (`team_id`, Int64), sorted by conference, group and mean group rank: the Group Play `wins` / `losses` / `point_diff` so far, the as-of `adj_net_rtg`, `p_win_group`, `p_wild_card`, `p_quarterfinal` (either), `p_semifinal`, `p_final`, `p_champion`, each with its Monte Carlo `_se` (`sqrt(p (1 - p) / n_sims)`), `mean_group_rank` and `n_sims`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2027 is 2026-27). |
| `as_of_date` | Date | The as-of date: games on or after it are simulated. Null when every final game counts as played. |
| `conference` | String | NBA conference (East / West). |
| `cup_group` | String | The team's NBA Cup group (East Group A .. West Group C). |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `wins` | Int64 | Group Play wins in the games already played. |
| `losses` | Int64 | Group Play losses in the games already played. |
| `point_diff` | Int64 | Group Play point differential in the games already played, 0 for a game that went to overtime. |
| `adj_net_rtg` | Float64 | As-of adjusted net rating from the season sim's possession ridge: points per 100 over an average opponent on a neutral court. |
| `p_win_group` | Float64 | Share of simulated Cups in which the team wins its group under the Cup tiebreakers. |
| `p_wild_card` | Float64 | Share of simulated Cups in which the team takes its conference's wild card: the best second-place team. |
| `p_quarterfinal` | Float64 | Share of simulated Cups in which the team reaches the knockout: a group win or the wild card. |
| `p_semifinal` | Float64 | Share of simulated Cups in which the team wins its quarterfinal. |
| `p_final` | Float64 | Share of simulated Cups in which the team reaches the final. |
| `p_champion` | Float64 | Share of simulated Cups the team wins (the final included). |
| `p_win_group_se` | Float64 | Monte Carlo standard error of p_win_group: sqrt(p (1 - p) / n_sims). |
| `p_wild_card_se` | Float64 | Monte Carlo standard error of p_wild_card: sqrt(p (1 - p) / n_sims). |
| `p_quarterfinal_se` | Float64 | Monte Carlo standard error of p_quarterfinal: sqrt(p (1 - p) / n_sims). |
| `p_semifinal_se` | Float64 | Monte Carlo standard error of p_semifinal: sqrt(p (1 - p) / n_sims). |
| `p_final_se` | Float64 | Monte Carlo standard error of p_final: sqrt(p (1 - p) / n_sims). |
| `p_champion_se` | Float64 | Monte Carlo standard error of p_champion: sqrt(p (1 - p) / n_sims). |
| `mean_group_rank` | Float64 | Mean Group Play finish (1-5) over the simulated Cups. |
| `n_sims` | Int64 | Number of simulated Cups behind every share on the row. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season` is before 2024 or `n_sims < 1`. |

**Example**

```python
import datetime as dt
from sportsdataverse.nba.nba_cup import nba_cup_sim
nba_cup_sim(2027, as_of_date=dt.date(2026, 11, 6)).select("team_id", "p_quarterfinal", "p_champion")
```

### nba_cup_standings {#nba_cup_standings}

`nba_cup_standings(season: 'int', *, as_of_date: 'dt.date | None' = None, cup_schedule: 'pl.DataFrame | None' = None, seed: 'int' = 20261103, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NBA Cup Group Play standings as of a date, with the official tiebreak order and wild cards.

The rules (NBA.com, "Emirates NBA Cup 101", https://www.nba.com/news/nba-cup-101, read
2026-11-04) are in `sportsdataverse.nba.nba_cup`: Group Play record, then head-to-head,
point differential and points scored in Group Play (overtime excluded), the previous
regular-season record and a random drawing; the wild card is the best second-place team of
each conference by the same order without head-to-head.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2027` is 2026-27); from 2024. |
| `as_of_date` | `date \| None` | `None` | Games on or after this date are unplayed. None: every final game counts. |
| `cup_schedule` | `DataFrame \| None` | `None` | A `~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2` frame for the season. None fetches it from stats.nba.com (a residential connection: the site hangs on datacenter IPs). |
| `seed` | `int` | `20261103` | Seeds the random drawing, the last tiebreaker. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team (stats.nba.com `team_id`, Int64), sorted by conference, group and rank: `group_rank` with `wins` / `losses` / `games_remaining`, the tiebreak inputs `point_diff` / `points` (overtime excluded) / `prior_win_pct`, `tiebreak` (the criterion that placed the team among those with its record; null when the record alone did), `conference_rank` (the group leaders 1-3, then the rest, the wild-card race), `leads_group`, `holds_wild_card` and `knockout_seed` (1-4 on the current table).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2027 is 2026-27). |
| `as_of_date` | Date | The as-of date: games on or after it are not counted. Null when every final game counts. |
| `conference` | String | NBA conference (East / West). |
| `cup_group` | String | The team's NBA Cup group (East Group A .. West Group C). |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `group_rank` | Int64 | Place in the group (1-5) by Group Play record, then head-to-head, point differential, points scored, the previous season's record and a random drawing. |
| `wins` | Int64 | Group Play wins. |
| `losses` | Int64 | Group Play losses. |
| `games_remaining` | Int64 | Group Play games still to play. |
| `point_diff` | Int64 | Group Play point differential, 0 for a game that went to overtime (the Cup rule). |
| `points` | Int64 | Group Play points scored, overtime points excluded (the Cup rule). |
| `prior_win_pct` | Float64 | The previous regular season's winning percentage, the fourth tiebreaker. |
| `tiebreak` | String | The criterion that placed the team among the teams with its group record: head_to_head, point_diff, points, prior_record or random; null when the record alone did. |
| `conference_rank` | Int64 | Rank in the conference as stats.nba.com orders it: the group leaders 1-3 (the seeding order), then the second-place teams (the wild-card race), then the rest, each by record and the tiebreakers without head-to-head. |
| `leads_group` | Boolean | Whether the team leads (or won) its group. |
| `holds_wild_card` | Boolean | Whether the team holds its conference's wild card on the current table (the best second-place team). |
| `knockout_seed` | Int64 | Knockout seed (1-3 the group leaders by record, 4 the wild card) on the current table; null otherwise. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `season` is before 2024 or the schedule does not hold six groups of five. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_cup import nba_cup_standings
nba_cup_standings(2026).filter(pl.col("knockout_seed").is_not_null())
```

### nba_draft_lottery_odds {#nba_draft_lottery_odds}

`nba_draft_lottery_odds(*, league_id: 'str' = '00', chances: 'Sequence[float] | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Exact draft-lottery odds by pre-lottery slot: the chance each slot gets each pick.

NBA: the 2019 odds, 14 slots, four picks drawn. WNBA: two picks drawn, with the published
chances for the number of lottery teams. Every ordered draw is enumerated (24,024 for the
NBA), so the odds are exact, not simulated.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `chances` | `Sequence[float] \| None` | `None` | Chances per slot, worst record first. Default: `NBA_LOTTERY_CHANCES` (NBA) or the five-team `WNBA_LOTTERY_CHANCES` table (WNBA, the 2026 lottery). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(lottery_slot, pick)`: `lottery_slot` (1 = the worst record), `chances` (the slot's chances), `pick` and `probability`.

| col_name | type | description |
|---|---|---|
| `lottery_slot` | Int64 | Pre-lottery slot, 1 = the worst record. |
| `chances` | Float64 | The slot's chances in 1,000 (NBA 2019 table; WNBA published table or the caller's). |
| `pick` | Int64 | Draft pick number. |
| `probability` | Float64 | Exact probability that the slot gets the pick, from enumerating every ordered draw. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `chances` has a non-positive or non-finite entry or fewer entries than picks drawn. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_season_rules import nba_draft_lottery_odds
odds = nba_draft_lottery_odds()
odds.filter(pl.col("lottery_slot") == 1)

# A WNBA lottery with its own chances

nba_draft_lottery_odds(league_id="10", chances=[442, 276, 178, 104])
```

### nba_epv_lineup_games {#nba_epv_lineup_games}

`nba_epv_lineup_games(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per five-man unit and game: the EPV added with the unit on the floor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

`epv_lineup_games` of the seasons' plays, on-court from the `nba_stats_game_lineups` release: one row per `(season, game_id, team_id, lineup_id)` with `off_epv`, `def_epv`, `net_epv` and `plays`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `team_id` | Int64 | stats.nba.com team id. |
| `lineup_id` | String | The five player ids, sorted and joined with '-'. |
| `off_epv` | Float64 | EPV added by the unit's team with the ball while the unit was on. |
| `def_epv` | Float64 | EPV added by the opponent with the ball while the unit was on. |
| `net_epv` | Float64 | off_epv - def_epv. |
| `plays` | Int64 | Play-by-play events with the unit on the floor. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a season is before the first one served. |

**Example**

```python
from sportsdataverse.nba.nba_epv import nba_epv_lineup_games
units = nba_epv_lineup_games(2026)
units.filter(game_id="0022500101")
```

### nba_epv_player_games {#nba_epv_player_games}

`nba_epv_player_games(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per player and game: EPV added on the player's own actions and on-court EPV +/-.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

`epv_player_games` of the seasons' plays, on-court from the `nba_stats_game_lineups` release: one row per `(season, game_id, team_id, player_id)` with `epv_own`, `own_actions`, `off_epv_on`, `def_epv_on` and `epv_on`. Ids are the stats.nba.com ids (Int64; `game_id` a String).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id` | Int64 | stats.nba.com player (person) id. |
| `epv_own` | Float64 | EPV credited to the player's own events: as the offensive actor the play's epv_added, as a defensive actor its negative, a steal or block the negative of the turnover or miss it ended. |
| `own_actions` | Int64 | How many of the player's own events were credited. |
| `off_epv_on` | Float64 | EPV added by the player's team with the ball while the player was on the floor. |
| `def_epv_on` | Float64 | EPV added by the opponent with the ball while the player was on the floor. |
| `epv_on` | Float64 | On-court EPV +/-: off_epv_on - def_epv_on. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a season is before the first one served. |

**Example**

```python
from sportsdataverse.nba.nba_epv import nba_epv_player_games
box = nba_epv_player_games(2026)
box.filter(game_id="0022500101")
```

### nba_epv_plays {#nba_epv_plays}

`nba_epv_plays(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

EPV and EPV added on every play-by-play event inside a possession.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per event, in game order: `season`, `game_id`, `possession_number`, `step` (the event's place in the possession), `order_index`, `action_number`, the offense, defense and home team ids, the event's `team_id` / `person_id` / `event_type` / `sub_type` / `description`, the possession's starting ten, the state (`EPV_FEATURES`), `points`, `remaining_points`, `is_last`, `next_in_period`, `epv`, `epv_start`, `epv_before`, `epv_after` and `epv_added`. Every game of every season type is scored.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `possession_number` | Int64 | The possession's number in the game (the possessions release's). |
| `step` | Int64 | The event's place in its possession, from 1. |
| `order_index` | Int64 | The event's order in the game's play-by-play. |
| `action_number` | Int64 | stats.nba.com action number of the event (the game lineups release's key). |
| `offense_team_id` | Int64 | Team id of the offense (the team with the ball). |
| `defense_team_id` | Int64 | Team id of the defense. |
| `home_team_id` | Int64 | Team id of the home team. |
| `team_id` | Int64 | Team id on the event: the team of the player who made it, or the team on a team event (a team rebound or timeout, which the feed files under person_id). |
| `person_id` | Int64 | Player id on the event; the team id, or 0, on team events. |
| `event_type` | String | The release's event type (made_shot, missed_shot, rebound, foul, free_throw, turnover, ...). |
| `sub_type` | String | The release's event sub type (the foul or free-throw kind, the shot type, ...). |
| `description` | String | The play-by-play text of the event. |
| `off_player_1` | Int64 | stats.nba.com player id of offensive player 1 of 5 when the possession started. |
| `off_player_2` | Int64 | stats.nba.com player id of offensive player 2 of 5 when the possession started. |
| `off_player_3` | Int64 | stats.nba.com player id of offensive player 3 of 5 when the possession started. |
| `off_player_4` | Int64 | stats.nba.com player id of offensive player 4 of 5 when the possession started. |
| `off_player_5` | Int64 | stats.nba.com player id of offensive player 5 of 5 when the possession started. |
| `def_player_1` | Int64 | stats.nba.com player id of defensive player 1 of 5 when the possession started. |
| `def_player_2` | Int64 | stats.nba.com player id of defensive player 2 of 5 when the possession started. |
| `def_player_3` | Int64 | stats.nba.com player id of defensive player 3 of 5 when the possession started. |
| `def_player_4` | Int64 | stats.nba.com player id of defensive player 4 of 5 when the possession started. |
| `def_player_5` | Int64 | stats.nba.com player id of defensive player 5 of 5 when the possession started. |
| `start_type` | Int64 | State: how the possession started, 0 dead ball, 1 timeout, 2 made shot, 3 missed shot, 4 live-ball turnover. |
| `period` | Int64 | State: the period, overtimes folded to 5. |
| `seconds_remaining` | Float64 | State: seconds left in the period after the event. |
| `start_margin` | Float64 | State: the offense's lead when the possession started (before its first event). |
| `transition` | Int64 | State: 0 half court, 1 transition window open (live start, no shot or dead ball yet, within 10 s), 2 a transition possession after its first shot. |
| `last_event` | Int64 | State: the event just processed, coded by its index in nba_epv.LAST_EVENTS (11 the start). |
| `ft_pending` | Int64 | State: free throws still to come in the trip. |
| `shot_zone` | Int64 | State: zone of the possession's latest shot, 0 none, 1 rim, 2 short, 3 mid two, 4 three. |
| `points` | Int64 | The offense's points on the event. |
| `remaining_points` | Int64 | The offense's points after the event to the end of the possession (the model's label). |
| `is_last` | Boolean | Whether the event is its possession's last. |
| `next_in_period` | Boolean | Whether another possession follows in the same period. |
| `epv` | Float64 | Expected points for the offense to the end of the possession, after the event. |
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
from sportsdataverse.nba.nba_epv import nba_epv_plays
plays = nba_epv_plays(2026)
plays.filter(game_id="0022500101").sort("epv_added", descending=True).head(5)
```

### nba_epv_team_games {#nba_epv_team_games}

`nba_epv_team_games(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per team and game: EPV added on offense and allowed on defense.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), from the booster's first training season (2022) on. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

`epv_team_games` of the seasons' plays: one row per `(season, game_id, team_id)` with `opp_team_id`, `off_epv`, `def_epv`, `net_epv`, `poss`, `opp_poss`, `off_epv_per_poss` and `def_epv_per_poss`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `team_id` | Int64 | stats.nba.com team id. |
| `opp_team_id` | Int64 | The opponent's team id. |
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
from sportsdataverse.nba.nba_epv import nba_epv_team_games
teams = nba_epv_team_games(2026)
teams.group_by("team_id").agg(pl.col("off_epv_per_poss").mean()).sort("off_epv_per_poss")
```

### nba_foul_drawing {#nba_foul_drawing}

`nba_foul_drawing(season: 'str', *, league_id: 'str' = '00', base: 'Optional[pl.DataFrame]' = None, advanced: 'Optional[pl.DataFrame]' = None, player_mix: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Expected FTA + residual foul-drawing skill from Synergy play-type mix.

`lg_ft_rate_t` = poss-weighted league mean of `ft_freq` for play type
`t` (`Σ_players(ft_freq_t·poss_t) / Σ_players(poss_t)`).
`expected_fta = scale · Σ_t poss_t · lg_ft_rate_t`;
`foul_draw_skill = 100·(fta − expected_fta)/poss`.

`ft_freq` (Synergy's `ft_poss_pct`) counts FT-drawing *trips*, not
individual free-throw attempts (a shooting foul is typically a 2-shot
trip) -- `scale = Σ actual fta / Σ raw trip-based estimate` is derived
from the fetched season itself (never hard-coded) so the trip-to-attempt
conversion self-normalizes and `Σ expected_fta ≡ Σ fta` holds exactly.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base` measure) frame: `player_id`, `fta`, `poss` (bypasses the live fetch). |
| `advanced` | `Optional[DataFrame]` | `None` | Injected `Advanced`-measure frame with `player_id`, `pfd` (personal fouls drawn); optional -- `pfd` is `null` when omitted (`fta` is the always-present proxy). |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix: `player_id`, `play_type`, `poss`, `ft_freq`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `poss`/`fta`/ `expected_fta`/`foul_draw_skill` (Float64), `pfd` (Float64, null when *advanced* has no data for that player or is omitted). Zero-row frame with this schema when the inputs are empty (sparse-coverage leagues never raise).

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_foul_drawing
f = nba_foul_drawing("2023-24")
print(f.sort("foul_draw_skill", descending=True).head(10))

# Injected offline (oracle / test) path

f = nba_foul_drawing("2023-24", base=base_df, player_mix=mix_df)

# Pipeline next step

f.filter(pl.col("poss") >= 200).sort("foul_draw_skill", descending=True)
```

### nba_game_flow {#nba_game_flow}

`nba_game_flow(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Game flow per team and game: runs, droughts, kills, lead changes, time leading and game control.

The definitions are in `sportsdataverse._common.game_flow`. Over the 1,315 games of the
2025-26 NBA season (every season type), `lead_changes` equals the official cdn.nba.com box
score's in every game and `times_tied` and `largest_lead` in all but one; over the 310 games
of the 2025 WNBA season all three equal cdn.wnba.com's in every game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id)`: `opp_team_id`, `is_home`, `pts` / `opp_pts` and `won`; `game_seconds`; `lead_sec` / `trail_sec` / `tied_sec` and `lead_pct`; `lead_changes` and `times_tied` (game totals); `largest_lead`, `largest_deficit` and `comeback`; `max_run`, `runs_10` and `opp_runs_10` (unanswered-point runs); `longest_drought_sec`; `avg_margin` and `avg_wp` (game control); `def_poss`, `stops`, `kills` and `double_kills`. Ids are the stats.nba.com ids (Int64; `game_id` a String). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `team_id` | Int64 | stats.nba.com team id. |
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
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_game_flow import nba_game_flow
flow = nba_game_flow(2026)
flow.sort("comeback", descending=True).head()
```

### nba_game_lineups_used {#nba_game_lineups_used}

`nba_game_lineups_used(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The lineups used: per five-man unit and game, minutes, possessions, +/- and ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, lineup_id)`: `lineup_id` (the five player ids sorted and joined with `-`), `player_1..5`, and the same totals and ratings as `nba_game_player_impact`, sorted by minutes within each team and game. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `team_id` | Int64 | stats.nba.com team id. |
| `opp_team_id` | Int64 | The opponent's team id. |
| `lineup_id` | String | The five player ids of the unit, sorted and joined with '-'. |
| `player_1` | Int64 | Player 1 of the unit, in lineup_id order. |
| `player_2` | Int64 | Player 2 of the unit, in lineup_id order. |
| `player_3` | Int64 | Player 3 of the unit, in lineup_id order. |
| `player_4` | Int64 | Player 4 of the unit, in lineup_id order. |
| `player_5` | Int64 | Player 5 of the unit, in lineup_id order. |
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
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_game_impact import nba_game_lineups_used
units = nba_game_lineups_used(2026)
units.filter(game_id="0022500101").group_by("team_id").head(5)
```

### nba_game_player_impact {#nba_game_player_impact}

`nba_game_player_impact(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The game impact box: per player and game, minutes, on-court possessions, +/- and ratings.

The on-court +/- is the official box score's: on the 2025-26 NBA season it equals
stats.nba.com's `plus_minus_points` on 91% of player-games, the rest being on-court lineup
errors in the source. The definitions are in `sportsdataverse._common.game_impact`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, game_id, team_id, player_id)`: `opp_team_id`, `minutes` and `seconds` on the floor, `poss` / `opp_poss` (offensive / defensive possessions on the floor), `pts` / `opp_pts` (points scored / allowed while on), `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions) and their `_n` sample sizes. Ids are the stats.nba.com ids (Int64; `game_id` a String). Every game of every season type is included. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `team_id` | Int64 | stats.nba.com team id. |
| `opp_team_id` | Int64 | The opponent's team id. |
| `player_id` | Int64 | stats.nba.com player (person) id. |
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
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_game_impact import nba_game_player_impact
box = nba_game_player_impact(2026)
box.filter(game_id="0022500101")
```

### nba_impact_rows {#nba_impact_rows}

`nba_impact_rows(pbp: 'pl.DataFrame', game_lineups: 'pl.DataFrame', possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'pl.DataFrame'`

Map the stats.nba.com releases onto the game-impact row frame.

One row per play-by-play action (its clock and points, on the lineup at that action; a free
throw on the lineup at the last foul) and one per possession (its lineup and which side had
the ball). Games not in `schedule` are dropped, and so are the possessions of a game with
no play-by-play actions and those whose offense is neither team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_nba_stats_pbp` / `load_wnba_stats_pbp` rows (`game_id`, `order_index`, `action_number`, `period`, `seconds_remaining`, `score_home`, `score_away`, `is_free_throw`, `is_foul`, `season`). |
| `game_lineups` | `DataFrame` |  | `load_nba_stats_game_lineups` / `load_wnba_stats_game_lineups` rows. |
| `possessions` | `DataFrame` |  | `load_nba_stats_possessions` / `load_wnba_stats_possessions` rows. |
| `schedule` | `DataFrame` |  | `load_nba_stats_schedules` / `load_wnba_stats_schedules` rows (`game_id`, `home_team_id`, `away_team_id`), every season type. |
| `league_id` | `str` | `'00'` | `"00"` NBA (12-minute quarters) or `"10"` WNBA (10-minute quarters). |

**Returns**

Rows in `sportsdataverse._common.game_impact.impact_row_schema` with Int64 player ids.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the action or possession. |
| `game_id` | String | stats.nba.com game id. |
| `home_team_id` | Int64 | stats.nba.com team id of the home team, from the schedule. |
| `away_team_id` | Int64 | stats.nba.com team id of the away team, from the schedule. |
| `home_1` | Int64 | Home player 1 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_2` | Int64 | Home player 2 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_3` | Int64 | Home player 3 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_4` | Int64 | Home player 4 on the floor (on a free throw, the player on the floor at the last foul). |
| `home_5` | Int64 | Home player 5 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_1` | Int64 | Away player 1 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_2` | Int64 | Away player 2 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_3` | Int64 | Away player 3 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_4` | Int64 | Away player 4 on the floor (on a free throw, the player on the floor at the last foul). |
| `away_5` | Int64 | Away player 5 on the floor (on a free throw, the player on the floor at the last foul). |
| `seconds` | Float64 | Game clock that ran into this action (0 on a possession row). |
| `home_pts` | Int64 | Points the home team scored on this action (0 on a possession row). |
| `away_pts` | Int64 | Points the away team scored on this action (0 on a possession row). |
| `home_poss` | Int64 | 1 on a possession row where the home team has the ball, else 0. |
| `away_poss` | Int64 | 1 on a possession row where the away team has the ball, else 0. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba import nba_loaders as L
from sportsdataverse.nba.nba_game_impact import nba_impact_rows
rows = nba_impact_rows(L.load_nba_stats_pbp(2025), L.load_nba_stats_game_lineups(2025),
                       L.load_nba_stats_possessions(2025), L.load_nba_stats_schedules(2025))
```

### nba_l2m {#nba_l2m}

`nba_l2m(game_id: 'str | int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse an NBA Last Two Minute report from official.nba.com.

Retrieves the L2M report for a given game and returns parsed tables of calls,
game metadata, and error statistics. The game_id is zero-padded to 10 digits
(e.g., 42500405 becomes "0042500405"). A report is published for any game that
is within 3 points (5 points before the 2017-18 season) at any point during the
last two minutes of the fourth quarter or overtime -- not only playoff games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str \| int` |  | NBA game ID (can be int or str). Automatically zero-padded to 10 digits. |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict. Otherwise, a dict with keys `"calls"`, `"game"`, `"stats"` mapping to DataFrames as documented in `parse_nba_l2m`.

| col_name | type | description |
|---|---|---|
| `calls.game_id` | String | 10-digit NBA game id (zero-padded), the join key back to the L2M game and stats tables. |
| `calls.period` | Int64 | Period number extracted from period_name's digits: 4 for the fourth quarter, 5+ for overtime periods (Q5 = OT1 through Q8 = OT4). |
| `calls.period_name` | String | Raw NBA.com period label for the graded play, e.g. Q4 for the fourth quarter or Q5 for the first overtime. |
| `calls.pc_time` | String | Raw game clock string from the L2M report before normalization, in MM:SS or MM:SS.t tenths-of-a-second format. |
| `calls.seconds_remaining` | Float64 | Seconds remaining in the period at the graded play, parsed out of pc_time. |
| `calls.call_type` | String | Raw "Call: Type" label from the report, e.g. "Foul: Shooting" or "Turnover:  24 Second Violation"; whitespace (including doubled spaces) is NOT cleaned here -- the cleaned, upper-cased parts are call and type. |
| `calls.call` | String | Upper-cased category before the colon in call_type, e.g. FOUL, TURNOVER, or STOPPAGE. |
| `calls.type` | String | Upper-cased detail after the colon in call_type, e.g. SHOOTING or 24 SECOND VIOLATION. |
| `calls.committing` | String | Player, team nickname, or coach responsible for the graded action. |
| `calls.disadvantaged` | String | Player or team nickname disadvantaged by the graded action, blank when not applicable. |
| `calls.decision` | String | Normalized grading decision: CC (correct call), CNC (correct non-call), IC (incorrect call), or INC (incorrect non-call); null when the report marks the play undetectable at game speed. |
| `calls.decision_raw` | String | Raw grading code from the report before normalization, including the rare NCC/NCI aliases folded into decision. |
| `calls.comment` | String | Grader's free-text explanation of the ruling, sometimes naming a player as "Last (TEAM)". |
| `calls.difficulty` | String | Grader's difficulty rating for the play, e.g. Observable, Difficult, Blatant/Hard, or Undetectable. |
| `calls.video_event_id` | String | L2M video event id from the report (source field VideolLink, sic); not a stats.nba.com play-by-play EVENTNUM, so it cannot be joined to pbp data. |
| `calls.pos_id` | Int64 | Possession id assigned by the report, rising monotonically across the game; rows sharing pos_start/pos_end belong to the same possession. |
| `calls.pos_start` | String | Game clock at the start of the possession containing this graded play. |
| `calls.pos_end` | String | Game clock at the end of the possession containing this graded play. |
| `calls.pos_team_id` | Int64 | 10-digit NBA team id of the team in possession during the graded play. |
| `game.game_id` | String | 10-digit NBA game id (zero-padded), the join key to the calls and stats tables. |
| `game.game_date` | Date | Game date parsed from the report's local tip-off timestamp (no timezone offset is applied). |
| `game.season_type` | String | Season type inferred from the third digit of game_id: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `game.home_team_id` | Int64 | 10-digit NBA team id (1610612xxx) of the home team. |
| `game.away_team_id` | Int64 | 10-digit NBA team id (1610612xxx) of the away team. |
| `game.home_team_abbr` | String | Three-letter abbreviation of the home team. |
| `game.away_team_abbr` | String | Three-letter abbreviation of the away team. |
| `game.home_team_name` | String | Home team nickname as published in the report, e.g. Thunder rather than Oklahoma City Thunder. |
| `game.away_team_name` | String | Away team nickname as published in the report. |
| `game.home_score` | Int64 | Home team's final score for the game. |
| `game.away_score` | Int64 | Away team's final score for the game. |
| `game.l2m_comments` | String | Report-level note from the league, e.g. a technical-issue disclaimer about missing video; null for almost every game. |
| `stats.game_id` | String | 10-digit NBA game id (zero-padded), the join key to the calls and game tables. |
| `stats.stat_name` | String | Name of the report-level error-rate statistic: Calls, Errors in Favor, or Possessions in Favor. |
| `stats.home` | Int64 | Value of stat_name attributed to the home team. |
| `stats.away` | Int64 | Value of stat_name attributed to the away team. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `game_id` is not one non-negative integer id -- a bool, a negative or fractional number, a string that is not all digits, or more than 10 digits (checked before any request). |
| `NoDataError` | The game has no L2M report (a 404, or S3's `AccessDenied` 403). Only games within 3 points (5 before 2017-18) at some point in the last two minutes of the fourth quarter or overtime get one, so most games have none; a game that just ended may not be graded yet. |
| `AssetFetchError` | The fetch failed (a transport error, a rate limit, an Akamai WAF block, or a 5xx that outlived the retry budget), or the 200 body is not a JSON object whose `game` is exactly one row, or its `l2m`/`stats` table is not a list of records (empty is fine) -- an error envelope or a changed schema, checked with `raw=True` too. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_l2m
result = nba_l2m("0042500405")
calls = result["calls"]
print(f"Game had {calls.height} tracked plays in the L2M window")

# Parse as pandas instead

result = nba_l2m("0042500405", return_as_pandas=True)
calls_pd = result["calls"]

# Access raw JSON

payload = nba_l2m("0042500405", raw=True)
print(payload["game"])
```

### nba_l2m_games {#nba_l2m_games}

`nba_l2m_games(season: 'int | str', *, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Fetch the list of games with Last Two Minute reports for an NBA season.

Retrieves and parses the L2M season index page from official.nba.com,
returning a table of all games for which L2M reports exist. JSON reports
exist only from 2019-01-01 onward; earlier seasons' index pages list PDFs,
which this function ignores. A release loader for historical (PDF-era)
reports is planned but does not exist yet.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int \| str` |  | The NBA season (end year) as a 4-digit int or string, e.g. 2026 for the 2025-26 season. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

A `polars.DataFrame` (or `pandas.DataFrame` when `return_as_pandas=True`) with schema `{"game_id": Utf8, "season": Int32, "season_type": Utf8, "label": Utf8}`, one row per unique game ID in page order.

| col_name | type | description |
|---|---|---|
| `game_id` | String | 10-digit NBA game id (zero-padded), parsed from the season listing page's report link. |
| `season` | Int64 | NBA season end year passed to the function (e.g. 2026 for the 2025-26 season), stamped onto every row. |
| `season_type` | String | Season type inferred from the third digit of game_id: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `label` | String | Matchup label text scraped from the listing page link, e.g. "Thunder 125, Rockets 124 (2OT)". |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season` is not a 4-digit year (an int or an all-digit string), checked before any request. |
| `NoDataError` | Official.nba.com has no listing page for that season (a 404, or S3's `AccessDenied` 403) -- e.g. a future season, or one from before the L2M program began. |
| `AssetFetchError` | The fetch failed (network error, rate limit, or Akamai WAF block), or a 200 response that is missing the expected "Last Two Minute" page marker (an Akamai interstitial, a blank body, or a redesigned page), or that links reports in a shape the parser does not read (a redesigned page) -- checked here, not in `parse_nba_l2m_games`, so the parser itself never raises. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_l2m_games
df = nba_l2m_games(2026)
print(f"Season had {df.height} games with L2M reports")

# Get playoff games only

df = nba_l2m_games(2026)
playoffs = df.filter(df["season_type"] == "playoffs")

# Convert to pandas

df = nba_l2m_games(2026, return_as_pandas=True)
```

### nba_lineup_count_rows {#nba_lineup_count_rows}

`nba_lineup_count_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame') -> 'pl.DataFrame'`

Map `nba_stats_possessions` rows onto the lineup-seasons count frame.

Each possession's counts go to the side that had the ball, its `dreb` (the defense's
defensive rebounds) to the other side, on the possession's lineup. A possession whose offense
is neither team of its `schedule` game is dropped, as in the game impact box.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_nba_stats_possessions` / `load_wnba_stats_possessions` rows (`fg2a`, `fg2m`, `fg3a`, `fg3m`, `fta`, `oreb`, `dreb`, `tov`). |
| `schedule` | `DataFrame` |  | The games to keep (`game_id`, `home_team_id`, `away_team_id`). |

**Returns**

One row per possession in `sportsdataverse._common.lineup_seasons.lineup_count_schema` with Int64 player ids.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.nba.com game id. |
| `home_team_id` | Int64 | stats.nba.com team id of the home side. |
| `away_team_id` | Int64 | stats.nba.com team id of the away side. |
| `home_1` | Int64 | stats.nba.com id of home player 1 on the floor when the possession starts. |
| `home_2` | Int64 | stats.nba.com id of home player 2 on the floor when the possession starts. |
| `home_3` | Int64 | stats.nba.com id of home player 3 on the floor when the possession starts. |
| `home_4` | Int64 | stats.nba.com id of home player 4 on the floor when the possession starts. |
| `home_5` | Int64 | stats.nba.com id of home player 5 on the floor when the possession starts. |
| `away_1` | Int64 | stats.nba.com id of away player 1 on the floor when the possession starts. |
| `away_2` | Int64 | stats.nba.com id of away player 2 on the floor when the possession starts. |
| `away_3` | Int64 | stats.nba.com id of away player 3 on the floor when the possession starts. |
| `away_4` | Int64 | stats.nba.com id of away player 4 on the floor when the possession starts. |
| `away_5` | Int64 | stats.nba.com id of away player 5 on the floor when the possession starts. |
| `home_fga` | Int64 | The home side's field-goal attempts in the possession. |
| `home_fgm` | Int64 | The home side's field goals made in the possession. |
| `home_fg3a` | Int64 | The home side's three-point attempts in the possession. |
| `home_fg3m` | Int64 | The home side's threes made in the possession. |
| `home_fta` | Int64 | The home side's free-throw attempts in the possession. |
| `home_oreb` | Int64 | The home side's offensive rebounds in the possession. |
| `home_dreb` | Int64 | The home side's defensive rebounds in the possession. |
| `home_tov` | Int64 | The home side's turnovers in the possession. |
| `away_fga` | Int64 | The away side's field-goal attempts in the possession. |
| `away_fgm` | Int64 | The away side's field goals made in the possession. |
| `away_fg3a` | Int64 | The away side's three-point attempts in the possession. |
| `away_fg3m` | Int64 | The away side's threes made in the possession. |
| `away_fta` | Int64 | The away side's free-throw attempts in the possession. |
| `away_oreb` | Int64 | The away side's offensive rebounds in the possession. |
| `away_dreb` | Int64 | The away side's defensive rebounds in the possession. |
| `away_tov` | Int64 | The away side's turnovers in the possession. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a count column holds a null in a possession of a `schedule` game. |

**Example**

```python
from sportsdataverse.nba import nba_loaders as L
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows
rows = nba_lineup_count_rows(L.load_nba_stats_possessions(2024), L.load_nba_stats_schedules(2024))
```
