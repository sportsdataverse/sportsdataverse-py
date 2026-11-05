---
title: "NBA — additional Python functions — Analytics: nba_cup–nba_score"
sidebar_label: "Analytics: nba_cup–nba_score"
sidebar_position: 14
description: "NBA — additional Python functions — Analytics: nba_cup–nba_score — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_cup–nba_score

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

### nba_play_context {#nba_play_context}

`nba_play_context(game_id: 'str', league_id: 'str' = '00', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Fetch one game and return its possessions with the full CTG play-context surface.

Single live call to `nba_stats_playbyplayv3`, then
`add_play_context`. Works for NBA (`league_id="00"`), WNBA and the
G-League — `stats.wnba.com` ships the same play-by-play shapes, and every
threshold here is league-agnostic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | Ten-character game identifier (e.g. `"0022200001"`). |
| `league_id` | `str` | `'00'` | League identifier (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 10.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Possession frame with the play-context columns. Empty/malformed payloads return a zero-row frame — never raises.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba.nba_play_context import nba_play_context
poss = nba_play_context("0022200001")
print(poss["possession_start_type_ctg"].value_counts())

# Transition rate for the game

import polars as pl
clean = poss.filter(
    (pl.col("is_garbage_time") == False) & (pl.col("is_heave_possession") == False)
)
print(clean["is_transition"].mean())
```

### nba_player_ages {#nba_player_ages}

`nba_player_ages(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Per-player age for a season (bulk), for the DARKO aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_leaguedashplayerbiostats` replacement for offline tests. |

**Returns**

Frame `player_id:Int64, age:Float64`.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_player_ages
ages = nba_player_ages("2023-24")
print(ages.head())
```

### nba_player_identity {#nba_player_identity}

`nba_player_identity(player_logs: 'pl.DataFrame') -> 'pl.DataFrame'`

Human-readable identity for every player in a season's box logs.

Model outputs key on `player_id` alone, which makes them unusable without a
second lookup -- a leaderboard reads `1628983` instead of
`Shai Gilgeous-Alexander`. This derives the display columns from the season's
own game logs, so they are **season-accurate**: a player's team is what he
actually played for that year, not his current one (which is what a player
directory would give and would silently mislabel every historical season).

A traded player has rows for several teams. `team_*` is his **primary** team
by minutes -- the one a reader means when they say "his team that season" --
and `teams` lists every abbreviation he appeared for, in descending minutes,
so a trade is visible rather than silently collapsed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game rows from `leaguegamelog` (the `player_or_team="P"` variant), carrying `player_id`, `player_name`, `team_id`, `team_abbreviation`, `team_name` and `min`. |

**Returns**

One row per `player_id` with `PLAYER_IDENTITY_SCHEMA`. An empty input -- or one missing any required column, `min` included -- gives the zero-row frame with that schema, so callers can join unconditionally. `min` is required rather than optional: without it every team totals zero minutes and "primary team" quietly degrades to whichever `team_id` sorts first, which looks like an answer but is not one. A malformed frame yields the typed zero-row frame instead of raising, so a caller can join unconditionally.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `teams` | String | Nested list of member-team membership spans. |

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_player_identity

logs = pl.DataFrame({
    "player_id": [1628983],
    "player_name": ["Shai Gilgeous-Alexander"],
    "team_id": [1610612760],
    "team_abbreviation": ["OKC"],
    "team_name": ["Oklahoma City Thunder"],
    "min": [34.0],
})
ratings = pl.DataFrame({"player_id": [1628983], "war": [21.9]})
named = ratings.join(nba_player_identity(logs), on="player_id", how="left")
print(named.select("player_name", "team_name", "war"))
```

### nba_player_positions {#nba_player_positions}

`nba_player_positions(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Fetch league-wide listed positions for a season as numeric 1-5.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_playerindex` replacement for offline tests. |

**Returns**

Frame with columns `player_id:Int64, position_num:Float64`.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | If `curl_cffi` is not installed (required by the stats runtime). |

**Example**

```python
from sportsdataverse.nba import nba_player_positions
pos = nba_player_positions("2023-24")
print(pos.head())

# Offline / injectable fetch for testing

import polars as pl
stub = lambda **kw: pl.DataFrame({"person_id": [1], "position": ["PG"]})
pos = nba_player_positions("2023-24", fetch=stub)
```

### nba_player_props {#nba_player_props}

`nba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-player expected prop lines + team pace projection for a matchup.

Loads the season's player box logs + team ratings, keeps only games played
strictly before `as_of_date` (both inputs), computes per-minute
`player_rates`, and projects each player's line onto their mean
minutes and the matchup's pace factor (`exp_poss / avg_pace`). Only the
two teams in the matchup are returned.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024`). |
| `game_id` | `str` |  | The game id. When `as_of_date` is `None` and this game is in the season's box logs or schedule, its date becomes the cut, so the default call never sees the game's own or any later line. |
| `home_team_id` | `str` |  | Home team id. |
| `away_team_id` | `str` |  | Away team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `as_of_date` | `Union[date, None]` | `None` | Only box lines and ratings games with `game_date < as_of_date` (strict) are used. `None` = the target game's date from the box logs, else from the schedule (one extra schedule read, only when the game is missing from the logs); a game in neither gets no cut and an `~sportsdataverse.errors.AsOfCutWarning`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | The box logs carry no `game_date` (the as-of cut cannot be applied). |

**Example**

```python
from sportsdataverse.nba.nba_player_props import nba_player_props
props = nba_player_props(2024, "401585828", "2", "6")

# Backtest as of a date

import datetime as dt
props = nba_player_props(2024, "401585828", "2", "6", as_of_date=dt.date(2024, 1, 15))
```

### nba_playtype_ratings {#nba_playtype_ratings}

`nba_playtype_ratings(season: 'str', *, league_id: 'str' = '00', off_team: 'Optional[pl.DataFrame]' = None, def_team: 'Optional[pl.DataFrame]' = None, schedule: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Season play-type-adjusted offensive/defensive team ratings.

Fetches (or uses injected) Synergy offensive/defensive team frames plus the
league schedule, computes raw per-type efficiency
(`raw_playtype_efficiency`), opponent-adjusts it
(`adjust_playtype_efficiency`), then rolls up to one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id` (Int64), `adj_off`/`adj_def`/`adj_net` (Float64) roll-ups, plus per-type wide columns `adj_off_ppp_<playtype>`/`adj_def_ppp_<playtype>`/`off_freq_<playtype>` (Float64) for each play type present in the data. `adj_off = Σ_t off_freq_t · adj_off_ppp_t · 100` (symmetric for `adj_def` off `def_freq_t`); `adj_net = adj_off - adj_def`. Returns a zero-row frame with the base roll-up schema when the upstream fetch is empty (sparse-coverage leagues never raise).

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_playtype_ratings
r = nba_playtype_ratings("2023-24")
print(r.sort("adj_off", descending=True).head(10))

# Injected offline (oracle / test) path

r = nba_playtype_ratings("2023-24", off_team=off_df, def_team=def_df, schedule=sched_df)

# Pipeline next step

r.filter(pl.col("adj_net") > 0).sort("adj_net", descending=True)
```

### nba_possession_rate_rows {#nba_possession_rate_rows}

`nba_possession_rate_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season') -> 'pl.DataFrame'`

Map `nba_stats_possessions` rows onto the team-season-rates possession frame.

Keeps the games of `schedule` whose `season_type` matches, takes the home team from the
schedule, and sets each possession's start clock to the previous possession's end clock in the
period (the period length for the first one). Points come from the made-shot counts.

`neutral_site` is always False: the stats schedule has no neutral-site or venue column, so
a neutral-site game (an NBA Cup semifinal in Las Vegas, a Mexico City or Paris game) counts
as a home game for the listed home team: at most five regular-season games a season.
ESPN's schedule `neutral_site` is not a usable substitute: it marks the 2023-24 Cup
semifinals False, and the ESPN-to-stats game id crosswalk starts at 2025-26.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_nba_stats_possessions` / `load_wnba_stats_possessions` rows. |
| `schedule` | `DataFrame` |  | `load_nba_stats_schedules` / `load_wnba_stats_schedules` rows (`game_id`, `season_type`, `home_team_id`). |
| `league_id` | `str` | `'00'` | `"00"` NBA (12-minute quarters) or `"10"` WNBA (10-minute quarters). |
| `season_type` | `str` | `'regular-season'` | The schedule `season_type` to keep: `"regular-season"` (the default, stats.nba.com's "Regular Season"; NBA Cup group and knockout games count, the Cup final does not), `"preseason"`, `"playoffs"` or `"play-in"`. |

**Returns**

One row per possession in `~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.nba.com game id. |
| `period` | Int64 | Period of the possession; overtimes follow the last regulation period. |
| `possession_number` | Int64 | Order of the possession in the game. |
| `late` | Boolean | True in the 4th quarter or an overtime: the periods the clutch window can fall in. |
| `start_clock` | Float64 | Seconds left in the period when the possession starts (when the previous possession ended). |
| `end_clock` | Float64 | Seconds left in the period when the possession ends. |
| `offense_team_id` | Int64 | stats.nba.com id of the team with the ball. |
| `defense_team_id` | Int64 | stats.nba.com id of the team on defense. |
| `offense_is_home` | Boolean | True when the offense is the (nominal) home team; it signs the running score for the clutch margin. |
| `neutral_site` | Boolean | Always False: the schedule names a home team for every NBA and WNBA game. |
| `points` | Int64 | Points the offense scored on the possession, from its made shots and free throws (2 * fg2m + 3 * fg3m + ftm). |
| `fga` | Float64 | Offense field-goal attempts on the possession. |
| `fgm` | Float64 | Offense field goals made on the possession. |
| `fg3m` | Float64 | Offense three-pointers made on the possession. |
| `fta` | Float64 | Offense free-throw attempts on the possession. |
| `oreb` | Float64 | Offense offensive rebounds on the possession. |
| `dreb` | Float64 | Defense defensive rebounds on the possession (the other half of the offensive-rebound chances). |
| `tov` | Float64 | Offense turnovers on the possession. |
| `ff_poss` | Float64 | Turnover-rate denominator: 1 per possession. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions, load_nba_stats_schedules
from sportsdataverse.nba.nba_team_season_rates import nba_possession_rate_rows
rows = nba_possession_rate_rows(load_nba_stats_possessions(2025), load_nba_stats_schedules(2025))
```

### nba_ratings_panel {#nba_ratings_panel}

`nba_ratings_panel(model: 'AnyModel', possessions: 'pl.DataFrame', dates: 'Optional[Sequence[datetime.date]]' = None, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Player-ratings-through-date long panel: one row per (player_id, date).

Refit-per-checkpoint (v1; no warm-start incrementality) — each date's row
calls `ratings_as_of` independently, so the panel is leakage-free by
construction: a possession dated after a given checkpoint can never affect
that checkpoint's row, no matter what other dates are also being computed
or what future rows exist in `possessions`. Cost is a full refit per
checkpoint date; for a season's sparse RAPM-family design this is seconds
per date, not minutes — acceptable for a nightly/daily cadence but not for
live in-game updating (out of scope; see spec non-goals).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel`. |
| `possessions` | `DataFrame` |  | A possession+lineup frame with a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `dates` | `Optional[Sequence[date]]` | `None` | Checkpoint dates to compute. `None` (default) uses every distinct `game_date` present in `possessions`, sorted ascending — a rating for every game day, matching what EPM/LEBRON publish nightly. Duplicates are deduped; input order does not matter (the output is always sorted by date). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Long frame with `RATINGS_PANEL_SCHEMA` columns (`player_id`, `date`, `o_rating`, `d_rating`, `rating`). Zero-row (that schema) when `possessions` is empty or no date yields any players.

No returns table is published for this function: no capture: its possessions input needs a game_date column, and neither nba_possessions nor the released possessions carry one.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `possessions` (non-empty) lacks a `game_date` column. |

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import nba_ratings_panel

checkpoints = [datetime.date(2023, 11, 1), datetime.date(2023, 12, 1)]
panel = nba_ratings_panel(RidgeRapmModel(), season_poss, dates=checkpoints)
print(panel.filter(pl.col("player_id") == 201939).sort("date"))

# Every game day, no explicit grid

panel = nba_ratings_panel(RidgeRapmModel(), season_poss)
```

### nba_raw_store_season_frame {#nba_raw_store_season_frame}

`nba_raw_store_season_frame(endpoint: 'str', season: 'int', variant: 'Optional[str]' = None, *, result_set: 'Optional[str]' = None, raw_store_dir: 'RawStoreDir' = None) -> "Optional['pl.DataFrame']"`

Read a committed SEASON-LEVEL capture from the raw store, parsed to a frame.

The per-game half of the store is served by the read-through per-game path;
this is the season-keyed half (`leaguegamelog`, `playerindex`,
`leaguedashplayerbiostats`, ...) that the `-raw` scraper writes as

* `{endpoint}/{season}/{variant}.json` -- parameterized captures, where
  *variant* is the slugified parameter sweep (e.g. `"regular-season"`,
  `"regular-season_totals"`), and
* `{endpoint}/{season}.json` -- unparameterized captures (e.g.
  `playerindex`), i.e. `variant=None`.

Roots may be a local checkout or an `http(s)://` base (the raw repo served
over raw.githubusercontent / a CDN), so a consumer -- notably the
`hoopR-nba-stats-data` model producer -- runs clone-free in CI against the
same committed tree the per-game compile reads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` |  | stats.nba.com endpoint slug (the store subdirectory). |
| `season` | `int` |  | Season **END** year (2024 = 2023-24) -- the key both halves of the store are filed under. |
| `variant` | `Optional[str]` | `None` | Capture variant slug, or `None` for an unparameterized capture. |
| `result_set` | `Optional[str]` | `None` | Named result set to return when the payload carries several; defaults to the first frame that parses. |
| `raw_store_dir` | `RawStoreDir` | `None` | Store root spec (dir or URL base) or per-endpoint mapping; `None` falls back to the env vars, `""` disables. |

**Returns**

The parsed `polars.DataFrame`, or `None` when the store is unset, the capture is absent, or the payload carries no usable frame -- so a caller can cleanly fall back to a live fetch.

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | Int64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |
| `video_available` | Int64 | Video available. |

**Example**

```python
from sportsdataverse.nba import nba_raw_store_season_frame
base = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
logs = nba_raw_store_season_frame("leaguegamelog", 2024, "regular-season", raw_store_dir=base)

# Fall back to a live fetch when the capture is absent

frame = nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=base)
positions = frame if frame is not None else nba_stats_playerindex(season="2023-24")
```

### nba_referee_assignments {#nba_referee_assignments}

`nba_referee_assignments(date: 'str | _dt.date', *, league: 'str' = 'nba', raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse NBA referee assignments for a given date from official.nba.com.

Retrieves one league's referee crew assignments and replay-center officials for a
date: NBA, G-League or WNBA, picked by `league` (`raw=True` returns all three
leagues' blocks). The `crew_position` column (1–4) represents the feed's slot
order; slot 1 is inferred to be the crew chief. The `season` column converts
from the feed's format to an END year: START+1 for NBA/G-League
(two-calendar-year seasons) and START unchanged for WNBA.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `str \| date` |  | The date to fetch assignments for (str in "YYYY-MM-DD" format or datetime.date). |
| `league` | `str` | `'nba'` | The league to extract ("nba", "gl", or "wnba"). Defaults to "nba". |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) with all three leagues instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict with keys "nba", "gl", "wnba". Otherwise, a dict with keys `"officials"` and `"replay_center"` mapping to DataFrames as documented in `parse_nba_referee_assignments`.

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
| `ValueError` | If league is not "nba", "gl", or "wnba", or date is not a valid "YYYY-MM-DD" date (checked before any request). |
| `NoDataError` | Official.nba.com answered 404, or 403 with S3's `AccessDenied` body. A date without games is not this: it comes back as a 200 with empty `rows` (see the Note). |
| `AssetFetchError` | The fetch failed (network error, rate limit, or Akamai WAF block), the 200 body is not a JSON object, or the league's `Table`/`Table1` rows are missing or malformed (a `Table` row without a 10-digit `game_id`, or a `Table1` row without a `replaycenter_official` name, included). With `raw=True` all three leagues are checked, since the whole payload is returned. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_referee_assignments
result = nba_referee_assignments("2026-06-13")
officials = result["officials"]
print(f"Found {officials.height} official slots")

# Fetch WNBA assignments for the same date

result = nba_referee_assignments("2026-06-13", league="wnba")
wnba_officials = result["officials"]
```

### nba_score_rows {#nba_score_rows}

`nba_score_rows(pbp: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season') -> 'pl.DataFrame'`

Map the stats.nba.com play-by-play onto the game-flow score rows.

Keeps the completed games of `schedule` whose `season_type` matches.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_nba_stats_pbp` / `load_wnba_stats_pbp` rows (`game_id`, `order_index`, `period`, `seconds_remaining`, `event_type`, `team_id`, `score_home`, `score_away`, `season`). |
| `schedule` | `DataFrame` |  | `load_nba_stats_schedules` / `load_wnba_stats_schedules` rows (`game_id`, `season_type`, `home_team_id`, `away_team_id`, `home_pts`). |
| `league_id` | `str` | `'00'` | `"00"` NBA (12-minute quarters) or `"10"` WNBA (10-minute quarters). |
| `season_type` | `str` | `'regular-season'` | The schedule `season_type` to keep (see `~sportsdataverse.nba.nba_team_season_rates.nba_possession_rate_rows`). |

**Returns**

Rows in `~sportsdataverse._common.game_flow.SCORE_ROWS_SCHEMA`, without `home_wp`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `order` | Int64 | The play's position in its game (the pbp order_index, or the release row order for college). |
| `home_team_id` | Int64 | The home team's id (stats.nba.com, or the ESPN team id for college; null for a non-D1 host). |
| `away_team_id` | Int64 | The away team's id (stats.nba.com, or the ESPN team id for college; null for a non-D1 visitor). |
| `neutral_site` | Boolean | True for a neutral-site game (no home-court prior); always False in the NBA / WNBA stats schedule, which carries no venue. |
| `elapsed` | Float64 | Game-clock seconds run when the play happens, counted from the tip. |
| `sec_left` | Float64 | The win-probability clock: seconds left in regulation, or in the overtime period. |
| `game_seconds` | Float64 | Game length in seconds: regulation plus the overtimes played. |
| `event_team_id` | Int64 | The play's team (the model's ball side); 0 or null for a team-less play. |
| `home_score` | Int64 | Home running score after the play. |
| `away_score` | Int64 | Away running score after the play. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` or `season_type` is unknown. |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_stats_pbp, load_nba_stats_schedules
from sportsdataverse.nba.nba_game_flow import nba_score_rows
rows = nba_score_rows(load_nba_stats_pbp(2025), load_nba_stats_schedules(2025))
```
