---
title: "NBA — additional Python functions — Analytics: nba_player–nba_season"
sidebar_label: "Analytics: nba_player–nba_season"
sidebar_position: 19
description: "NBA — additional Python functions — Analytics: nba_player–nba_season — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_player–nba_season

### nba_player_war {#nba_player_war}

`nba_player_war(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WAR and VORP per player-season, from our BPM, RAPM and BOBAN (regular season).

Each rating is anchored to the team (its players' possession-weighted mean rating over the
season is the team's net rating / 5; it is a season constraint, not an estimate for each
five-man lineup) and centred on the league, so 0 is a league-average player. `vorp` is points per
100 team possessions above a replacement player over one team's season of possessions
(Basketball-Reference's VORP scale); `war` is wins above replacement. A replacement-level
player has 0, and a season's `war` adds up to the league's wins minus those of a team of
replacement players. `rating` / `war` are the mean of the three sources; `rating_<s>`
/ `war_<s>` are each one. The replacement level and points per win are fitted per league
(`~sportsdataverse._common.player_war.REPLACEMENT_LEVEL`,
`~sportsdataverse._common.player_war.PTS_PER_WIN`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

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
| `pts_pg` | Float64 | Points per game he played (minutes over 0), from the regular-season stats game logs. |
| `reb_pg` | Float64 | Rebounds per game he played (minutes over 0), from the regular-season stats game logs. |
| `ast_pg` | Float64 | Assists per game he played (minutes over 0), from the regular-season stats game logs. |
| `pts_pg_rank` | Float64 | Rank of pts_pg among the season's players with at least the games gate (1 game); 1 = most, ties averaged. Null under ten ranked players. |
| `reb_pg_rank` | Float64 | Rank of reb_pg among the season's players with at least the games gate (1 game); 1 = most, ties averaged. Null under ten ranked players. |
| `ast_pg_rank` | Float64 | Rank of ast_pg among the season's players with at least the games gate (1 game); 1 = most, ties averaged. Null under ten ranked players. |
| `pts_pg_pct` | Float64 | Percentile of pts_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `reb_pg_pct` | Float64 | Percentile of reb_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `ast_pg_pct` | Float64 | Percentile of ast_pg_rank: 100 * (n + 1 - rank) / (n + 1), n the ranked players. |
| `pts_pg_n` | Int64 | Games behind pts_pg: the games he played (0 with no box line). |
| `reb_pg_n` | Int64 | Games behind reb_pg: the games he played (0 with no box line). |
| `ast_pg_n` | Int64 | Games behind ast_pg: the games he played (0 with no box line). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.nba.nba_player_war import nba_player_war
war = nba_player_war(2026)
war.select("player_name", "poss", "rating", "vorp", "war").head(10)

# WNBA via ``league_id``

wnba = nba_player_war(2025, league_id="10")
```

### nba_player_wowy {#nba_player_wowy}

`nba_player_wowy(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WOWY pairs: a team's season with two teammates together, apart and both off, per pair.

The definitions are in `sportsdataverse._common.player_on_off`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_player_on_off import nba_player_wowy
pairs = nba_player_wowy(2025)
pairs.filter(team_id=1610612738).head()
```

### nba_playoff_series {#nba_playoff_series}

`nba_playoff_series(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Every postseason game keyed to its series, with the series score after it.

A series is every postseason game between one pair of teams in a season; play-in games are
not a series. `series_id` is `"{season}-{lower ESPN team id}-{higher}"`. `team_a` is
game 1's home team. A series is `decided` once its leader has the wins to clinch
(NBA: four, every round since 2003; WNBA: the Finals only, two through 2004, three 2005-2024,
four from 2025) or plays a later series that postseason.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per postseason game (ESPN `season_type` 3): `season`, `series_id`, `round` (1 = first round .. 4 = Finals, from ESPN's `type_abbreviation`; null where ESPN gives none), `round_code`, `round_name` (the game headline without "Game N"), `game_number` (by tip-off), `game_id`, `game_date`, `completed`, the home and away `team_id` / `abbreviation` / `display_name` / `score`, `winner_team_id` (null until final), `team_a_id`, `team_b_id`, `team_a_wins` / `team_b_wins` (the series score after this game), and on every row of the series `series_games`, `decided` and `series_winner_team_id` (null until decided). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25; the WNBA season is its calendar year). |
| `series_id` | String | Series key: '{season}-{lower ESPN team id}-{higher ESPN team id}'. A pair of teams meets at most once per postseason, so the pair is the series. |
| `round` | Int64 | Round number from ESPN's type_abbreviation: 1 first round, 2 second round (NBA conference semifinals), 3 semifinals (NBA conference finals), 4 Finals; null where ESPN gives no round (older seasons). |
| `round_code` | String | ESPN's type_abbreviation for the round (RD16, QTR, SEMI, FINAL); null where it carries none. |
| `round_name` | String | The round as ESPN's game headline names it, without its 'Game N' (the latest game's headline in the series). |
| `game_number` | Int64 | Game number within the series, by tip-off. |
| `game_id` | Int64 | ESPN game id. |
| `game_date` | String | Game date (US). |
| `completed` | Boolean | True when the game is final. |
| `home_team_id` | Int64 | Home team's ESPN team id. |
| `home_abbreviation` | String | Home team's abbreviation. |
| `home_display_name` | String | Home team's display name. |
| `home_score` | Int64 | Home points (null before tip-off). |
| `away_team_id` | Int64 | Away team's ESPN team id. |
| `away_abbreviation` | String | Away team's abbreviation. |
| `away_display_name` | String | Away team's display name. |
| `away_score` | Int64 | Away points (null before tip-off). |
| `winner_team_id` | Int64 | The game winner's ESPN team id; null until the game is final. |
| `team_a_id` | Int64 | ESPN team id of game 1's home team (the side with home court). |
| `team_b_id` | Int64 | ESPN team id of the other side. |
| `team_a_wins` | Int64 | team_a's series wins after this game (finals up to and including it). |
| `team_b_wins` | Int64 | team_b's series wins after this game. |
| `series_games` | Int64 | Games listed in the series so far (played and scheduled); the same on every row of the series. |
| `decided` | Boolean | True once the series leader has the wins to clinch (NBA: four, every round since 2003; WNBA: the Finals only, two through 2004, three 2005-2024, four from 2025) or plays a later series that postseason. |
| `series_winner_team_id` | Int64 | The series winner's ESPN team id; null until decided. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` (checked before any load). |

**Example**

```python
from sportsdataverse.nba.nba_playoff_series import nba_playoff_series
games = nba_playoff_series(2025)
games.filter(games["round"] == 4).select("game_number", "home_score", "away_score", "team_a_wins", "team_b_wins")
```

### nba_playoff_series_players {#nba_playoff_series_players}

`nba_playoff_series_players(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Each player's totals and per-game line in each playoff series, against his regular season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per player who played (minutes above zero) in a final game of the series, keyed `(season, series_id, team_id, athlete_id)` with `athlete_display_name`: `games`, `min` and the summed counts as `nba_playoff_series_teams`; `<stat>_pg` per game for `min`, `pts`, `reb`, `ast`, `stl`, `blk`, `tov` and `fg3m`, plus `ts_pct` (`PTS / (2 * (FGA + 0.44 * FTA))`); `reg_games` and `reg_<stat>_pg` / `reg_ts_pct` over his regular season (every team he played for; null with none); and `<stat>_pg_diff` / `ts_pct_diff`, series minus regular season.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25; the WNBA season is its calendar year). |
| `series_id` | String | Series key: '{season}-{lower ESPN team id}-{higher ESPN team id}'. A pair of teams meets at most once per postseason, so the pair is the series. |
| `team_id` | Int64 | ESPN team id of the player's team in the series. |
| `athlete_id` | Int64 | ESPN athlete id. |
| `athlete_display_name` | String | The player's name. |
| `games` | Int64 | Series games he played (minutes above zero). |
| `min` | Float64 | Total minutes in the series. |
| `pts` | Int64 | Total points in the series: the sum of his ESPN game boxes. |
| `fgm` | Int64 | Total field goals made in the series: the sum of his ESPN game boxes. |
| `fga` | Int64 | Total field goals attempted in the series: the sum of his ESPN game boxes. |
| `fg3m` | Int64 | Total three-pointers made in the series: the sum of his ESPN game boxes. |
| `fg3a` | Int64 | Total three-pointers attempted in the series: the sum of his ESPN game boxes. |
| `ftm` | Int64 | Total free throws made in the series: the sum of his ESPN game boxes. |
| `fta` | Int64 | Total free throws attempted in the series: the sum of his ESPN game boxes. |
| `oreb` | Int64 | Total offensive rebounds in the series: the sum of his ESPN game boxes. |
| `dreb` | Int64 | Total defensive rebounds in the series: the sum of his ESPN game boxes. |
| `reb` | Int64 | Total total rebounds in the series: the sum of his ESPN game boxes. |
| `ast` | Int64 | Total assists in the series: the sum of his ESPN game boxes. |
| `stl` | Int64 | Total steals in the series: the sum of his ESPN game boxes. |
| `blk` | Int64 | Total blocks in the series: the sum of his ESPN game boxes. |
| `tov` | Int64 | Total turnovers in the series: the sum of his ESPN game boxes. |
| `pf` | Int64 | Total personal fouls in the series: the sum of his ESPN game boxes. |
| `min_pg` | Float64 | Minutes per series game played. |
| `pts_pg` | Float64 | Points per series game played. |
| `reb_pg` | Float64 | Total rebounds per series game played. |
| `ast_pg` | Float64 | Assists per series game played. |
| `stl_pg` | Float64 | Steals per series game played. |
| `blk_pg` | Float64 | Blocks per series game played. |
| `tov_pg` | Float64 | Turnovers per series game played. |
| `fg3m_pg` | Float64 | Three-pointers made per series game played. |
| `ts_pct` | Float64 | True shooting in the series: PTS / (2 * (FGA + 0.44 * FTA)); null with no attempts. |
| `reg_games` | Int64 | Regular-season games he played (every team he played for that season); null with none. |
| `reg_min_pg` | Float64 | Minutes per regular-season game played. |
| `reg_pts_pg` | Float64 | Points per regular-season game played. |
| `reg_reb_pg` | Float64 | Total rebounds per regular-season game played. |
| `reg_ast_pg` | Float64 | Assists per regular-season game played. |
| `reg_stl_pg` | Float64 | Steals per regular-season game played. |
| `reg_blk_pg` | Float64 | Blocks per regular-season game played. |
| `reg_tov_pg` | Float64 | Turnovers per regular-season game played. |
| `reg_fg3m_pg` | Float64 | Three-pointers made per regular-season game played. |
| `reg_ts_pct` | Float64 | True shooting over his regular season. |
| `min_pg_diff` | Float64 | min_pg - reg_min_pg: the series against his regular season (positive is up). |
| `pts_pg_diff` | Float64 | pts_pg - reg_pts_pg: the series against his regular season (positive is up). |
| `reb_pg_diff` | Float64 | reb_pg - reg_reb_pg: the series against his regular season (positive is up). |
| `ast_pg_diff` | Float64 | ast_pg - reg_ast_pg: the series against his regular season (positive is up). |
| `stl_pg_diff` | Float64 | stl_pg - reg_stl_pg: the series against his regular season (positive is up). |
| `blk_pg_diff` | Float64 | blk_pg - reg_blk_pg: the series against his regular season (positive is up). |
| `tov_pg_diff` | Float64 | tov_pg - reg_tov_pg: the series against his regular season (positive is up). |
| `fg3m_pg_diff` | Float64 | fg3m_pg - reg_fg3m_pg: the series against his regular season (positive is up). |
| `ts_pct_diff` | Float64 | ts_pct - reg_ts_pct. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_playoff_series import nba_playoff_series_players
p = nba_playoff_series_players(2025)
p.filter(p["series_id"] == "2025-11-25").sort("pts_pg_diff", descending=True).head()
```

### nba_playoff_series_teams {#nba_playoff_series_teams}

`nba_playoff_series_teams(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Each team's record and box totals in each playoff series: the sum of its game boxes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

Two rows per series with a final game, keyed `(season, series_id, team_id)` with `opponent_team_id` and `team_abbreviation`: `games`, `wins`, `losses`, `pts`, `opp_pts` and the summed counts `fgm`, `fga`, `fg3m`, `fg3a`, `ftm`, `fta`, `oreb`, `dreb`, `reb`, `ast`, `stl`, `blk`, `tov`, `pf` over the series' final games.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25; the WNBA season is its calendar year). |
| `series_id` | String | Series key: '{season}-{lower ESPN team id}-{higher ESPN team id}'. A pair of teams meets at most once per postseason, so the pair is the series. |
| `team_id` | Int64 | ESPN team id. |
| `opponent_team_id` | Int64 | The opponent's ESPN team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `games` | Int64 | Final games in the series. |
| `wins` | Int64 | Series wins. |
| `losses` | Int64 | Series losses. |
| `opp_pts` | Int64 | Opponent points over the series' final games. |
| `pts` | Int64 | Total points over the series' final games: the sum of the team's ESPN game boxes. |
| `fgm` | Int64 | Total field goals made over the series' final games: the sum of the team's ESPN game boxes. |
| `fga` | Int64 | Total field goals attempted over the series' final games: the sum of the team's ESPN game boxes. |
| `fg3m` | Int64 | Total three-pointers made over the series' final games: the sum of the team's ESPN game boxes. |
| `fg3a` | Int64 | Total three-pointers attempted over the series' final games: the sum of the team's ESPN game boxes. |
| `ftm` | Int64 | Total free throws made over the series' final games: the sum of the team's ESPN game boxes. |
| `fta` | Int64 | Total free throws attempted over the series' final games: the sum of the team's ESPN game boxes. |
| `oreb` | Int64 | Total offensive rebounds over the series' final games: the sum of the team's ESPN game boxes. |
| `dreb` | Int64 | Total defensive rebounds over the series' final games: the sum of the team's ESPN game boxes. |
| `reb` | Int64 | Total total rebounds over the series' final games: the sum of the team's ESPN game boxes. |
| `ast` | Int64 | Total assists over the series' final games: the sum of the team's ESPN game boxes. |
| `stl` | Int64 | Total steals over the series' final games: the sum of the team's ESPN game boxes. |
| `blk` | Int64 | Total blocks over the series' final games: the sum of the team's ESPN game boxes. |
| `tov` | Int64 | Total turnovers over the series' final games: the sum of the team's ESPN game boxes. |
| `pf` | Int64 | Total personal fouls over the series' final games: the sum of the team's ESPN game boxes. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_playoff_series import nba_playoff_series_teams
nba_playoff_series_teams(2025).sort("pts", descending=True).head()
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
period (the period length for the first one). Points come from the made-shot counts. A game
whose first period runs past 15 minutes is played in two 20-minute halves (the WNBA through
2005): its periods are 20 minutes, `late` starts with the 2nd half and period 3 is overtime.

`neutral_site` comes from `sportsdataverse.nba.nba_neutral_site`: the 2020 bubbles and
the curated games abroad and NBA Cup knockout games in Las Vegas, by stats `game_id`. The
stats schedule has no neutral-site or venue column, and ESPN's `neutral_site` misses most of
them (it marks the 2023-24 Cup semifinals False).

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
| `late` | Boolean | True in the last regulation period (the 4th quarter; the 2nd half of a two-halves game) or an overtime: the periods the clutch window can fall in. |
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
| `ftm` | Float64 | Offense free throws made on the possession. |
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

### nba_pregame_projections {#nba_pregame_projections}

`nba_pregame_projections(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Rated pregame projection for every ESPN game of an NBA / WNBA season, as of each game's date.

The number the game page's pregame card prints: the closed form on each team's
`team_strength` before the game; see the module docstring for the model and the as-of
boundary.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2026` is 2025-26); the WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per game of the season in the ESPN schedule release (scheduled games included; a row the release labels with another season is dropped): `season`, `game_id`, `game_date`, `as_of_date` (the ratings use regular-season possessions dated strictly before it), `home_team_id` / `away_team_id` (ESPN ids, Int32), `neutral_site`, `stats_game_id`, each side's `rating` (`adj_net_rtg`, per 100), `rank` (`adj_net_rtg_rank`), `games` and `pace`, `hfa` (points, 0 at a neutral site), `exp_home_margin` (points) and `rated_home_prob`. The ratings and prediction are null for a game with no stats match or a side with no rating yet. Then the in-game WP prior (BB-D25, see the module docstring): `wp_prior_home_rating` / `wp_prior_away_rating` (per 100) and their ranks, `wp_prior_poss`, `wp_prior_hfa` (points, 0 at a neutral site), `wp_prior_margin_sd`, `wp_prior_exp_home_margin` and `wp_prior_home_prob`; null for a game with no stats match. The ranks are also null before any team is rated (opening day of a season with no prior season released).

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season, named by the year it ends (2027 is 2026-27; the WNBA season is its calendar year). |
| `game_id` | Int32 | ESPN game id (Int32). |
| `game_date` | Date | Game date (ESPN schedule). |
| `as_of_date` | Date | The ratings use the completed games dated strictly before this date: the game's date, or the day after the last completed game for a game not yet reached. |
| `home_team_id` | Int32 | ESPN home team id (Int32). |
| `away_team_id` | Int32 | ESPN away team id (Int32). |
| `neutral_site` | Boolean | Neutral-site flag; a neutral game gets no home term. |
| `stats_game_id` | String | The stats.nba.com / stats.wnba.com game id matched on date and team names; null for a game with no match (All-Star games, the WNBA Commissioner's Cup final). |
| `home_rating` | Float64 | Home team's team_strength adj_net_rtg before the game: the possession ridge over the regular-season possessions dated before as_of_date, points per 100 over an average opponent on a neutral court. |
| `away_rating` | Float64 | Away team's adj_net_rtg before the game, on the same basis. |
| `home_rank` | Float64 | Home team's adj_net_rtg_rank as of the date (team_strength's rank: 1 = best, among teams with at least half the most games). |
| `away_rank` | Float64 | Away team's adj_net_rtg_rank as of the date. |
| `home_games` | Int64 | Home team's regular-season games behind its rating. |
| `away_games` | Int64 | Away team's games behind its rating. |
| `home_pace` | Float64 | Home team's team_strength pace (possessions per 48 minutes NBA, 40 WNBA) as of the date. |
| `away_pace` | Float64 | Away team's pace as of the date. |
| `hfa` | Float64 | Home-court term in points (the league's fitted hfa: 2.184 NBA, 1.184 WNBA), 0 at a neutral site. |
| `exp_home_margin` | Float64 | Expected home margin in points, the producer's closed form on the two ratings (positive favours the home team). |
| `rated_home_prob` | Float64 | Pregame home win probability, Phi(exp_home_margin / margin_sd) with the league's fitted margin_sd; null when a side has no rating. |
| `wp_prior_home_rating` | Float64 | In-game WP prior (BB-D25, nba_rated_priors): the home team's adj_net_rtg in the season sim's game model before the game, points per 100, shrunk toward last season's regressed rating (0 = average, which an unrated team is). |
| `wp_prior_away_rating` | Float64 | In-game WP prior: the away team's game-model adj_net_rtg before the game, on the same basis. |
| `wp_prior_home_rank` | Float64 | Home team's rank by wp_prior_home_rating among the game model's teams as of the date (1 = best; tied teams share the better, smaller rank); null before any team is rated (opening day with no prior season). |
| `wp_prior_away_rank` | Float64 | Away team's rank by wp_prior_away_rating as of the date. |
| `wp_prior_poss` | Float64 | In-game WP prior: the game model's possessions per team per game as of the date (last season's before any game; 100 on opening day when no previous season is released). |
| `wp_prior_hfa` | Float64 | In-game WP prior: the game model's home term in points, 2 x its per-100 home term x wp_prior_poss / 100; 0 at a neutral site. It is refit every day, so it is noisy in a season's first days. |
| `wp_prior_margin_sd` | Float64 | In-game WP prior: the probit scale of a game's margin in points (the season sim's). |
| `wp_prior_exp_home_margin` | Float64 | In-game WP prior's expected home margin in points: (wp_prior_home_rating - wp_prior_away_rating) x wp_prior_poss / 100 + wp_prior_hfa. |
| `wp_prior_home_prob` | Float64 | The pregame home win probability the in-game win-probability booster starts from when there is no line (BB-D25): Phi(wp_prior_exp_home_margin / wp_prior_margin_sd). Null only for a game with no stats match. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_pregame_projection import nba_pregame_projections
out = nba_pregame_projections(2026)
out.select("game_id", "game_date", "exp_home_margin", "rated_home_prob").head()

# WNBA

nba_pregame_projections(2025, league_id="10")
```

### nba_rated_priors {#nba_rated_priors}

`nba_rated_priors(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Rated pregame home win probability for every ESPN game of a season, as of each game's date.

The in-game win-probability prior when a game has no betting line
(`~sportsdataverse.nba.nba_game_page.home_prior`'s `rated_home_prob`); see the module
docstring for the model and the as-of boundary.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2026` is 2025-26); the WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per game of the season in the ESPN schedule release (scheduled games included; a row the release labels with another season is dropped): `season`, `game_id`, `game_date`, `home_team_id`, `away_team_id` (ESPN ids, Int32, as in the pbp release), `neutral_site`, `stats_game_id`, the two teams' adjusted net ratings before the game (`home_adj_net_rtg`, `away_adj_net_rtg`, per 100), the home term `hfa` (per 100, applied to both offenses), `exp_home_margin` (points) and `rated_home_prob`. The last six are null for a game with no stats.nba.com match (All-Star games and the WNBA Commissioner's Cup final).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | Int64 | ESPN game id (Int32, as in the pbp release). |
| `game_date` | Date | Game date (ESPN schedule); the ratings use the games dated strictly before it. |
| `home_team_id` | Int64 | ESPN home team id. |
| `away_team_id` | Int64 | ESPN away team id. |
| `neutral_site` | Boolean | ESPN's neutral-site flag; a neutral game gets no home term. |
| `stats_game_id` | String | The stats.nba.com / stats.wnba.com game id matched on date and team names; null for a game with no match (All-Star games). |
| `home_adj_net_rtg` | Float64 | Home team's adjusted net rating before the game, points per 100 over an average opponent on a neutral court (the season sim's as-of possession ridge, shrunk toward last season's regressed rating). |
| `away_adj_net_rtg` | Float64 | Away team's adjusted net rating before the game, points per 100, on the same basis. |
| `hfa` | Float64 | The as-of ridge's home term, points per 100 for the home offense over a neutral court (applied to both offenses). |
| `exp_home_margin` | Float64 | Expected home margin in points: (home_adj_net_rtg - away_adj_net_rtg + 2 * hfa) * possessions per game / 100, without the home term at a neutral site. |
| `rated_home_prob` | Float64 | Rated pregame home win probability: Phi(exp_home_margin / margin_sd) with the season sim's probit scale. Pass it to score_win_prob / game_page_tables as rated_home_prob to make it the prior of a game with no line; without it that game gets home-court advantage (0.5 at a neutral site). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_rated_prior import nba_rated_priors
priors = nba_rated_priors(2026)
priors.select("game_id", "game_date", "exp_home_margin", "rated_home_prob").head()

# WNBA

nba_rated_priors(2025, league_id="10")
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

### nba_season_sim {#nba_season_sim}

`nba_season_sim(season: 'int', *, as_of_date: 'dt.date | None' = None, league_id: 'str' = '00', n_sims: 'int' = 10000, seed: 'int' = 20261103, chances: 'list[float] | None' = None, cup_schedule: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season sim as of a date: projected W-L, division, seed, play-in, playoff and lottery odds.

Reads the `{nba,wnba}_stats_schedules` and `_possessions` releases for the season and the
one before, fits the game model on the regular-season games played before `as_of_date`,
shrunk toward last season's final ratings (`as_of_game_model`, `preseason_prior`),
and simulates the rest (`simulate_pro_season`). Before the first game the ratings are
the prior alone. The rules applied to each simulated season
are in `sportsdataverse.nba.nba_season_rules`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2027` is 2026-27); the WNBA season is its calendar year. |
| `as_of_date` | `date \| None` | `None` | Games on or after this date are simulated even when the release has their score (a backtest). None: every game with a score counts as played. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `n_sims` | `int` | `10000` | Simulated seasons (Monte Carlo error: see `SIM_SCHEMA`'s `_se` columns). |
| `seed` | `int` | `20261103` | Seeds the generator; the same release data and seed give the same output. |
| `chances` | `list[float] \| None` | `None` | Lottery chances per slot, worst record first, one per lottery team; overrides the published table (needed for the 2027 WNBA lottery's seven teams, not published yet). |
| `cup_schedule` | `DataFrame \| None` | `None` | NBA: stats.nba.com's schedule for the season (`~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2`), which marks the Cup games. Read only while the release lists TBD Cup games; None fetches it then (a residential connection), and a failed fetch falls back to league-average opponents for the Cup slots with a warning. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
import datetime as dt
from sportsdataverse.nba.nba_season_sim import nba_season_sim
sim = nba_season_sim(2026, as_of_date=dt.date(2026, 2, 13))
sim.select("team_id", "wins", "proj_wins", "p_playoffs", "p_pick_1").head()

# WNBA via ``league_id``

nba_season_sim(2025, as_of_date=dt.date(2025, 7, 1), league_id="10")
```
