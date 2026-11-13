---
title: "WNBA — additional Python functions — Analytics: wnba_player–wnba_team"
sidebar_label: "Analytics: wnba_player–wnba_team"
sidebar_position: 9
description: "WNBA — additional Python functions — Analytics: wnba_player–wnba_team — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Analytics: wnba_player–wnba_team

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

### wnba_situational_lineup {#wnba_situational_lineup}

`wnba_situational_lineup(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA five-man unit splits by situation (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_lineup.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `top_n` | `int \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, lineup_id, family, split)`: `lineup_id` is the five player ids sorted and joined with `-` (BB-D4's id), `player_1..5` the same ids, then the columns of `nba_situational_team`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `lineup_id` | String | The five player ids of the unit on the floor as the possession starts, sorted and joined with '-' (BB-D4's id). |
| `player_1` | Int64 | Player 1 of the unit, in lineup_id order. |
| `player_2` | Int64 | Player 2 of the unit, in lineup_id order. |
| `player_3` | Int64 | Player 3 of the unit, in lineup_id order. |
| `player_4` | Int64 | Player 4 of the unit, in lineup_id order. |
| `player_5` | Int64 | Player 5 of the unit, in lineup_id order. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

### wnba_situational_player {#wnba_situational_player}

`wnba_situational_player(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA player on-floor splits by situation (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_player.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `top_n` | `int \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, player_id, family, split)`, the columns of `nba_situational_team`: the team's possessions, points and ratings with the player on the floor as the possession starts. `player_id` is the stats.nba.com person id (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id` | Int64 | stats.nba.com person id. A possession counts for the five on the floor as it starts, with all its points. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

### wnba_situational_team {#wnba_situational_team}

`wnba_situational_team(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA team splits by situation (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `top_n` | `int \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, family, split)`: `games`, `minutes` / `seconds`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions) and their `_n`. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

### wnba_situational_team_games {#wnba_situational_team_games}

`wnba_situational_team_games(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA team splits by situation, one game at a time (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_team_games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `top_n` | `int \| None` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, game_id, family, split)`, the columns of `nba_situational_team` plus `game_id` (the stats.nba.com game id); `games` is 1. A split the team did not play in the game has no row. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `game_id` | String | stats.wnba.com game id. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | 1: the row is one game (summed over a team-season's rows it is the season row's games). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

### wnba_team_clutch {#wnba_team_clutch}

`wnba_team_clutch(season: 'int', *, league_id: 'str' = '10', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

WNBA clutch skill (league_id='10'). See sportsdataverse.nba.nba_clutch.nba_team_clutch.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  |  |
| `league_id` | `str` | `'10'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per team: `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_skill_shrunk, clutch_poss`. Empty input returns that schema with zero rows.

No returns table is published for this function: no capture: it reads stats.wnba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

### wnba_team_game_possessions {#wnba_team_game_possessions}

`wnba_team_game_possessions(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA per team and game possession counts (league_id='10'). See sportsdataverse.nba.nba_team_season_rates.nba_team_game_possessions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id, game_id, split)`: a game has an `all` row, a `home` or `away` row (neither at a neutral site) and, when it has clutch possessions, `close` and `clutch` rows. `team_id` is the stats.nba.com team id (Int64), `game_id` the stats.nba.com game id. `poss` / `opp_poss` are the team's offensive / defensive possessions, `pts` / `opp_pts` their points and `seconds` the game clock they used. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | WNBA season (its calendar year). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `game_id` | String | stats.wnba.com game id. The game this row's possessions are from. |
| `split` | String | The season table's split this game is in: all (every game); home or away (neither at a neutral site); close and clutch when the game has clutch possessions. A team-season's rows of a split add up to its wnba_team_season_rates row. |
| `poss` | Int64 | Offensive possessions in the split, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the split (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the split. Counted from each possession's made shots and free throws. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the split. |
| `seconds` | Float64 | Game-clock seconds the team's possessions in the split used; pace over a set of games is regulation minutes x 60 x (poss + opp_poss) / 2 / summed seconds. |

### wnba_team_luck {#wnba_team_luck}

`wnba_team_luck(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA luck and close-game record: Pythagorean expected wins vs actual, close-game W-L (league_id='10'). See sportsdataverse.nba.nba_team_luck.nba_team_luck.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id)`; `team_id` is the stats.nba.com team id (Int64). `games`, `wins`, `losses`, `pts` and `opp_pts` are from the final scores; `win_pct`, `pythag_win_pct`, `exp_wins` and `luck_wins` (`wins - exp_wins`) follow. `pbp_games` counts the games with published possessions, and `close_games`, `close_wins`, `close_losses` and `close_win_pct` (null with no close game) are the close games among them. `pythag_win_pct`, `luck_wins` and `close_win_pct` have `<m>_rank` (1 = highest), `<m>_pct` (the Weibull percentile) and `<m>_n` (games, or close games); ranks are within the season, among teams with at least half the most games (`close_win_pct` also needs possessions for half of them and five close games), and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, the calendar year. |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `games` | Int64 | Games in the season type's completed games (the stats schedule's final scores). |
| `wins` | Int64 | Wins in the season type's completed games (the stats schedule's final scores). |
| `losses` | Int64 | Losses in the season type's completed games (the stats schedule's final scores). |
| `pts` | Int64 | Points scored in those games (final scores). |
| `opp_pts` | Int64 | Points allowed in those games (final scores). |
| `win_pct` | Float64 | Win fraction: wins divided by games, from the final scores. |
| `pythag_win_pct` | Float64 | Pythagorean win fraction: pts^x / (pts^x + opp_pts^x), x = PYTHAG_EXPONENT['wnba'], fitted on past seasons by tools/fit_team_luck_pythag.py. |
| `exp_wins` | Float64 | Expected wins: pythag_win_pct * games. |
| `luck_wins` | Float64 | Luck in wins: wins minus exp_wins. Positive means the team won more games than its scoring says it should have. |
| `pbp_games` | Int64 | Games with published possessions (wnba_stats_possessions); close_games is counted among these. A game whose late possessions are missing from the release counts as not close. |
| `close_games` | Int64 | Close games: at least one possession in play with 5:00 or less left (starting there, or starting before 5:00 and ending after it) in the 4th quarter (the 2nd half through 2005, when the WNBA played two 20-minute halves) or an overtime, the score within 5 as that possession starts (stats.nba.com's clutch window; BB-D3's close split). The margin is the running sum of the possessions' points, so it can drift from the scoreboard: NBA 2025-26 agrees with an independent ESPN play-by-play recount on 1215 of 1229 games. |
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

### wnba_team_season_flow {#wnba_team_season_flow}

`wnba_team_season_flow(seasons: 'int | list[int]', *, league_id: 'str' = '10', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WNBA game flow per team and season, with ranks (league_id='10'). See sportsdataverse.nba.nba_game_flow.nba_team_season_flow.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  |  |
| `league_id` | `str` | `'10'` |  |
| `season_type` | `str` | `'regular-season'` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

One row per `(season, team_id)`: `games`, `wins`; `lead_pct`, `trail_pct`, `tied_pct`, `avg_margin` and `avg_wp` (weighted by game clock); `lead_changes_pg` and `times_tied_pg`; `largest_lead`, `largest_comeback` and `max_run` (season bests); `runs_10` / `opp_runs_10` and their per-game rates; `avg_longest_drought_sec`; `def_poss`, `stops`, `stop_pct`; `kills`, `double_kills`, `kills_pg`, `double_kills_pg` and `kills_per_100`. The ten rated metrics carry `<m>_rank` (1 = best; `opp_runs_10_pg` and `avg_longest_drought_sec` rank low first), `<m>_pct` and `<m>_n` (games; the games with possessions for `kills_pg` / `double_kills_pg` and with a win probability for `avg_wp`; defensive possessions for `stop_pct` and `kills_per_100`), among teams with at least half the season's most games, null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.wnba.com team id. |
| `games` | Int64 | Games played (completed games of the season type). |
| `wins` | Int64 | Games the team won among its completed games of the season type (pts > opp_pts). |
| `trail_pct` | Float64 | Share of the team's game clock spent behind (sum trail_sec / sum game_seconds). |
| `tied_pct` | Float64 | Share of the team's game clock spent tied. |
| `lead_changes_pg` | Float64 | Lead changes per game. |
| `times_tied_pg` | Float64 | Times tied per game. |
| `largest_lead` | Int64 | The team's biggest lead of the season. |
| `largest_comeback` | Int64 | The biggest deficit the team overcame in a win this season (0 if none). |
| `max_run` | Int64 | The team's biggest n-0 run of the season. |
| `runs_10` | Int64 | The team's runs of 10 or more unanswered points, summed over the season. |
| `opp_runs_10` | Int64 | The opponents' runs of 10 or more unanswered points against the team, summed. |
| `def_poss` | Int64 | Defensive possessions, summed. |
| `stops` | Int64 | Stops (scoreless defensive possessions), summed. |
| `kills` | Int64 | Kills (three straight stops), summed. |
| `double_kills` | Int64 | Double kills (six straight stops), summed. |
| `avg_margin` | Float64 | Game control: the score margin averaged over the season's game clock. |
| `avg_wp` | Float64 | Game control: the in-game win probability averaged over the season's game clock. |
| `lead_pct` | Float64 | Share of the team's game clock spent ahead (sum lead_sec / sum game_seconds). |
| `runs_10_pg` | Float64 | The team's 10-0 (or better) runs per game. |
| `opp_runs_10_pg` | Float64 | The opponents' 10-0 (or better) runs against the team per game. |
| `avg_longest_drought_sec` | Float64 | The team's longest scoring drought, in seconds, averaged over its games. |
| `stop_pct` | Float64 | stops / def_poss; null with no possessions. |
| `kills_pg` | Float64 | Kills per game. |
| `double_kills_pg` | Float64 | Double kills per game. |
| `kills_per_100` | Float64 | Kills per 100 defensive possessions; null with no possessions. |
| `avg_margin_rank` | Float64 | Rank of avg_margin within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_wp_rank` | Float64 | Rank of avg_wp within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `lead_pct_rank` | Float64 | Rank of lead_pct within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `runs_10_pg_rank` | Float64 | Rank of runs_10_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `opp_runs_10_pg_rank` | Float64 | Rank of opp_runs_10_pg within the season (the lowest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_longest_drought_sec_rank` | Float64 | Rank of avg_longest_drought_sec within the season (the lowest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `stop_pct_rank` | Float64 | Rank of stop_pct within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `kills_pg_rank` | Float64 | Rank of kills_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `double_kills_pg_rank` | Float64 | Rank of double_kills_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `kills_per_100_rank` | Float64 | Rank of kills_per_100 within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_margin_pct` | Float64 | Percentile of avg_margin among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_wp_pct` | Float64 | Percentile of avg_wp among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `lead_pct_pct` | Float64 | Percentile of lead_pct among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `runs_10_pg_pct` | Float64 | Percentile of runs_10_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_runs_10_pg_pct` | Float64 | Percentile of opp_runs_10_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_longest_drought_sec_pct` | Float64 | Percentile of avg_longest_drought_sec among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `stop_pct_pct` | Float64 | Percentile of stop_pct among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `kills_pg_pct` | Float64 | Percentile of kills_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `double_kills_pg_pct` | Float64 | Percentile of double_kills_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `kills_per_100_pct` | Float64 | Percentile of kills_per_100 among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_margin_n` | Int64 | Sample size of avg_margin: games. |
| `avg_wp_n` | Int64 | Sample size of avg_wp: the games with a win probability. |
| `lead_pct_n` | Int64 | Sample size of lead_pct: games. |
| `runs_10_pg_n` | Int64 | Sample size of runs_10_pg: games. |
| `opp_runs_10_pg_n` | Int64 | Sample size of opp_runs_10_pg: games. |
| `avg_longest_drought_sec_n` | Int64 | Sample size of avg_longest_drought_sec: games. |
| `stop_pct_n` | Int64 | Sample size of stop_pct: the teams defensive possessions. |
| `kills_pg_n` | Int64 | Sample size of kills_pg: the games with published possessions. |
| `double_kills_pg_n` | Int64 | Sample size of double_kills_pg: the games with published possessions. |
| `kills_per_100_n` | Int64 | Sample size of kills_per_100: the teams defensive possessions. |
| `kills_per75` | Float64 | Kills per 75 defensive possessions: 0.75 * kills_per_100 (BB-G8's per-75 basis); null with no possessions. |
| `kills_per75_rank` | Float64 | Rank of kills_per75: kills_per_100_rank, copied (scaling by 0.75 keeps the order). |
| `kills_per75_pct` | Float64 | Percentile of kills_per75: kills_per_100_pct, copied (scaling by 0.75 keeps the order). |
| `kills_per75_n` | Int64 | Sample size of kills_per75: kills_per_100_n, copied. |
