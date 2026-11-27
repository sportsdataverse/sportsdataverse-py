# Men's college basketball — additional Python functions — Analytics: mbb_tournament–ncaa_team

> Men's college basketball — additional Python functions — Analytics: mbb_tournament–ncaa_team — function reference in sdv-py, the SportsDataverse Python package.

### mbb_tournament_ineligible {#mbb_tournament_ineligible}

`mbb_tournament_ineligible(seasons: 'list[int] | None' = None, *, league: 'str | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Division I basketball programs that could not play in the NCAA tournament, per season.

A bundled, hand-curated list (2010-2027): programs reclassifying to Division I (four seasons
from Division II until 2025, three since the NCAA's January 2025 rule; St. Thomas came from
Division III), and APR, NCAA-infractions and self-imposed postseason bans. Every row cites
its source. The college season products (bracketology, season sim, strength of schedule) use
it by default to set `eligible`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int] \| None` | `None` | Seasons to keep, as the end year (`2024` is 2023-24). `None` keeps all. |
| `league` | `str \| None` | `None` | `"mens"`, `"womens"`, or `None` for both. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per ineligible `(season, league, team_id)`: `season, league, team_id` (ESPN team id, Utf8), `team`, `reason` (`reclassifying`, `apr_ban`, `infractions_ban`, `self_imposed_ban` or `other`), `detail`, `source_url` and `confidence` (`high` / `medium`).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, as the end year (2024 is 2023-24). |
| `league` | String | mens or womens. |
| `team_id` | String | ESPN team id (Utf8). |
| `team` | String | School name of the barred program, as ESPN spells it. |
| `reason` | String | Why the program was barred: reclassifying, apr_ban, infractions_ban, self_imposed_ban or other. |
| `detail` | String | Short note on the row, such as the reclassification year or the case behind the ban. |
| `source_url` | String | The source that documents the row. |
| `confidence` | String | How well the source documents the row: high (the NCAA, the school or the conference says so) or medium (a secondary source). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"`, `"womens"` or `None`. |

**Example**

```python
from sportsdataverse.mbb import mbb_tournament_ineligible
barred = mbb_tournament_ineligible([2023], league="mens")

# Pipeline next step (one line)

barred.filter(pl.col("reason") == "reclassifying").get_column("team").to_list()
```

### mbb_tournament_run_players {#mbb_tournament_run_players}

`mbb_tournament_run_players(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Each player's totals and per-game line over a run, against the player's regular season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per player who played in a final game of the run, keyed `(season, run_id, team_id, athlete_id)`: the columns of `sportsdataverse.nba.nba_playoff_series.nba_playoff_series_players` (totals, `<stat>_pg`, `ts_pct`, `reg_games` / `reg_<stat>_pg` / `reg_ts_pct` and the shift `<stat>_pg_diff` / `ts_pct_diff`). The regular season leaves out the conference tournament games.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `run_id` | String | Run key: '{season}-{tournament_key}-{ESPN team id}', one team's games in one tournament. |
| `team_id` | Int64 | ESPN team id of the player's team in the run. |
| `athlete_id` | Int64 | ESPN athlete id (joins the ESPN rosters and player box scores). |
| `athlete_display_name` | String | The player's name. |
| `games` | Int64 | Run games played (not marked did-not-play, with minutes or any box count). |
| `min` | Float64 | Total minutes in the run. |
| `pts` | Int64 | Total points in the run: the sum of the player's ESPN game boxes. |
| `fgm` | Int64 | Total field goals made in the run: the sum of the player's ESPN game boxes. |
| `fga` | Int64 | Total field goals attempted in the run: the sum of the player's ESPN game boxes. |
| `fg3m` | Int64 | Total three-pointers made in the run: the sum of the player's ESPN game boxes. |
| `fg3a` | Int64 | Total three-pointers attempted in the run: the sum of the player's ESPN game boxes. |
| `ftm` | Int64 | Total free throws made in the run: the sum of the player's ESPN game boxes. |
| `fta` | Int64 | Total free throws attempted in the run: the sum of the player's ESPN game boxes. |
| `oreb` | Int64 | Total offensive rebounds in the run: the sum of the player's ESPN game boxes. |
| `dreb` | Int64 | Total defensive rebounds in the run: the sum of the player's ESPN game boxes. |
| `reb` | Int64 | Total total rebounds in the run: the sum of the player's ESPN game boxes. |
| `ast` | Int64 | Total assists in the run: the sum of the player's ESPN game boxes. |
| `stl` | Int64 | Total steals in the run: the sum of the player's ESPN game boxes. |
| `blk` | Int64 | Total blocks in the run: the sum of the player's ESPN game boxes. |
| `tov` | Int64 | Total turnovers in the run: the sum of the player's ESPN game boxes. |
| `pf` | Int64 | Total personal fouls in the run: the sum of the player's ESPN game boxes. |
| `min_pg` | Float64 | Minutes per run game played. |
| `pts_pg` | Float64 | Points per run game played. |
| `reb_pg` | Float64 | Total rebounds per run game played. |
| `ast_pg` | Float64 | Assists per run game played. |
| `stl_pg` | Float64 | Steals per run game played. |
| `blk_pg` | Float64 | Blocks per run game played. |
| `tov_pg` | Float64 | Turnovers per run game played. |
| `fg3m_pg` | Float64 | Three-pointers made per run game played. |
| `ts_pct` | Float64 | True shooting in the run: PTS / (2 * (FGA + 0.44 * FTA)); null with no attempts. |
| `reg_games` | Int64 | Regular-season games played, without the conference-tournament games (every team the player played for); null with none. |
| `reg_min_pg` | Float64 | Minutes per regular-season game played. |
| `reg_pts_pg` | Float64 | Points per regular-season game played. |
| `reg_reb_pg` | Float64 | Total rebounds per regular-season game played. |
| `reg_ast_pg` | Float64 | Assists per regular-season game played. |
| `reg_stl_pg` | Float64 | Steals per regular-season game played. |
| `reg_blk_pg` | Float64 | Blocks per regular-season game played. |
| `reg_tov_pg` | Float64 | Turnovers per regular-season game played. |
| `reg_fg3m_pg` | Float64 | Three-pointers made per regular-season game played. |
| `reg_ts_pct` | Float64 | True shooting over the player's regular season. |
| `min_pg_diff` | Float64 | min_pg - reg_min_pg: the run against the player's regular season (positive is up). |
| `pts_pg_diff` | Float64 | pts_pg - reg_pts_pg: the run against the player's regular season (positive is up). |
| `reb_pg_diff` | Float64 | reb_pg - reg_reb_pg: the run against the player's regular season (positive is up). |
| `ast_pg_diff` | Float64 | ast_pg - reg_ast_pg: the run against the player's regular season (positive is up). |
| `stl_pg_diff` | Float64 | stl_pg - reg_stl_pg: the run against the player's regular season (positive is up). |
| `blk_pg_diff` | Float64 | blk_pg - reg_blk_pg: the run against the player's regular season (positive is up). |
| `tov_pg_diff` | Float64 | tov_pg - reg_tov_pg: the run against the player's regular season (positive is up). |
| `fg3m_pg_diff` | Float64 | fg3m_pg - reg_fg3m_pg: the run against the player's regular season (positive is up). |
| `ts_pct_diff` | Float64 | ts_pct - reg_ts_pct. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_tournament_runs import mbb_tournament_run_players
mbb_tournament_run_players(2025).sort("pts_pg_diff", descending=True).head()
```

### mbb_tournament_run_teams {#mbb_tournament_run_teams}

`mbb_tournament_run_teams(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Each run's record and box totals: the sum of the team's game boxes in the tournament.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per run with a final game, keyed `(season, run_id, team_id)` with the run's `tournament_name` and `tournament_kind` (`ncaa`, `conference` or `postseason`, as on `mbb_tournament_runs`) and `team_abbreviation`: `games`, `wins`, `losses`, `pts`, `opp_pts` and the summed counts `fgm`, `fga`, `fg3m`, `fg3a`, `ftm`, `fta`, `oreb`, `dreb`, `reb`, `ast`, `stl`, `blk`, `tov`, `pf`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `run_id` | String | Run key: '{season}-{tournament_key}-{ESPN team id}', one team's games in one tournament. |
| `tournament_name` | String | The run's tournament name, as on its tournament_runs rows. |
| `tournament_kind` | String | The run's tournament kind, as on its tournament_runs rows: ncaa (the NCAA tournament), conference (a conference tournament) or postseason (the NIT, WNIT, CBI, Crown, WBIT and other postseason events). |
| `team_id` | Int64 | ESPN team id of the row's team (joins the ESPN schedule and team box scores). |
| `team_abbreviation` | String | Team abbreviation. |
| `games` | Int64 | Final games in the run. |
| `wins` | Int64 | Games the team won in the tournament run, counting its final games only. |
| `losses` | Int64 | Games the team lost in the tournament run, counting its final games only. |
| `opp_pts` | Int64 | Opponent points over the run's final games. |
| `pts` | Int64 | Total points over the run's final games: the sum of the team's ESPN game boxes. |
| `fgm` | Int64 | Total field goals made over the run's final games: the sum of the team's ESPN game boxes. |
| `fga` | Int64 | Total field goals attempted over the run's final games: the sum of the team's ESPN game boxes. |
| `fg3m` | Int64 | Total three-pointers made over the run's final games: the sum of the team's ESPN game boxes. |
| `fg3a` | Int64 | Total three-pointers attempted over the run's final games: the sum of the team's ESPN game boxes. |
| `ftm` | Int64 | Total free throws made over the run's final games: the sum of the team's ESPN game boxes. |
| `fta` | Int64 | Total free throws attempted over the run's final games: the sum of the team's ESPN game boxes. |
| `oreb` | Int64 | Total offensive rebounds over the run's final games: the sum of the team's ESPN game boxes. |
| `dreb` | Int64 | Total defensive rebounds over the run's final games: the sum of the team's ESPN game boxes. |
| `reb` | Int64 | Total total rebounds over the run's final games: the sum of the team's ESPN game boxes. |
| `ast` | Int64 | Total assists over the run's final games: the sum of the team's ESPN game boxes. |
| `stl` | Int64 | Total steals over the run's final games: the sum of the team's ESPN game boxes. |
| `blk` | Int64 | Total blocks over the run's final games: the sum of the team's ESPN game boxes. |
| `tov` | Int64 | Total turnovers over the run's final games: the sum of the team's ESPN game boxes. |
| `pf` | Int64 | Total personal fouls over the run's final games: the sum of the team's ESPN game boxes. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_tournament_runs import mbb_tournament_run_teams
mbb_tournament_run_teams(2025).sort("wins", descending=True).head()
```

### mbb_tournament_runs {#mbb_tournament_runs}

`mbb_tournament_runs(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Every tournament game, once per side, keyed to that team's run, with the run's record after it.

A run is one team's games in one tournament. The games are the postseason (ESPN
`season_type` 3: the NCAA tournament from the First Four, the NIT / WNIT, the CBI, the
Crown, the WBIT ...) and the conference tournaments (a `tournament_id` with a conference
group, from mid-February). `run_id` is `"{season}-{tournament_key}-{team_id}"`;
`tournament_key` is ESPN's `tournament_id`, or the headline's event name where the id is
missing (older seasons). A conference tournament ESPN did not tag is not a run.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (`sportsdataverse.wbb.wbb_tournament_runs.wbb_tournament_runs`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

Two rows per tournament game (one per side): `season`, `run_id`, `tournament_key`, `tournament_id`, `tournament_name` (the event name most of its headlines carry), `tournament_kind` (`ncaa`, `conference` or `postseason`), `conference_id`, `round_name`, `game_number` (by tip-off), `game_id`, `game_date`, `completed`, `home_away` (`neutral` at a neutral site), the team's and the opponent's id, abbreviation, display name and score, `winner_team_id`, `wins` / `losses` (after the game), `run_games` and `run_result` (`champion`: won the tournament's last listed game, once final; `eliminated`: lost its last listed game; else `alive`).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `run_id` | String | Run key: '{season}-{tournament_key}-{ESPN team id}', one team's games in one tournament. |
| `tournament_key` | String | The tournament: ESPN's tournament_id as text, or the headline's event name as a slug where the id is missing (older seasons). |
| `tournament_id` | Int64 | ESPN tournament id (22 is the NCAA tournament); an untagged game takes the id of the tagged tournament of its season with the same event name, else null. |
| `tournament_name` | String | The event name most of the tournament's game headlines carry (the text before ' - '). |
| `tournament_kind` | String | ncaa (ESPN tournament 22, First Four onward), conference (a conference tournament: a tagged regular-season tournament with a conference group, from February 15) or postseason (the NIT, WNIT, CBI, Crown, WBIT and other postseason events). |
| `conference_id` | Int64 | ESPN group id of the conference, for a conference tournament; null otherwise. |
| `round_name` | String | The rest of the game headline after the event name (for example 'East Region - 1st Round', 'Final'), without a trailing site. |
| `game_number` | Int64 | Game number within the run, by tip-off. |
| `game_id` | Int64 | ESPN game id of the tournament game (joins the ESPN schedule, summary and box score). |
| `game_date` | String | Game date (US). |
| `completed` | Boolean | True when the game is final. |
| `home_away` | String | home or away, or neutral at a neutral site. |
| `team_id` | Int64 | The run's team: ESPN team id. |
| `team_abbreviation` | String | The team's abbreviation. |
| `team_display_name` | String | The team's display name. |
| `team_score` | Int64 | The team's points (null before tip-off). |
| `opponent_team_id` | Int64 | The opponent's ESPN team id. |
| `opponent_abbreviation` | String | The opponent's abbreviation. |
| `opponent_display_name` | String | The opponent's display name. |
| `opponent_score` | Int64 | The opponent's points (null before tip-off). |
| `winner_team_id` | Int64 | The game winner's ESPN team id; null until the game is final. |
| `wins` | Int64 | The run's wins after this game. |
| `losses` | Int64 | The run's losses after this game. |
| `run_games` | Int64 | Games listed in the run (played and scheduled), as of the release, repeated on every row of the run: an earlier game's row already shows how far the run went, so never use it as a feature for that game. |
| `run_result` | String | champion (won the tournament's last listed game, by tip-off, once final, when it is the only game on the tournament's last date), eliminated (lost its last listed game) or alive. The outcome as of the release, repeated on every row of the run, not as of that game: never use it as a feature for an earlier game. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"` (checked before any load). |

**Example**

```python
from sportsdataverse.mbb.mbb_tournament_runs import mbb_tournament_runs
runs = mbb_tournament_runs(2025)
runs.filter((runs["tournament_kind"] == "ncaa") & (runs["run_result"] == "champion"))
```

### ncaa_box_events {#ncaa_box_events}

`ncaa_box_events(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Each player's box counts per stats.ncaa.org event, the input of the stint and split box.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release order (`season`, `contest_id`, `event_team_espn_team_id`, `player_1_id`, `player_2_id`, `event_type`, `event_result`, `shot_value`). |

**Returns**

Rows in `sportsdataverse._common.game_stints.box_event_schema` (String player ids), `order` the event's row in the release (the stint rows' `order`): one per event with a count for its `player_1_id`, and one per assisted field goal for its `player_2_id`. An event with no player id or no ESPN team id (a non-D1 side) has no row.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id; a non-D1 team has none and gets no rows. |
| `player_id` | String | stats.ncaa.org player id. |
| `order` | Int64 | The action's place in the game: the stint rows' order (stats.nba.com order_index; for college the event's row in the pbp release). |
| `points` | Int64 | Points the player scored: 2 or 3 per made field goal and 1 per made free throw. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goals attempted. |
| `fg3m` | Int64 | Three-pointers made. |
| `fg3a` | Int64 | Three-pointers attempted. |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throws attempted. |
| `oreb` | Int64 | Offensive rebounds (a team rebound is no player's). |
| `dreb` | Int64 | Defensive rebounds (a team rebound is no player's). |
| `ast` | Int64 | Assists: made field goals the play-by-play credits him as the passer on. |
| `stl` | Int64 | Steals he is credited with. |
| `blk` | Int64 | Shots he blocked. |
| `tov` | Int64 | Turnovers (a team turnover is no player's). |
| `pf` | Int64 | Personal fouls (technicals are not counted). |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp
from sportsdataverse.mbb.mbb_game_stints import ncaa_box_events
ncaa_box_events(load_ncaa_mbb_pbp(2025)).head()
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
it, on that event's lineup; a free throw on the lineup at its own foul (the latest, or the one logged after it at the same clock reading; not an earlier period's once clock has run)) and one per
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

`ncaa_lineup_count_rows(pbp: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map the stats.ncaa.org pbp onto the lineup-seasons count frame.

Each shot, free throw, rebound and turnover counts for the side whose `event_team` it is,
on the lineup at the event; a free throw on the lineup at the most recent foul
(`sportsdataverse._common.game_impact.credit_free_throws_to_foul_lineup`), unless that
foul is from an earlier period and clock has run in the free throw's. Events with no count are
dropped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`: halves, or the women's quarters from 2016, set where each period's clock starts. |

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

### ncaa_mbb_lineups {#ncaa_mbb_lineups}

`ncaa_mbb_lineups(pbp: 'pl.DataFrame', *, include_transition: 'bool' = False, fix_tip_in: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Aggregate bigballR-contract play-by-play into per-lineup stats.

Port of bigballR `get_lineups` (`all_functions.R:1945-2521`). Rows
with any missing on-court player and substitution rows are dropped, each
row's home/away five are byte-sorted so a lineup always occupies the same
columns, and the home + away passes are combined per `(p1..p5, team)`.
Ratios are derived from the summed counters, rounded to 3 decimals, and
NA/Inf are zeroed exactly where R does it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame in the sdv-py 35-column snake_case bigballR contract (`parse_ncaa_bb_game_pbp` output). May span multiple games. |
| `include_transition` | `bool` | `False` | When True, append the `_trans`/`_half` split surface plus `o_trans_pct`/`d_trans_pct` (213 columns total). |
| `fix_tip_in` | `bool` | `True` | When True (default), rim stats count the scrape engine's real `"Tip In"` vocabulary. When False, reproduce R's literal `"Tip-In"` test (`all_functions.R:2012`) — tip-ins silently excluded — for oracle parity. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per lineup+team — 75 columns (`LINEUPS_COLUMNS`) or 213 with `include_transition=True` (`LINEUPS_TRANSITION_COLUMNS`), rows sorted by `p1..p5, team`. Empty input yields an empty frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `p1` | String |  |
| `p2` | String |  |
| `p3` | String |  |
| `p4` | String |  |
| `p5` | String |  |
| `team` | String | Team-side label or team identifier. |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_lineups
lineups = ncaa_mbb_lineups(pbp)
print(lineups.shape)

# Transition/half-court splits, pandas out

df_pd = ncaa_mbb_lineups(pbp, include_transition=True, return_as_pandas=True)

# Pipeline next step (one line)

lineups.filter(pl.col("mins") > 10).sort("netrtg", descending=True).head()
```

### ncaa_mbb_on_off {#ncaa_mbb_on_off}

`ncaa_mbb_on_off(players: 'Union[str, Sequence[str]]', lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every on/off combination of the given players.

Port of bigballR `on_off_generator` (`all_functions.R:2555-2749`,
`include_transition=F` path). For k players, all `2^k` on/off
assignments are enumerated in R's `expand.grid` order (first player
varies fastest; first row all-On, last all-Off); each combination sums
the lineups whose membership matches exactly, re-derives the full ratio
block (including `e_poss`), rounds to 3 decimals, and zeroes NA/Inf.
Combinations matching zero lineups produce an all-zero row.

Faithful R quirk (kept, flagged): when `included`/`excluded` is
passed, the base lineup set comes from the membership filter ONLY — the
inferred-team filter is skipped (`all_functions.R:2584-2589`), so an
included player on another team would leak that team's lineups in.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `Union[str, Sequence[str]]` |  | Player name(s) to split on (the `Status` axis). |
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `included` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Optional membership filter forwarded to `ncaa_mbb_player_lineups` (replaces the team filter). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with `2^k` rows — `status` (e.g. `"A.PLAYER On | B.PLAYER Off"`) + the 69 stat columns (`ON_OFF_COLUMNS`).

| col_name | type | description |
|---|---|---|
| `status` | String | Status label. |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the players' team cannot be uniquely inferred from the lineups (R's `"ERROR- Player team not found"`). |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_on_off
split = ncaa_mbb_on_off("KEATON.WAGLER", lineups)
print(split.shape)

# Two-player interaction

duo = ncaa_mbb_on_off(["A.PLAYER", "B.PLAYER"], lineups)

# Pipeline next step (one line)

split.select("status", "netrtg")
```

### ncaa_mbb_player_combos {#ncaa_mbb_player_combos}

`ncaa_mbb_player_combos(lineups: 'pl.DataFrame', *, n: 'int' = 2, min_mins: 'float' = 0, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, include_transition: 'bool' = False, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team stats for every n-player combination on the court together.

Port of bigballR `get_player_combos` + `team_comb`
(`get_player_combos.R:19-38` + `:42-191`). Combos are enumerated per
team over the byte-sorted unique player pool (lexicographic
`gtools::combinations` order), filtered to combos whose lineups total
strictly more than `min_mins` minutes, then each combo's lineup rows
are summed and the ratio block re-derived. **Rounding is 2 decimals**
here (R rounds 3 in `get_lineups` / `on_off_generator`) and `e_poss`
is the SUM of the per-lineup estimates, not a recompute — both faithful.

R's `include_transition` switch is dead code (an exact-match guard at
`get_player_combos.R:28-30` always forces it back to `FALSE`); the
port implements the suffix-match intent but keeps the `False` default,
which is the only R-reachable behavior.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups`. |
| `n` | `int` | `2` | Combination size, 1-5. |
| `min_mins` | `float` | `0` | Keep combos with total on-court minutes strictly greater than this (summed over the rounded per-lineup `mins`). |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be on the court in every lineup considered. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must be off the court in every lineup considered. |
| `include_transition` | `bool` | `False` | Re-derive the `_trans`/`_half` ratio surface (requires a transition lineups frame; forced `False` otherwise). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`pl.DataFrame` (or `pd.DataFrame`) with one row per combo: `team, p1..pn` + the stat surface of the input frame. Teams appear in byte-sorted order.

| col_name | type | description |
|---|---|---|
| `team` | String | Team-side label or team identifier. |
| `p1` | String |  |
| `p2` | String |  |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `n` is not an integer from 1 to 5. |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_combos
duos = ncaa_mbb_player_combos(lineups, n=2, min_mins=5)
print(duos.shape)

# Anchored on one player

trios = ncaa_mbb_player_combos(lineups, n=3, included="KEATON.WAGLER")

# Pipeline next step (one line)

duos.sort("netrtg", descending=True).head()
```

### ncaa_mbb_player_lineups {#ncaa_mbb_player_lineups}

`ncaa_mbb_player_lineups(lineups: 'pl.DataFrame', *, included: 'Union[str, Sequence[str], None]' = None, excluded: 'Union[str, Sequence[str], None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Filter a lineups frame by on-court player membership.

Port of bigballR `get_player_lineups` (`all_functions.R:2761-2792`):
keep rows where every `included` player is on the court (in `p1..p5`)
and no `excluded` player is. With both filters `None` the input is
returned unchanged (R's `Included = NA, Excluded = NA` passthrough).
Membership is tested by name against `p1..p5` (R tests the positional
first five columns); row order is preserved.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `lineups` | `DataFrame` |  | Lineups frame from `ncaa_mbb_lineups` (any frame with `p1..p5` works). |
| `included` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must ALL be on the court. |
| `excluded` | `Union[str, Sequence[str], None]` | `None` | Player name(s) that must NONE be on the court. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Row-subset of `lineups`; schema unchanged.

| col_name | type | description |
|---|---|---|
| `p1` | String |  |
| `p2` | String |  |
| `p3` | String |  |
| `p4` | String |  |
| `p5` | String |  |
| `team` | String | Team-side label or team identifier. |
| `mins` | Float64 |  |
| `o_mins` | Float64 |  |
| `d_mins` | Float64 |  |
| `o_poss` | Float64 |  |
| `d_poss` | Float64 |  |
| `ortg` | Float64 |  |
| `drtg` | Float64 |  |
| `netrtg` | Float64 |  |
| `pts` | Float64 | Points scored. |
| `d_pts` | Float64 |  |
| `fga` | Float64 | Field goal attempts. |
| `d_fga` | Float64 |  |
| `fgm` | Float64 | Field goals made. |
| `d_fgm` | Float64 |  |
| `tpa` | Float64 |  |
| `d_tpa` | Float64 |  |
| `tpm` | Float64 |  |
| `d_tpm` | Float64 |  |
| `fta` | Float64 | Free throw attempts. |
| `d_fta` | Float64 |  |
| `ftm` | Float64 | Free throws made. |
| `d_ftm` | Float64 |  |
| `rima` | Float64 |  |
| `d_rima` | Float64 |  |
| `rimm` | Float64 |  |
| `d_rimm` | Float64 |  |
| `orb` | Float64 |  |
| `d_orb` | Float64 |  |
| `drb` | Float64 |  |
| `d_drb` | Float64 |  |
| `blk` | Float64 | Blocks. |
| `d_blk` | Float64 |  |
| `to` | Float64 | To. |
| `d_to` | Float64 |  |
| `ast` | Float64 | Assists. |
| `d_ast` | Float64 |  |
| `e_poss` | Float64 |  |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `d_fg_pct` | Float64 |  |
| `tpp` | Float64 |  |
| `d_tpp` | Float64 |  |
| `ftp` | Float64 |  |
| `d_ftp` | Float64 |  |
| `efg_pct` | Float64 |  |
| `d_efg_pct` | Float64 |  |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `d_ts_pct` | Float64 |  |
| `rim_pct` | Float64 |  |
| `d_rim_pct` | Float64 |  |
| `mid_pct` | Float64 |  |
| `d_mid_pct` | Float64 |  |
| `tp_rate` | Float64 |  |
| `d_tp_rate` | Float64 |  |
| `rim_rate` | Float64 |  |
| `d_rim_rate` | Float64 |  |
| `mid_rate` | Float64 |  |
| `d_mid_rate` | Float64 |  |
| `ft_rate` | Float64 | Ft rate. |
| `d_ft_rate` | Float64 |  |
| `ast_rate` | Float64 |  |
| `d_ast_rate` | Float64 |  |
| `to_rate` | Float64 | To rate. |
| `d_to_rate` | Float64 |  |
| `blk_rate` | Float64 |  |
| `o_blk_rate` | Float64 |  |
| `orb_pct` | Float64 | Offensive rebound percentage. |
| `drb_pct` | Float64 | Defensive rebound percentage. |
| `time_per_poss` | Float64 |  |
| `d_time_per_poss` | Float64 |  |

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_lineups import ncaa_mbb_player_lineups
on = ncaa_mbb_player_lineups(lineups, included="KEATON.WAGLER")
print(on.shape)

# Included + excluded combination

df = ncaa_mbb_player_lineups(lineups, included=["A.PLAYER"], excluded=["B.PLAYER"])

# Pipeline next step (one line)

on.select(pl.col("mins").sum())
```

### ncaa_possession_rate_rows {#ncaa_possession_rate_rows}

`ncaa_possession_rate_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map `ncaa_{mbb,wbb}_possessions` rows onto the team-season-rates possession frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_ncaa_mbb_possessions` / `load_ncaa_wbb_possessions` rows. |
| `schedule` | `DataFrame` |  | `load_mbb_schedule` / `load_wbb_schedule` rows (`game_id`, `neutral_site`), joined on `espn_game_id`. |
| `league` | `str` | `'mens'` | `"mens"` (two 20-minute halves) or `"womens"` (10-minute quarters from 2016, halves before). |

**Returns**

One row per possession in `~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`, with null four-factor counts.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | The stats.ncaa.org contest id. |
| `period` | Int64 | Period of the possession; overtimes follow the last regulation period. |
| `possession_number` | Int64 | Order of the possession in the game. |
| `late` | Boolean | True in the 2nd half (men; women before 2016), the 4th quarter (women from 2016) or an overtime: the periods the clutch window can fall in. |
| `start_clock` | Float64 | Seconds left in the period when the possession starts: its length plus the lengths of the later possessions in the period. |
| `end_clock` | Float64 | Seconds left in the period when the possession ends. |
| `offense_team_id` | Int64 | ESPN id of the team with the ball; null for a non-D1 team. |
| `defense_team_id` | Int64 | ESPN id of the team on defense; null for a non-D1 team. |
| `offense_is_home` | Boolean | True when the offense is the (nominal) home team; it signs the running score for the clutch margin. |
| `neutral_site` | Boolean | True for an ESPN neutral-site game (False when ESPN does not carry the game); such a game is in neither the home nor the away split. |
| `points` | Int64 | Points the offense scored on the possession. |
| `fga` | Float64 | Always null: the NCAA possessions carry no fga counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fgm` | Float64 | Always null: the NCAA possessions carry no fgm counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fg3m` | Float64 | Always null: the NCAA possessions carry no fg3m counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fta` | Float64 | Always null: the NCAA possessions carry no fta counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `ftm` | Float64 | Always null: the NCAA possessions carry no ftm counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `oreb` | Float64 | Always null: the NCAA possessions carry no oreb counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `dreb` | Float64 | Always null: the NCAA possessions carry no dreb counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `tov` | Float64 | Always null: the NCAA possessions carry no tov counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `ff_poss` | Float64 | Always null: the turnover rate's denominator comes from the team box. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_team_season_rates import ncaa_possession_rate_rows
rows = ncaa_possession_rate_rows(load_ncaa_mbb_possessions(2025), load_mbb_schedule(2025))
```

### ncaa_score_rows {#ncaa_score_rows}

`ncaa_score_rows(pbp: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map the stats.ncaa.org play-by-play onto the game-flow score rows.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order (`contest_id`, `season`, `period`, `game_seconds`, `home_score`, `away_score`, `event_team_espn_team_id`, `home_espn_team_id`, `away_espn_team_id`, `espn_game_id`). |
| `schedule` | `DataFrame` |  | `load_mbb_schedule` / `load_wbb_schedule` rows (`game_id`, `neutral_site`), joined on `espn_game_id`; a contest it lacks is not neutral. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

Rows in `~sportsdataverse._common.game_flow.SCORE_ROWS_SCHEMA`, without `home_wp`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.ncaa.org contest id. |
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
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_pbp
from sportsdataverse.mbb.mbb_game_flow import ncaa_score_rows
rows = ncaa_score_rows(load_ncaa_mbb_pbp(2026), load_mbb_schedule(2026))
```

### ncaa_situation_rows {#ncaa_situation_rows}

`ncaa_situation_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map `ncaa_{mbb,wbb}_possessions` rows onto the situational-splits possession frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_ncaa_mbb_possessions` / `load_ncaa_wbb_possessions` rows. |
| `schedule` | `DataFrame` |  | `load_mbb_schedule` / `load_wbb_schedule` rows (`game_id`, `neutral_site`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

One row per possession in `~sportsdataverse._common.situational_splits.situation_row_schema` (String player ids, null `is_second_chance`).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | The stats.ncaa.org contest id. |
| `period` | Int64 | Period of the possession; overtimes follow the last regulation period. |
| `possession_number` | Int64 | Order of the possession in the game. |
| `late` | Boolean | True in the 2nd half (men; women before 2016), the 4th quarter (women from 2016) or an overtime: the periods the clutch window can fall in. |
| `start_clock` | Float64 | Seconds left in the period when the possession starts: its length plus the lengths of the later possessions in the period. |
| `end_clock` | Float64 | Seconds left in the period when the possession ends. |
| `offense_team_id` | Int64 | ESPN id of the team with the ball; null for a non-D1 team. |
| `defense_team_id` | Int64 | ESPN id of the team on defense; null for a non-D1 team. |
| `offense_is_home` | Boolean | True when the offense is the (nominal) home team; it signs the running score for the clutch margin. |
| `neutral_site` | Boolean | True for an ESPN neutral-site game (False when ESPN does not carry the game); such a game is in neither the home nor the away split. |
| `points` | Int64 | Points the offense scored on the possession. |
| `fga` | Float64 | Always null: the NCAA possessions carry no fga counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fgm` | Float64 | Always null: the NCAA possessions carry no fgm counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fg3m` | Float64 | Always null: the NCAA possessions carry no fg3m counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `fta` | Float64 | Always null: the NCAA possessions carry no fta counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `ftm` | Float64 | Always null: the NCAA possessions carry no ftm counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `oreb` | Float64 | Always null: the NCAA possessions carry no oreb counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `dreb` | Float64 | Always null: the NCAA possessions carry no dreb counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `tov` | Float64 | Always null: the NCAA possessions carry no tov counts; the four factors come from the team box (ncaa_team_box_count_rows). |
| `ff_poss` | Float64 | Always null: the turnover rate's denominator comes from the team box. |
| `reg_periods` | Int64 | Regulation periods of the game: 4 quarters, or 2 halves. |
| `is_transition` | Boolean | The possession is transition (BB-D7's flag for the pros, the release's own for college). |
| `is_garbage_time` | Boolean | The possession is in garbage time (bigballR's rule; BB-D7 for the pros, the release's flag for college). |
| `is_second_chance` | Boolean | The possession had an offensive rebound; null in college (the NCAA possessions carry no rebound counts). |
| `off_1` | String | Offense player 1 on the floor as the possession starts. |
| `off_2` | String | Offense player 2 on the floor as the possession starts. |
| `off_3` | String | Offense player 3 on the floor as the possession starts. |
| `off_4` | String | Offense player 4 on the floor as the possession starts. |
| `off_5` | String | Offense player 5 on the floor as the possession starts. |
| `def_1` | String | Defense player 1 on the floor as the possession starts. |
| `def_2` | String | Defense player 2 on the floor as the possession starts. |
| `def_3` | String | Defense player 3 on the floor as the possession starts. |
| `def_4` | String | Defense player 4 on the floor as the possession starts. |
| `def_5` | String | Defense player 5 on the floor as the possession starts. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_situational import ncaa_situation_rows
rows = ncaa_situation_rows(load_ncaa_mbb_possessions(2025), load_mbb_schedule(2025))
```

### ncaa_stint_rows {#ncaa_stint_rows}

`ncaa_stint_rows(pbp: 'pl.DataFrame', possessions: 'pl.DataFrame', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Map the stats.ncaa.org pbp and possessions onto the stint-row frame.

One row per event in game order, on the event's lineup: its clock, its points (a free throw's
on the row of its own foul (the latest, or the one logged after it at the same clock reading; not an earlier period's once clock has run)) and the possessions that start at it. A possession starts
at the first event of its `poss_num`; one whose `poss_team` is neither side is dropped.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows, in release (game) order, with `period`, `game_seconds` and `poss_num`. |
| `possessions` | `DataFrame` |  | `load_ncaa_mbb_possessions` / `load_ncaa_wbb_possessions` rows (`contest_id`, `poss_num`, `poss_team`, `home`, `away`). |
| `league` | `str` | `'mens'` | `"mens"` (20-minute halves) or `"womens"` (10-minute quarters from 2016, halves before); overtimes are 5 minutes. |

**Returns**

Rows in `sportsdataverse._common.game_stints.stint_row_schema` with String player ids: `period`, `clock` (seconds left in the period after the event), `order` (the event's row in the release) and `is_garbage_time` (the event's own flag).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the action. |
| `game_id` | String | stats.ncaa.org contest id. |
| `home_team_id` | Int64 | Home ESPN team id (null for a non-D1 team). |
| `away_team_id` | Int64 | Away ESPN team id (null for a non-D1 team). |
| `home_1` | String | Home player 1 on the floor. |
| `home_2` | String | Home player 2 on the floor. |
| `home_3` | String | Home player 3 on the floor. |
| `home_4` | String | Home player 4 on the floor. |
| `home_5` | String | Home player 5 on the floor. |
| `away_1` | String | Away player 1 on the floor. |
| `away_2` | String | Away player 2 on the floor. |
| `away_3` | String | Away player 3 on the floor. |
| `away_4` | String | Away player 4 on the floor. |
| `away_5` | String | Away player 5 on the floor. |
| `seconds` | Float64 | Game clock that ran into this action (from the previous action of the period). |
| `home_pts` | Int64 | Points the home team scored on this action; a free throw's points sit on the row of its foul (`ft_foul_order`). |
| `away_pts` | Int64 | Points the away team scored on this action; a free throw's points sit on the row of its foul (`ft_foul_order`). |
| `home_poss` | Int64 | Possessions of the home team that start at this action. |
| `away_poss` | Int64 | Possessions of the away team that start at this action. |
| `period` | Int64 | Period of the action. |
| `clock` | Float64 | Period clock (seconds left) after the event; 0 on the period's last event, which takes the period's tail. |
| `order` | Int64 | The event's row in the pbp release (its place in the game). |
| `is_garbage_time` | Boolean | Whether the event is in garbage time: the release's own is_garbage_time (bigballR's rule). |
| `ft_foul_order` | Int64 | On a free throw whose points moved, the order of its foul's row: the game's latest foul, or the one logged after it at the same clock reading; an earlier period's foul only while no clock has run in the free throw's period. Null on every other row. game_stints puts the free throw's box counts on that row's stint when the shooter is on it. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp, load_ncaa_mbb_possessions
from sportsdataverse.mbb.mbb_game_stints import ncaa_stint_rows
rows = ncaa_stint_rows(load_ncaa_mbb_pbp(2025), load_ncaa_mbb_possessions(2025))
```

### ncaa_team_box_count_rows {#ncaa_team_box_count_rows}

`ncaa_team_box_count_rows(team_box: 'pl.DataFrame') -> 'pl.DataFrame'`

Map `ncaa_{mbb,wbb}_team_box` rows onto the four-factor count frame.

Each row is one team's offense in one game: its FGA, FGM, 3PM, FTA, FTM, offensive rebounds and
turnovers, the opponent's defensive rebounds (`d_drb`) and its own possessions (`o_poss`).
`offense_is_home` is the box's own home designation (`team == home`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_box` | `DataFrame` |  | `load_ncaa_mbb_team_box` / `load_ncaa_wbb_team_box` rows. |

**Returns**

One row per (game, team) in `~sportsdataverse._common.team_season_rates.COUNT_ROWS_SCHEMA`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `game_id` | String | The stats.ncaa.org contest id. |
| `offense_team_id` | Int64 | ESPN id of the team whose offense the row counts; null for a non-D1 team. |
| `defense_team_id` | Int64 | ESPN id of its opponent; null for a non-D1 team. |
| `offense_is_home` | Boolean | True when the team is the game's (nominal) home team in the NCAA team box; a neutral-site game is in neither split. |
| `fga` | Float64 | Field-goal attempts in the game. |
| `fgm` | Float64 | Field goals made in the game. |
| `fg3m` | Float64 | Three-pointers made in the game. |
| `fta` | Float64 | Free-throw attempts in the game. |
| `ftm` | Float64 | Free throws made in the game. |
| `oreb` | Float64 | Offensive rebounds in the game. |
| `dreb` | Float64 | The opponent's defensive rebounds in the game (the other half of the offensive-rebound chances). |
| `tov` | Float64 | Turnovers in the game. |
| `ff_poss` | Float64 | The team box's offensive possessions in the game, the turnover-rate denominator. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_team_box
from sportsdataverse.mbb.mbb_team_season_rates import ncaa_team_box_count_rows
counts = ncaa_team_box_count_rows(load_ncaa_mbb_team_box(2025))
```

### ncaa_team_coaches {#ncaa_team_coaches}

`ncaa_team_coaches(root: 'str | Path', seasons: 'int | list[int]', *, league: 'str' = 'mens') -> 'pl.DataFrame'`

Every team-season's head coach from a raw-repo tree of stats.ncaa.org roster pages.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `root` | `str \| Path` |  | The `ncaa-mbb-hoops-raw` / `ncaa-wbb-hoops-raw` checkout. |
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

One row per page with a coach card, in `NCAA_COACHES_SCHEMA`; `team_id` is the ESPN team id from the season's `teams/json` crosswalk (null when it has none). A season with no crosswalk file is skipped with a warning.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `ncaa_team_id` | Int64 | stats.ncaa.org team-season id (the roster page's file name, teams/{id}). |
| `team_id` | Int64 | ESPN team id of the school, from the season's teams/json crosswalk; null when the crosswalk has none. |
| `team` | String | School name as stats.ncaa.org's crosswalk gives it. |
| `coach_id` | Int64 | NCAA person id of the head coach (stats.ncaa.org /people/{id}). |
| `coach` | String | Head coach's name on the page's Coach card. |
| `career_seasons` | Int64 | The coach's seasons as a head coach through this one, every school and division (the card's Seasons). |
| `career_wins` | Int64 | The coach's career wins through this season (the card's Record; the NCAA drops vacated wins). |
| `career_losses` | Int64 | The coach's career losses through this season (the card's Record). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league` is not `"mens"` or `"womens"`. |

### ncaa_team_game_counts {#ncaa_team_game_counts}

`ncaa_team_game_counts(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Each team's shots, free throws, rebounds and turnovers per game, whole and with garbage time split out.

Sums the stats.ncaa.org events by the team that made them (`event_team_espn_team_id`): a
field goal is a `shot_value` 2 or 3 event, a free throw a `shot_value` 1 event, made when
`event_result` is `made`; `oreb` / `dreb` / `tov` count the `Offensive Rebound` /
`Defensive Rebound` / `Turnover` events (team rebounds and team turnovers included,
`Deadball Rebound` not). The `garbage` split is each event's own `is_garbage_time`
(bigballR's flag, the one the possessions release carries); an event with no flag is in
`all` only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_ncaa_mbb_pbp` / `load_ncaa_wbb_pbp` rows. |

**Returns**

One row per `(season, game_id, team_id, family, split)`: family `all` (split `all`) and family `garbage` (`garbage_time`, `not_garbage_time`, a split with no event of the team has no row), with `game_id` the stats.ncaa.org contest id (Utf8), `team_id` the ESPN team id (Int64; an event of a team without one, a non-D1 side, is dropped) and the `COUNT_COLUMNS` (Int64). Within a game the two garbage rows add up to the `all` row when every event is flagged. An empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year (the year the season ends). |
| `game_id` | String | The stats.ncaa.org contest id. |
| `team_id` | Int64 | ESPN team id of the team that made the events; events of a side with no ESPN id (non-D1) are dropped. |
| `family` | String | all (the whole game) or garbage. |
| `split` | String | all, or garbage_time / not_garbage_time by each event's is_garbage_time. |
| `fga` | Int64 | Field-goal attempts in the split: the team's shot_value 2 and 3 events in the ncaa pbp. |
| `fgm` | Int64 | Made field goals in the split (event_result made). |
| `fg3a` | Int64 | Three-point attempts in the split (shot_value 3). |
| `fg3m` | Int64 | Made threes in the split. |
| `fta` | Int64 | Free-throw attempts in the split (shot_value 1). |
| `ftm` | Int64 | Made free throws in the split. |
| `oreb` | Int64 | Offensive rebounds in the split (Offensive Rebound events, team rebounds included). |
| `dreb` | Int64 | Defensive rebounds in the split (Defensive Rebound events, team rebounds included). |
| `tov` | Int64 | Turnovers in the split (Turnover events, team turnovers included). |

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | If `pbp` is missing a column it reads. |

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp
from sportsdataverse.mbb.mbb_situational import ncaa_team_game_counts
ncaa_team_game_counts(load_ncaa_mbb_pbp(2026)).filter(split="not_garbage_time").head()
```
