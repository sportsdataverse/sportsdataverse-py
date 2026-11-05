---
title: "NBA — additional Python functions — Analytics: nba_lineup–nba_raw"
sidebar_label: "Analytics: nba_lineup–nba_raw"
sidebar_position: 15
description: "NBA — additional Python functions — Analytics: nba_lineup–nba_raw — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_lineup–nba_raw

### nba_lineup_seasons {#nba_lineup_seasons}

`nba_lineup_seasons(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', min_poss: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Lineup seasons: 2-, 3- and 5-man units with possessions, minutes, ratings and four factors.

The definitions are in `sportsdataverse._common.lineup_seasons`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `min_poss` | `int \| None` | `None` | The rank gate, offensive plus defensive possessions. Defaults to 400 (NBA) or 200 (WNBA). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

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
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss. Null with a zero denominator. On 2024-25 units it runs about 5 above stats.nba.com's lineup rating, which credits a possession to every lineup on the floor during it (its counts run ~4% higher). |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss. Null with a zero denominator. On 2024-25 units it runs about 5 above stats.nba.com's lineup rating, which credits a possession to every lineup on the floor during it (its counts run ~4% higher). |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `tov_pct` | Float64 | Turnovers per offensive possession, as a fraction. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `orb_pct` | Float64 | Offensive-rebound rate: OREB / (OREB + the opponent's DREB), as a fraction. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `ft_rate` | Float64 | Free-throw rate: FTA / FGA. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `opp_efg_pct` | Float64 | The opponent's effective field-goal percentage with the unit on the floor. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `opp_tov_pct` | Float64 | The opponent's turnovers per defensive possession. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `opp_orb_pct` | Float64 | The opponent's offensive-rebound rate: its OREB / (its OREB + the unit's DREB). Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `opp_ft_rate` | Float64 | The opponent's free-throw rate: FTA / FGA. Null with a zero denominator. Each possession's shots, free throws, rebounds and turnovers count for the lineup that started it, even after a substitution inside it. |
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)); null for a non-qualifier or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, `season_type` is not one of the four above, or the possessions carry a null shot, free-throw, rebound or turnover count. |

**Example**

```python
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_seasons
units = nba_lineup_seasons(2025)
units.filter(n_players=5).sort("net_rtg_rank").head()
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

### nba_play_style_features {#nba_play_style_features}

`nba_play_style_features(seasons: 'int | list[int]', *, side: 'str' = 'offense', entity: 'str' = 'player', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The play-style clustering features, one row per qualifying player- or team-season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). Seasons before `NBA_PLAY_STYLE_FIRST_SEASON` are skipped. |
| `side` | `str` | `'offense'` | `"offense"` or `"defense"`. |
| `entity` | `str` | `'player'` | `"player"` or `"team"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

The ids, `season` (and `min` for players) and the raw (unstandardized) features of `NBA_PLAY_STYLE_FEATURES` for `(side, entity)` (every candidate feature for the team defense). A season no release covers yet is skipped; with none left the frame has those columns and zero rows. The returns table below is the default `side="offense", entity="player"` frame.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | stats.nba.com player id. |
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `player_name` | String | Player's display name on stats.nba.com. |
| `team_id` | Int64 | stats.nba.com team id (the league-dashboard team; a traded player's last team). |
| `min` | Float64 | Regular-season minutes; the qualifier is a quarter of the season's most minutes. |
| `pt_isolation` | Float64 | Share of the player's Synergy possessions that were isolations (stats.nba.com play-type frequency). |
| `pt_prballhandler` | Float64 | Share of Synergy possessions as the pick-and-roll ball handler. |
| `pt_prrollman` | Float64 | Share of Synergy possessions as the pick-and-roll roll man. |
| `pt_postup` | Float64 | Share of Synergy possessions that were post-ups. |
| `pt_spotup` | Float64 | Share of Synergy possessions that were spot-ups. |
| `pt_handoff` | Float64 | Share of Synergy possessions out of handoffs. |
| `pt_offscreen` | Float64 | Share of Synergy possessions coming off screens. |
| `pt_cut` | Float64 | Share of Synergy possessions on cuts. |
| `pt_transition` | Float64 | Share of Synergy possessions in transition. |
| `pt_offrebound` | Float64 | Share of Synergy possessions on putbacks after an offensive rebound. |
| `fga3_share` | Float64 | Share of field-goal attempts that were threes. |
| `uast_share` | Float64 | Share of made field goals that were unassisted. |
| `paint_pts_share` | Float64 | Share of points scored in the paint. |
| `mid_pts_share` | Float64 | Share of points from mid-range twos. |
| `drives_p36` | Float64 | Drives per 36 minutes (tracking). |
| `touches_p36` | Float64 | Touches per 36 minutes (tracking). |
| `sec_per_touch` | Float64 | Average seconds per touch (tracking). |
| `drib_per_touch` | Float64 | Average dribbles per touch (tracking). |
| `catch_shoot_share` | Float64 | Catch-and-shoot attempts as a share of field-goal attempts. |
| `pull_up_share` | Float64 | Pull-up attempts as a share of field-goal attempts. |
| `post_touches_p36` | Float64 | Post touches per 36 minutes. |
| `paint_touches_p36` | Float64 | Paint touches per 36 minutes. |
| `potential_ast_p36` | Float64 | Potential assists per 36 minutes. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `side` or `entity` is not one of the values above. |

**Example**

```python
from sportsdataverse.nba.nba_play_style import nba_play_style_features
feats = nba_play_style_features(2025, side="defense", entity="team")
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

### nba_player_on_off {#nba_player_on_off}

`nba_player_on_off(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', min_poss: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Player on/off: each player's team with him on the floor and without him, per team-season.

The definitions are in `sportsdataverse._common.player_on_off`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `min_poss` | `int \| None` | `None` | The rank gate, offensive plus defensive possessions both on and off the floor. Defaults to 400 (NBA) or 200 (WNBA). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

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
| `off_rtg_on_rank` | Float64 | Rank of off_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. |
| `def_rtg_on_rank` | Float64 | Rank of def_rtg_on within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. |
| `net_rtg_on_rank` | Float64 | Rank of net_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. |
| `off_rtg_diff_rank` | Float64 | Rank of off_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. |
| `def_rtg_diff_rank` | Float64 | Rank of def_rtg_diff within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. |
| `net_rtg_diff_rank` | Float64 | Rank of net_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. |
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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_player_on_off import nba_player_on_off
on_off = nba_player_on_off(2025)
on_off.sort("net_rtg_diff_rank").head()
```

### nba_player_play_styles {#nba_player_play_styles}

`nba_player_play_styles(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Offensive and defensive play-style archetypes per player-season (BB-D20).

The method is in `sportsdataverse._common.play_style` and the features in
`sportsdataverse.nba.nba_play_style`. A player qualifies on a side with at least a quarter
of the season's most minutes (`PLAYER_MIN_SHARE`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). Offense covers 2016+, defense 2018+; an uncovered side is null. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(player_id, season)` qualifying on either side: `player_id` (Int64), `season`, `player_name`, `team_id` (Int64), `min` (season minutes) and for `off_` and `def_` the `archetype`, `cluster`, `dist_to_center`, `second_archetype` and `margin`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | stats.nba.com player id. |
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `player_name` | String | Player's display name on stats.nba.com. |
| `team_id` | Int64 | stats.nba.com team id (the league-dashboard team; a traded player's last team). |
| `min` | Float64 | Regular-season minutes; the qualifier is a quarter of the season's most minutes. |
| `off_archetype` | String | Offensive play-style archetype: the label of the nearest frozen KMeans center to the player-season's within-season z-scored features; null when the player does not qualify on that side or the season is not covered. |
| `off_cluster` | Int64 | Index of the offensive archetype's center in the bundled artifact (0-based). |
| `off_dist_to_center` | Float64 | Euclidean distance (z-space) to the offensive archetype's center; small means a prototypical example. |
| `off_second_archetype` | String | The next-nearest offensive archetype, the style a hybrid leans toward. |
| `off_margin` | Float64 | Distance to the second center minus distance to the nearest; small means a hybrid between the two archetypes. |
| `def_archetype` | String | Defensive play-style archetype: the label of the nearest frozen KMeans center to the player-season's within-season z-scored features; null when the player does not qualify on that side or the season is not covered. |
| `def_cluster` | Int64 | Index of the defensive archetype's center in the bundled artifact (0-based). |
| `def_dist_to_center` | Float64 | Euclidean distance (z-space) to the defensive archetype's center; small means a prototypical example. |
| `def_second_archetype` | String | The next-nearest defensive archetype, the style a hybrid leans toward. |
| `def_margin` | Float64 | Distance to the second center minus distance to the nearest; small means a hybrid between the two archetypes. |

**Example**

```python
from sportsdataverse.nba.nba_play_style import nba_player_play_styles
styles = nba_player_play_styles(2025)
styles.group_by("off_archetype").len()
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
