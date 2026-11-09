---
title: "NBA — additional Python functions — Analytics: nba_lineup–nba_player"
sidebar_label: "Analytics: nba_lineup–nba_player"
sidebar_position: 15
description: "NBA — additional Python functions — Analytics: nba_lineup–nba_player — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_lineup–nba_player

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
| `off_rtg_rank` | Float64 | Rank of off_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose off_rtg_n is at least 1,300 offensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `def_rtg_rank` | Float64 | Rank of def_rtg within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose def_rtg_n is at least 2,000 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `net_rtg_rank` | Float64 | Rank of net_rtg within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose net_rtg_n is at least 2,000 possessions (offense plus defense) (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `efg_pct_rank` | Float64 | Rank of efg_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose efg_pct_n is at least 1,200 field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `tov_pct_rank` | Float64 | Rank of tov_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose tov_pct_n is at least 1,400 offensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `orb_pct_rank` | Float64 | Rank of orb_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose orb_pct_n is at least 410 offensive-rebound chances (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `ft_rate_rank` | Float64 | Rank of ft_rate within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose ft_rate_n is at least 1,300 field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose opp_efg_pct_n is at least 1,900 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct within the season and unit size, league-wide (the highest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose opp_tov_pct_n is at least 960 defensive possessions (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose opp_orb_pct_n is at least 1,200 opponent offensive-rebound chances (the sample at which the rate is half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate within the season and unit size, league-wide (the lowest value ranks 1; ties average), among units with at least min_poss possessions (poss + opp_poss; default 400 with league_id '00' (NBA), 200 with league_id '10' (WNBA)), whose opp_ft_rate_n is at least 1,500 opponent field-goal attempts (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null for a non-qualifier, a sample under that gate or a null rate, and for every unit of the group when fewer than ten qualifying units have a finite value of this rate or they all share one value. |
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

### nba_neutral_site_games {#nba_neutral_site_games}

`nba_neutral_site_games(seasons: 'list[int] | None' = None, *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The curated NBA / WNBA neutral-site games: played abroad, or NBA Cup knockout games in Las Vegas.

The 2020 bubbles are a date rule (`BUBBLES`), not rows. The team ratings, the season sim,
the rated priors and game flow drop the home-court term for every game in either.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int] \| None` | `None` | Seasons to keep, named by the year they end (`2026` is 2025-26; the WNBA season is its calendar year). `None` keeps all. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per game: `league`, `season`, `game_id` (the stats.nba.com id, Utf8), `game_date`, `season_type` (`regular-season`, or `nba-cup` for a Cup final), `home_team` / `away_team` (stats abbreviations, the designated home team first), `arena`, `city`, `country`, `reason` (`international` or `cup_knockout`) and `source_url`.

| col_name | type | description |
|---|---|---|
| `league` | String | nba or wnba. |
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (Utf8). |
| `game_date` | Date | Game date (US Eastern), as in the stats schedule release. |
| `season_type` | String | Schedule season type: regular-season, or nba-cup for an NBA Cup final (which does not count in the standings). |
| `home_team` | String | Designated home team (stats.nba.com abbreviation). |
| `away_team` | String | Away team (stats.nba.com abbreviation). |
| `arena` | String | Arena the game was played in. |
| `city` | String | City of the arena. |
| `country` | String | Country of the arena. |
| `reason` | String | Why the game is neutral: international (played abroad) or cup_knockout (an NBA Cup knockout game in Las Vegas). |
| `source_url` | String | The source that documents the game. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba import nba_neutral_site_games
games = nba_neutral_site_games([2026])

# WNBA

nba_neutral_site_games(league_id="10")
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
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 6.0). |
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
    (pl.col("is_garbage_time_ctg") == False) & (pl.col("is_heave_possession") == False)
)
print(clean["is_transition_ctg"].mean())
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

### nba_player_factor_rapm {#nba_player_factor_rapm}

`nba_player_factor_rapm(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', min_poss: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Player RAPM split into Dean Oliver's four factors, offense and defense, per player-season.

The definitions are in `sportsdataverse._common.factor_rapm`: `o_efg_rapm + o_tov_rapm +
o_orb_rapm + o_ftr_rapm + o_other_rapm` is `o_rapm` and the five `d_*` components add up to
`d_rapm`, each season fit on its own.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `min_poss` | `int \| None` | `None` | The rank gate, offensive plus defensive possessions. Defaults to 400 (NBA) or 200 (WNBA). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, player_id)`: `off_poss` / `def_poss`, `rapm` / `o_rapm` / `d_rapm`, the ten components (the four factors and the remainder, each way; per 100 possessions, higher is better), `<m>_rank` / `<m>_pct` within the season among players with at least `min_poss` possessions, `<m>_se` (clustered by game), `<m>_n`, and the `pts_per_fga` and `ridge_alpha` of the fit. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `player_id` | Int64 | stats.nba.com player id. |
| `off_poss` | Int64 | Offensive possessions the player was on the floor for (with all ten lineup slots and counts known). |
| `def_poss` | Int64 | Defensive possessions the player was on the floor for (with all ten lineup slots and counts known). |
| `rapm` | Float64 | Plain RAPM per 100 possessions: o_rapm + d_rapm. |
| `o_rapm` | Float64 | Offensive plain RAPM per 100 possessions (nba_rapm's single-season ridge at the fixed NBA penalty, see ridge_alpha, shrunk to zero); equals o_efg_rapm + o_tov_rapm + o_orb_rapm + o_ftr_rapm + o_other_rapm. Fit on the possessions release (2026-08-13). At the same penalty it tracks nba_player_impact's (fit 2026-10-12 on a re-compile with the same possessions and some different lineups): 2025-26 corr 0.997, max 1.31. nba_player_impact releases published before November 2026 were fit at a per-season penalty, so they differ in scale until re-published. |
| `d_rapm` | Float64 | Defensive plain RAPM per 100 possessions, sign-flipped so higher is better; equals d_efg_rapm + d_tov_rapm + d_drb_rapm + d_ftr_rapm + d_other_rapm. Fit at the fixed NBA penalty (see ridge_alpha) on the possessions release (2026-08-13). At the same penalty it tracks nba_player_impact's (fit 2026-10-12 on a re-compile with the same possessions and some different lineups): 2025-26 corr 0.997, max 1.31. nba_player_impact releases published before November 2026 were fit at a per-season penalty, so they differ in scale until re-published. |
| `o_efg_rapm` | Float64 | Offensive shooting (eFG) component of o_rapm: the ridge fit to field-goal points less pts_per_fga per attempt, per 100 offensive possessions. |
| `o_tov_rapm` | Float64 | Offensive turnover component of o_rapm: the ridge fit to -pts_per_fga per turnover, per 100 offensive possessions (positive = fewer turnovers). |
| `o_orb_rapm` | Float64 | Offensive rebounding component of o_rapm: the ridge fit to +pts_per_fga per offensive rebound, per 100 offensive possessions. |
| `o_ftr_rapm` | Float64 | Offensive free-throw component of o_rapm, on possessions with a free-throw attempt: the ridge fit to free-throw points less pts_per_fga per chance that ended in neither a shot nor a turnover (FT rate and FT%), per 100 offensive possessions. |
| `o_other_rapm` | Float64 | Offensive remainder of o_rapm (bookkeeping, not a skill): the ridge fit to what the four factors leave of points - pts_per_fga, i.e. -pts_per_fga per chance with no shot, turnover or free throw (a period running out) and +pts_per_fga per shot or turnover beyond the recorded chances (an unrecorded offensive rebound); non-zero on about 1% of possessions. |
| `d_efg_rapm` | Float64 | Defensive shooting (opponent eFG) component of d_rapm, per 100 possessions, sign-flipped so higher is better: positive = the opponent shoots worse. |
| `d_tov_rapm` | Float64 | Defensive turnover component of d_rapm, per 100 possessions, sign-flipped so higher is better: positive = forces more turnovers. |
| `d_drb_rapm` | Float64 | Defensive rebounding component of d_rapm, per 100 possessions, sign-flipped so higher is better: positive = fewer opponent offensive rebounds. |
| `d_ftr_rapm` | Float64 | Defensive free-throw component of d_rapm, per 100 possessions, sign-flipped so higher is better: positive = fewer opponent free-throw points. |
| `d_other_rapm` | Float64 | Defensive remainder of d_rapm (bookkeeping, not a skill), per 100 possessions, sign-flipped so higher is better: the opponent's o_other part. |
| `rapm_rank` | Float64 | Rank of rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) rapm repeats at Spearman 0.322, so single-season ranks are noisy. |
| `rapm_pct` | Float64 | Percentile of rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) rapm repeats at Spearman 0.322, so single-season ranks are noisy. |
| `o_rapm_rank` | Float64 | Rank of o_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_rapm repeats at Spearman 0.292, so single-season ranks are noisy. |
| `o_rapm_pct` | Float64 | Percentile of o_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_rapm repeats at Spearman 0.292, so single-season ranks are noisy. |
| `d_rapm_rank` | Float64 | Rank of d_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_rapm repeats at Spearman 0.361, so single-season ranks are noisy. |
| `d_rapm_pct` | Float64 | Percentile of d_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_rapm repeats at Spearman 0.361, so single-season ranks are noisy. |
| `o_efg_rapm_rank` | Float64 | Rank of o_efg_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_efg_rapm repeats at Spearman 0.308, so single-season ranks are noisy. |
| `o_efg_rapm_pct` | Float64 | Percentile of o_efg_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_efg_rapm repeats at Spearman 0.308, so single-season ranks are noisy. |
| `o_tov_rapm_rank` | Float64 | Rank of o_tov_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_tov_rapm repeats at Spearman 0.334, so single-season ranks are noisy. |
| `o_tov_rapm_pct` | Float64 | Percentile of o_tov_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_tov_rapm repeats at Spearman 0.334, so single-season ranks are noisy. |
| `o_orb_rapm_rank` | Float64 | Rank of o_orb_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_orb_rapm repeats at Spearman 0.423, so single-season ranks are noisy. |
| `o_orb_rapm_pct` | Float64 | Percentile of o_orb_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_orb_rapm repeats at Spearman 0.423, so single-season ranks are noisy. |
| `o_ftr_rapm_rank` | Float64 | Rank of o_ftr_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_ftr_rapm repeats at Spearman 0.366, so single-season ranks are noisy. |
| `o_ftr_rapm_pct` | Float64 | Percentile of o_ftr_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_ftr_rapm repeats at Spearman 0.366, so single-season ranks are noisy. |
| `o_other_rapm_rank` | Float64 | Rank of o_other_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_other_rapm repeats at Spearman 0.259, so single-season ranks are noisy. |
| `o_other_rapm_pct` | Float64 | Percentile of o_other_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) o_other_rapm repeats at Spearman 0.259, so single-season ranks are noisy. |
| `d_efg_rapm_rank` | Float64 | Rank of d_efg_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_efg_rapm repeats at Spearman 0.247, so single-season ranks are noisy. |
| `d_efg_rapm_pct` | Float64 | Percentile of d_efg_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_efg_rapm repeats at Spearman 0.247, so single-season ranks are noisy. |
| `d_tov_rapm_rank` | Float64 | Rank of d_tov_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_tov_rapm repeats at Spearman 0.387, so single-season ranks are noisy. |
| `d_tov_rapm_pct` | Float64 | Percentile of d_tov_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_tov_rapm repeats at Spearman 0.387, so single-season ranks are noisy. |
| `d_drb_rapm_rank` | Float64 | Rank of d_drb_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_drb_rapm repeats at Spearman 0.229, so single-season ranks are noisy. |
| `d_drb_rapm_pct` | Float64 | Percentile of d_drb_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_drb_rapm repeats at Spearman 0.229, so single-season ranks are noisy. |
| `d_ftr_rapm_rank` | Float64 | Rank of d_ftr_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_ftr_rapm repeats at Spearman 0.341, so single-season ranks are noisy. |
| `d_ftr_rapm_pct` | Float64 | Percentile of d_ftr_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_ftr_rapm repeats at Spearman 0.341, so single-season ranks are noisy. |
| `d_other_rapm_rank` | Float64 | Rank of d_other_rapm within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (off_poss + def_poss, default 400 (NBA) or 200 (WNBA)); null below 10 qualifiers. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_other_rapm repeats at Spearman 0.376, so single-season ranks are noisy. |
| `d_other_rapm_pct` | Float64 | Percentile of d_other_rapm among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. Year over year (2024-25 to 2025-26, 293 players over 2,000 possessions) d_other_rapm repeats at Spearman 0.376, so single-season ranks are noisy. |
| `rapm_se` | Float64 | Standard error of rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_rapm_se` | Float64 | Standard error of o_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_rapm_se` | Float64 | Standard error of d_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_efg_rapm_se` | Float64 | Standard error of o_efg_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_tov_rapm_se` | Float64 | Standard error of o_tov_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_orb_rapm_se` | Float64 | Standard error of o_orb_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_ftr_rapm_se` | Float64 | Standard error of o_ftr_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `o_other_rapm_se` | Float64 | Standard error of o_other_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_efg_rapm_se` | Float64 | Standard error of d_efg_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_tov_rapm_se` | Float64 | Standard error of d_tov_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_drb_rapm_se` | Float64 | Standard error of d_drb_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_ftr_rapm_se` | Float64 | Standard error of d_ftr_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `d_other_rapm_se` | Float64 | Standard error of d_other_rapm, clustered by game (the ridge sandwich): sampling noise around the shrunk estimate, not the shrinkage bias. |
| `rapm_n` | Int64 | Sample size of rapm: off_poss + def_poss. |
| `o_rapm_n` | Int64 | Sample size of o_rapm: off_poss. |
| `d_rapm_n` | Int64 | Sample size of d_rapm: def_poss. |
| `o_efg_rapm_n` | Int64 | Sample size of o_efg_rapm: off_poss. |
| `o_tov_rapm_n` | Int64 | Sample size of o_tov_rapm: off_poss. |
| `o_orb_rapm_n` | Int64 | Sample size of o_orb_rapm: off_poss. |
| `o_ftr_rapm_n` | Int64 | Sample size of o_ftr_rapm: off_poss. |
| `o_other_rapm_n` | Int64 | Sample size of o_other_rapm: off_poss. |
| `d_efg_rapm_n` | Int64 | Sample size of d_efg_rapm: def_poss. |
| `d_tov_rapm_n` | Int64 | Sample size of d_tov_rapm: def_poss. |
| `d_drb_rapm_n` | Int64 | Sample size of d_drb_rapm: def_poss. |
| `d_ftr_rapm_n` | Int64 | Sample size of d_ftr_rapm: def_poss. |
| `d_other_rapm_n` | Int64 | Sample size of d_other_rapm: def_poss. |
| `pts_per_fga` | Float64 | The season's points per field-goal attempt, the value of one shot used in the split (pts_per_fga per turnover, offensive rebound and free-throw chance). |
| `ridge_alpha` | Float64 | The ridge penalty of the fit, shared by every component so they add up: plain RAPM's fixed league value (NBA 2,683, WNBA 1,931; sportsdataverse.nba.nba_rapm.RAPM_PENALTY), the same in every season. Rows fit before November 2026 carry RidgeCV's per-season leave-one-out pick instead, on a scale that changed with it. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` or `season_type` is not one of the four above. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_player_factor_rapm import nba_player_factor_rapm
split = nba_player_factor_rapm(2025)
split.select("player_id", "rapm", "o_efg_rapm", "o_tov_rapm", "d_drb_rapm").head()

# The six-factor view (free throws folded into shooting)

six = split.select(
    "season", "player_id",
    o_shooting=pl.col("o_efg_rapm") + pl.col("o_ftr_rapm"),
    o_tov=pl.col("o_tov_rapm"), o_reb=pl.col("o_orb_rapm"),
    d_shooting=pl.col("d_efg_rapm") + pl.col("d_ftr_rapm"),
    d_tov=pl.col("d_tov_rapm"), d_reb=pl.col("d_drb_rapm"),
)
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
| `off_rtg_on_rank` | Float64 | Rank of off_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose off_rtg_on_n is at least 1,700 offensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `def_rtg_on_rank` | Float64 | Rank of def_rtg_on within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose def_rtg_on_n is at least 1,600 defensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_on_rank` | Float64 | Rank of net_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose net_rtg_on_n is at least 3,200 possessions on the floor (offense plus defense) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `off_rtg_diff_rank` | Float64 | Rank of off_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose off_rtg_diff_n is at least 2,400 offensive possessions on the thinner side (on or off the floor) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `def_rtg_diff_rank` | Float64 | Rank of def_rtg_diff within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose def_rtg_diff_n is at least 3,400 defensive possessions on the thinner side (on or off the floor) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_diff_rank` | Float64 | Rank of net_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose net_rtg_diff_n is at least 7,900 possessions on the thinner side (offense plus defense, on or off the floor) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
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
