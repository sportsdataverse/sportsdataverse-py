---
title: "NBA — additional Python functions — Analytics: nba_player"
sidebar_label: "Analytics: nba_player"
sidebar_position: 18
description: "NBA — additional Python functions — Analytics: nba_player — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_player

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

### nba_player_game_expected_box {#nba_player_game_expected_box}

`nba_player_game_expected_box(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NBA points, rebounds and assists per player-game against his season, per minute and usage-adjusted.

For every played player-game: `<s>_base` is his season per-minute rate times the game's
minutes; `<s>_exp` scales that by the game's usage and minutes against his season's
(a Poisson model fitted on earlier seasons, scored on later held-out ones). Both use the
season without this game (over the regular season (a playoff or play-in game is measured against it)), so a game is never in its own baseline.
`<s>_exp` is null before the model's first held-out season and where a usage is unknown.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (2026 is 2025-26). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(game_id, athlete_id)`: `season`, `season_type`, `team_id`, `minutes`, `usg_pct`, `pts` / `reb` / `ast`, the season-without-the-game `rate_games` / `rate_minutes` / `rate_usg_pct`, and `pts_base` / `reb_base` / `ast_base` / `pts_exp` / `reb_exp` / `ast_exp`. A season without a release returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `season_type` | Int64 | ESPN season type of the game (2 regular season, 3 postseason, 5 NBA play-in). |
| `game_id` | Int64 | ESPN game id, the join key to the schedule and the player box score. |
| `team_id` | Int64 | ESPN team id of the player's team in the game. |
| `athlete_id` | Int64 | ESPN athlete id of the player, the join key to the player box score. |
| `minutes` | Float64 | Minutes played in the game (above 0: a player who did not play has no row). |
| `usg_pct` | Float64 | Usage: 100 x uses x (team minutes / 5) / (minutes x team uses), uses = FGA + 0.44 FTA + TOV, the team sums over that team-game's players; null where a player of the team-game lacks a count. |
| `pts` | Int64 | Points in the game. |
| `reb` | Int64 | Rebounds in the game. |
| `ast` | Int64 | Assists in the game. |
| `rate_games` | Int64 | Games of the season without this one (the pros' regular season, every college game) that the expectations read. |
| `rate_minutes` | Float64 | Minutes of those games. |
| `rate_usg_pct` | Float64 | Season usage without this game: 100 x sum(uses) / sum(minutes x team uses / (team minutes / 5)); null where no game of it has a usage. |
| `pts_base` | Float64 | Expected points at the per-minute rate: the season-without-this-game points per minute times the game's minutes; null when that season has no minutes. |
| `reb_base` | Float64 | Expected rebounds at the per-minute rate, as pts_base. |
| `ast_base` | Float64 | Expected assists at the per-minute rate, as pts_base. |
| `pts_exp` | Float64 | Usage-adjusted expected points: pts_base x exp(a + b_usg ln(usg_pct / rate_usg_pct) + b_min ln(minutes / rate minutes per game)), a Poisson fit on earlier seasons (sportsdataverse._common.player_box_expected); null before the model's first held-out season or without both usages. |
| `reb_exp` | Float64 | Usage-adjusted expected rebounds, as pts_exp. |
| `ast_exp` | Float64 | Usage-adjusted expected assists, as pts_exp. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_player_box_expected import nba_player_game_expected_box
games = nba_player_game_expected_box(2026)
games.with_columns((pl.col("pts") - pl.col("pts_exp")).alias("pts_vs_exp")).sort("pts_vs_exp").tail()
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

One row per `(season, team_id, player_id)`: `games` on the floor and `team_games`, then for `_on` and `_off` the `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions); `<rating>_diff` (on minus off); `<m>_rank` / `<m>_pct` for the on and diff ratings (within the season, among players with at least `min_poss` both ways) and `<m>_n` for every rating. `poss_on + poss_off` is the team's possessions. Then his span with the team: `team_games_span` (the team's games from his first game for it, or the day after his last for an earlier team that season, to the day before his first for a later team, else the season's end, by the schedule's `game_date`), `span_first_date` / `span_last_date`, the off side over that span (`<total>_off_span` and `<rating>_off_span`, not ranked) and `<rating>_diff_span` (on minus it): for a player traded or signed mid-season, the on/off over his time with the team. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

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

### nba_player_season_box_rates {#nba_player_season_box_rates}

`nba_player_season_box_rates(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NBA per-36 points, rebounds and assists per player-season, ranked, with the season usage.

Over the regular season (a playoff or play-in game is measured against it), from ESPN's player box. `pts_per36` / `reb_per36` /
`ast_per36` are 36 x the total / minutes, each with `_n` (the minutes) and
`_rank` / `_pct` among the season's players whose minutes reach the rate's gate (the
minutes at which it is half signal on odd / even games). `usg_pct` is the season usage
(FGA + 0.44 FTA + TOV over the team's while on the floor), not ranked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (2026 is 2025-26). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, athlete_id)` (the ESPN id): `team_id` (his last game's team), `games`, `minutes`, `pts` / `reb` / `ast`, `usg_pct` with `usg_pct_n`, and the three per-36 rates with `_n` / `_rank` / `_pct`. A season without a release returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `athlete_id` | Int64 | ESPN athlete id of the player, the join key to the player box score. |
| `team_id` | Int64 | ESPN team id of the player's last game of the season (a traded player has one row). |
| `games` | Int64 | Games played (the pros' regular season, every college game). |
| `minutes` | Float64 | Minutes played in those games. |
| `pts` | Int64 | Points in those games. |
| `reb` | Int64 | Rebounds in those games. |
| `ast` | Int64 | Assists in those games. |
| `usg_pct` | Float64 | Season usage: 100 x sum(uses) / sum(minutes x team uses / (team minutes / 5)), uses = FGA + 0.44 FTA + TOV; not ranked. |
| `usg_pct_n` | Int64 | Minutes of the games with a usage behind usg_pct. |
| `pts_per36` | Float64 | Points per 36 minutes: 36 x pts / minutes. |
| `pts_per36_n` | Int64 | Minutes behind pts_per36. |
| `reb_per36` | Float64 | Rebounds per 36 minutes: 36 x reb / minutes. |
| `reb_per36_n` | Int64 | Minutes behind reb_per36. |
| `ast_per36` | Float64 | Assists per 36 minutes: 36 x ast / minutes. |
| `ast_per36_n` | Int64 | Minutes behind ast_per36. |
| `pts_per36_rank` | Float64 | Rank of pts_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `pts_per36_pct` | Float64 | Percentile of pts_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |
| `reb_per36_rank` | Float64 | Rank of reb_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `reb_per36_pct` | Float64 | Percentile of reb_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |
| `ast_per36_rank` | Float64 | Rank of ast_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `ast_per36_pct` | Float64 | Percentile of ast_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |

**Example**

```python
from sportsdataverse.nba.nba_player_box_expected import nba_player_season_box_rates
rates = nba_player_season_box_rates(2026)
rates.sort("pts_per36_rank").select("athlete_id", "minutes", "pts_per36", "pts_per36_pct").head()
```

### nba_player_shot_zones {#nba_player_shot_zones}

`nba_player_shot_zones(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3.

One row per player, team and season over the regular season (a traded player has a row per
team). Zones and the unlocated attempts are as in the module docstring.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id)` (stats.nba.com ids, Int64) with `player_name`, `games`, `fga`, `fgm`, `unlocated_fga` and, for each zone `z` (`rim`, `mid`, `corner_3`, `above_break_3`), `{z}_fga`, `{z}_fgm`, `{z}_ast` (assisted makes), `{z}_fg_pct`, `{z}_freq` (share of the located attempts) and `{z}_ast_pct` (share of the makes assisted). The four `{z}_fga` plus `unlocated_fga` add up to `fga`. Every rate has `<m>_n` (its denominator), `<m>_rank` (1 = highest) and `<m>_pct` (the Weibull percentile), ranked within the season among players whose `<m>_n` reaches the rate's gate and null under ten players. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id` | Int64 | stats.nba.com player (person) id. |
| `player_name` | String | Player name as the shots release spells it. |
| `games` | Int64 | The player's regular-season games with at least one field-goal attempt for this team. |
| `fga` | Int64 | Field-goal attempts by the player (regular-season games). |
| `fgm` | Int64 | Field goals made by the player. |
| `unlocated_fga` | Int64 | Attempts by the player with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_fgm` | Int64 | Makes by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_ast` | Int64 | Assisted makes by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `mid_fga` | Int64 | Attempts by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_fgm` | Int64 | Makes by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_ast` | Int64 | Assisted makes by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `corner_3_fga` | Int64 | Attempts by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_fgm` | Int64 | Makes by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_ast` | Int64 | Assisted makes by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `above_break_3_fga` | Int64 | Attempts by the player from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the player from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the player from above the break (every other three). |
| `rim_fg_pct` | Float64 | rim_fgm / rim_fga; null with no attempt. |
| `rim_freq` | Float64 | rim_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `rim_ast_pct` | Float64 | Share of rim_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `mid_fg_pct` | Float64 | mid_fgm / mid_fga; null with no attempt. |
| `mid_freq` | Float64 | mid_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `mid_ast_pct` | Float64 | Share of mid_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `corner_3_fg_pct` | Float64 | corner_3_fgm / corner_3_fga; null with no attempt. |
| `corner_3_freq` | Float64 | corner_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `corner_3_ast_pct` | Float64 | Share of corner_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `above_break_3_fg_pct` | Float64 | above_break_3_fgm / above_break_3_fga; null with no attempt. |
| `above_break_3_freq` | Float64 | above_break_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `above_break_3_ast_pct` | Float64 | Share of above_break_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `rim_fg_pct_n` | Int64 | Sample size of rim_fg_pct: rim_fga. |
| `rim_freq_n` | Int64 | Sample size of rim_freq: located attempts (fga - unlocated_fga). |
| `rim_ast_pct_n` | Int64 | Sample size of rim_ast_pct: makes in the zone. |
| `mid_fg_pct_n` | Int64 | Sample size of mid_fg_pct: mid_fga. |
| `mid_freq_n` | Int64 | Sample size of mid_freq: located attempts (fga - unlocated_fga). |
| `mid_ast_pct_n` | Int64 | Sample size of mid_ast_pct: makes in the zone. |
| `corner_3_fg_pct_n` | Int64 | Sample size of corner_3_fg_pct: corner_3_fga. |
| `corner_3_freq_n` | Int64 | Sample size of corner_3_freq: located attempts (fga - unlocated_fga). |
| `corner_3_ast_pct_n` | Int64 | Sample size of corner_3_ast_pct: makes in the zone. |
| `above_break_3_fg_pct_n` | Int64 | Sample size of above_break_3_fg_pct: above_break_3_fga. |
| `above_break_3_freq_n` | Int64 | Sample size of above_break_3_freq: located attempts (fga - unlocated_fga). |
| `above_break_3_ast_pct_n` | Int64 | Sample size of above_break_3_ast_pct: makes in the zone. |
| `rim_fg_pct_rank` | Float64 | Rank of rim_fg_pct within the season (the highest ranks 1; ties average), among players whose rim_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_freq_rank` | Float64 | Rank of rim_freq within the season (the highest ranks 1; ties average), among players whose rim_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_ast_pct_rank` | Float64 | Rank of rim_ast_pct within the season (the highest ranks 1; ties average), among players whose rim_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_fg_pct_rank` | Float64 | Rank of mid_fg_pct within the season (the highest ranks 1; ties average), among players whose mid_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_freq_rank` | Float64 | Rank of mid_freq within the season (the highest ranks 1; ties average), among players whose mid_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_ast_pct_rank` | Float64 | Rank of mid_ast_pct within the season (the highest ranks 1; ties average), among players whose mid_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_fg_pct_rank` | Float64 | Rank of corner_3_fg_pct within the season (the highest ranks 1; ties average), among players whose corner_3_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_freq_rank` | Float64 | Rank of corner_3_freq within the season (the highest ranks 1; ties average), among players whose corner_3_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_ast_pct_rank` | Float64 | Rank of corner_3_ast_pct within the season (the highest ranks 1; ties average), among players whose corner_3_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_fg_pct_rank` | Float64 | Rank of above_break_3_fg_pct within the season (the highest ranks 1; ties average), among players whose above_break_3_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_freq_rank` | Float64 | Rank of above_break_3_freq within the season (the highest ranks 1; ties average), among players whose above_break_3_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_ast_pct_rank` | Float64 | Rank of above_break_3_ast_pct within the season (the highest ranks 1; ties average), among players whose above_break_3_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_fg_pct_pct` | Float64 | Percentile of rim_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_freq_pct` | Float64 | Percentile of rim_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_ast_pct_pct` | Float64 | Percentile of rim_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_fg_pct_pct` | Float64 | Percentile of mid_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_freq_pct` | Float64 | Percentile of mid_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_ast_pct_pct` | Float64 | Percentile of mid_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_fg_pct_pct` | Float64 | Percentile of corner_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_freq_pct` | Float64 | Percentile of corner_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_ast_pct_pct` | Float64 | Percentile of corner_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_fg_pct_pct` | Float64 | Percentile of above_break_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_freq_pct` | Float64 | Percentile of above_break_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_ast_pct_pct` | Float64 | Percentile of above_break_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.nba.nba_shot_zone_profiles import nba_player_shot_zones
zones = nba_player_shot_zones(2025)
zones.sort("corner_3_fga", descending=True).select("player_name", "corner_3_fga", "corner_3_fg_pct").head()

# WNBA via ``league_id``

wnba = nba_player_shot_zones(2025, league_id="10")
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
