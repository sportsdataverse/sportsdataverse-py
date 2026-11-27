---
title: "College football — additional Python functions — Analytics: cfb_player–play_type"
sidebar_label: "Analytics: cfb_player–play_type"
sidebar_position: 13
description: "College football — additional Python functions — Analytics: cfb_player–play_type — function reference in sdv-py, the SportsDataverse Python package."
---
# College football — additional Python functions — Analytics: cfb_player–play_type

### cfb_player_havoc {#cfb_player_havoc}

`cfb_player_havoc(seasons: 'int | list[int]', *, min_games: 'int' = 6, player_box: 'pl.DataFrame | None' = None, defensive_players: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

CFB player havoc per season: tackles for loss, sacks, pass breakups, interceptions and forced fumbles, per game.

These are counting stats, not a grade. A defender's havoc is his tackles for loss, pass
breakups, interceptions and forced fumbles, the player version of the team havoc rate. Sacks
are listed and ranked on their own but not added again, because a sack is already a tackle for
loss. Events are counted, not plays: a strip-sack is two events for the same player, where team
havoc counts that play once. Nothing is adjusted for opponent, scheme or position, so an edge
rusher and a corner are ranked on the same scale.

Tackles for loss (half credit on a shared one), sacks, pass breakups and interceptions come
from ESPN's player box score. In the college box, `passesDefended` excludes the defender's
interceptions, so the two are added without double counting. Forced fumbles come from the
play-by-play (`load_cfb_adv_defensive_players`), which only credits a forced fumble when
ESPN's play text names the player. Measured on 2025, that is about 0.5 a game, so this column
undercounts. College football has no public snap counts, so every rate is per game, and a game
counts only when the player has a defensive or interception box line in it or the play text
credits him with a forced fumble in it. ESPN's defensive
box covers 935 of 958 games in 2025 but 473 in 2023 and 154 in 2021, so earlier seasons are
partial. No PFF data is used.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons (2004 or later for the box release; the defensive box lines are near-complete from 2025). |
| `min_games` | `int` | `6` | A rate is ranked only for a player with at least this many games (its `_n`). Defaults to 6, half a regular season. |
| `player_box` | `DataFrame \| None` | `None` | An ESPN player box frame shaped like `load_cfb_player_box` (no download). |
| `defensive_players` | `DataFrame \| None` | `None` | A frame shaped like `load_cfb_adv_defensive_players` (no download). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, athlete_id)` (ESPN ids, Int64): `athlete_name`, `games`, the season totals `havoc` / `tfl` / `sacks` / `pass_breakups` / `interceptions` / `forced_fumbles` (Float64, since a shared tackle for loss is a half), and for each total its `<stat>_per_game` with `_n` (games), `_rank` (1 is the most) and the Weibull `_pct`. These are ranked within the season among players with at least `min_games` games, and are null below that, or when fewer than 10 players qualify. A season with no box lines gives that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the player's games (every game in the box release, bowls included). |
| `team_id` | Int64 | ESPN team id of the defender's team (Int64). |
| `athlete_id` | Int64 | ESPN athlete id (Int64). |
| `athlete_name` | String | ESPN display name, from his last box line of the season. |
| `games` | Int64 | Games with a defensive or interception box line for the player (college football has no public snap counts); the denominator of every per-game rate. |
| `havoc` | Float64 | Havoc events: tfl + pass_breakups + interceptions + forced_fumbles. Sacks are not added again, as a sack is already a tackle for loss, and a strip-sack counts twice. A counting stat, not a grade. |
| `tfl` | Float64 | Tackles for loss from ESPN's player box, sacks included; a shared tackle for loss is a half. |
| `sacks` | Float64 | Sacks from ESPN's player box; a shared sack is a half. Already inside tfl. |
| `pass_breakups` | Float64 | Passes defended from ESPN's college player box, which excludes the defender's interceptions. |
| `interceptions` | Float64 | Interceptions made, from the box's interceptions category (never the passing category's thrown interceptions). |
| `forced_fumbles` | Float64 | Forced fumbles credited in ESPN's play text (load_cfb_adv_defensive_players), keyed to the box by team and name. Undercounts: about 0.5 a game in 2025. |
| `havoc_per_game` | Float64 | Per-game havoc events (tackles for loss + pass breakups + interceptions + forced fumbles): the season total over games. |
| `havoc_per_game_n` | Int64 | Sample size of havoc_per_game: the player's games. |
| `havoc_per_game_rank` | Float64 | Rank of havoc_per_game within the season, 1 = most, average ties; null under min_games or with fewer than 10 qualifiers. |
| `havoc_per_game_pct` | Float64 | Weibull percentile of havoc_per_game within the season, 100 * (n + 1 - rank) / (n + 1) over the n ranked players; null where the rank is. |
| `tfl_per_game` | Float64 | Per-game tackles for loss (sacks included; a shared one is a half): the season total over games. |
| `tfl_per_game_n` | Int64 | Sample size of tfl_per_game: the player's games. |
| `tfl_per_game_rank` | Float64 | Rank of tfl_per_game within the season, 1 = most, average ties; null under min_games or with fewer than 10 qualifiers. |
| `tfl_per_game_pct` | Float64 | Weibull percentile of tfl_per_game within the season, 100 * (n + 1 - rank) / (n + 1) over the n ranked players; null where the rank is. |
| `sacks_per_game` | Float64 | Per-game sacks (a shared one is a half): the season total over games. |
| `sacks_per_game_n` | Int64 | Sample size of sacks_per_game: the player's games. |
| `sacks_per_game_rank` | Float64 | Rank of sacks_per_game within the season, 1 = most, average ties; null under min_games or with fewer than 10 qualifiers. |
| `sacks_per_game_pct` | Float64 | Weibull percentile of sacks_per_game within the season, 100 * (n + 1 - rank) / (n + 1) over the n ranked players; null where the rank is. |
| `pass_breakups_per_game` | Float64 | Per-game pass breakups (ESPN's college passesDefended, which excludes interceptions): the season total over games. |
| `pass_breakups_per_game_n` | Int64 | Sample size of pass_breakups_per_game: the player's games. |
| `pass_breakups_per_game_rank` | Float64 | Rank of pass_breakups_per_game within the season, 1 = most, average ties; null under min_games or with fewer than 10 qualifiers. |
| `pass_breakups_per_game_pct` | Float64 | Weibull percentile of pass_breakups_per_game within the season, 100 * (n + 1 - rank) / (n + 1) over the n ranked players; null where the rank is. |
| `interceptions_per_game` | Float64 | Per-game interceptions made: the season total over games. |
| `interceptions_per_game_n` | Int64 | Sample size of interceptions_per_game: the player's games. |
| `interceptions_per_game_rank` | Float64 | Rank of interceptions_per_game within the season, 1 = most, average ties; null under min_games or with fewer than 10 qualifiers. |
| `interceptions_per_game_pct` | Float64 | Weibull percentile of interceptions_per_game within the season, 100 * (n + 1 - rank) / (n + 1) over the n ranked players; null where the rank is. |
| `forced_fumbles_per_game` | Float64 | Per-game forced fumbles named in ESPN's play text: the season total over games. |
| `forced_fumbles_per_game_n` | Int64 | Sample size of forced_fumbles_per_game: the player's games. |
| `forced_fumbles_per_game_rank` | Float64 | Rank of forced_fumbles_per_game within the season, 1 = most, average ties; null under min_games or with fewer than 10 qualifiers. |
| `forced_fumbles_per_game_pct` | Float64 | Weibull percentile of forced_fumbles_per_game within the season, 100 * (n + 1 - rank) / (n + 1) over the n ranked players; null where the rank is. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If a season is before 2004 (raised by the loaders). |

**Example**

```python
from sportsdataverse.cfb import cfb_player_havoc
havoc = cfb_player_havoc(2025)
havoc.sort("havoc_per_game_rank").select(
    "athlete_name", "games", "havoc", "havoc_per_game", "havoc_per_game_pct"
).head()
```

### cfb_playoff_seeds {#cfb_playoff_seeds}

`cfb_playoff_seeds(standings: 'FrameLike', rankings: 'FrameLike | None' = None, playoff_seeds: 'int' = 12, *, season: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | Any'`

Assign College Football Playoff seeds (12-team straight seeding, by season).

Two selection rules, picked by `season`; both seed the field straight by
ranking order (no champion bump to the top four):

* **2026+** (`season=None` or `>= 2026`, the current rule): the ACC,
  Big Ten, Big 12 and SEC champions are in regardless of ranking; so is
  the highest-ranked team (champion or not) of the American, Conference
  USA, MAC, Mountain West, Pac-12 and Sun Belt; and Notre Dame (`"Notre Dame"` or
  ESPN id `"87"`) when it ranks inside the top `playoff_seeds`; the
  rest of the field is at-large.
* **2024-2025** (`season < 2026`): the 5 highest-ranked conference
  champions are guaranteed inclusion, the rest is at-large.

An automatic qualifier ranked outside the top `playoff_seeds` displaces
the lowest-ranked at-large team and takes the lowest seed. Conference
names match both cfbd and short spellings (e.g. `"American Athletic"` /
`"American"`).

When `rankings` is None the ordering falls back to the standings
tiebreaker metrics — `win_pct` desc, then `sov`, `sos`, `pd`
desc, then team name (documented deterministic fallback; a committee
ranking is the intended input). Notre Dame's "ranked inside the field"
test reads its position in that same ordering.

Differs from cfbseedR 0.3.0's selection rule (`autobid`) in one place:
cfbseedR tests Notre Dame's committee `rank` only (no bid without
`rankings`). Both match Notre Dame as `"Notre Dame"` or `"87"`, and cfbseedR's
`cfb_standings()` keys the rule by a `season`-keyed input as
`cfb_standings` does here; its
`cfb_simulations()` takes an explicit `autobid` (default `"2026"`)
where `sportsdataverse.cfb.cfb_simulations` follows the season.
Neither engine reads `postseason_eligible` when seeding: an ineligible team can still take a bid.

Called on its own, this function cannot see a season: pass `season=`
for standings of a pre-2026 season (`cfb_standings(...,
playoff_seeds=...)` reads it from `season`-keyed games instead).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `standings` | `FrameLike` |  | Output of `cfb_standings` (needs `sim`, `team`, `conference`, `conf_champ`, `win_pct`, `sov`, `sos`, `pd`). |
| `rankings` | `FrameLike \| None` | `None` | Optional frame with columns `team` and `rank` (1 = best). Unranked teams order after ranked ones by the fallback. |
| `playoff_seeds` | `int` | `12` | Field size (default 12). Automatic qualifiers are capped at this size. |
| `season` | `int \| None` | `None` | Season whose selection rule applies (default `None` = the current, 2026 rule). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The standings frame with a `seed` column (Int64; null for teams outside the field), sorted by sim and seed.

| col_name | type | description |
|---|---|---|
| `sim` | Int64 | Season or simulation identifier the standings row belongs to. |
| `team` | String | Team name (join key across the seedr engine frames). |
| `conference` | String | Conference the team belongs to; null or "FBS Independents" marks an independent. |
| `games` | Int64 | Total games played across all game types (regular season, conference championship and postseason). |
| `wins` | Int64 | Wins across all played games (conference championship and postseason included). |
| `losses` | Int64 | Losses across all played games. |
| `ties` | Int64 | Ties across all played games. |
| `win_pct` | Float64 | Overall win percentage - (wins + 0.5 * ties) / games, 0.0 when no games have been played. |
| `pd` | Float64 | Point differential (points for minus points against, via game margins) summed over all played games. |
| `conf_games` | Int64 | Number of conference regular-season games played (both teams in the same conference; CONF_CHAMP games excluded). |
| `conf_wins` | Int64 | Wins in conference regular-season games. |
| `conf_losses` | Int64 | Losses in conference regular-season games. |
| `conf_ties` | Int64 | Ties in conference regular-season games. |
| `conf_pct` | Float64 | Conference win percentage - (conf_wins + 0.5 * conf_ties) / conf_games, 0.0 with no conference games; the primary sort key for conference ranks. |
| `conf_pd` | Float64 | Point differential summed over conference regular-season games only; the POINTS-depth tiebreaker rung. |
| `sov` | Float64 | Strength of victory, conference-REG-scoped (unlike nflseedR's overall version) - the pooled sos formula over conference victories only; 0.0 for independents or teams without conference wins. |
| `sos` | Float64 | Strength of schedule, conference-REG-scoped (unlike nflseedR's overall version) - conference opponents' summed conference wins (ties half) over their summed conference games, one term per conference game played (pooled, as cfbseedR); 0.0 for independents. |
| `conf_rank` | Int64 | Rank within the conference from the tiebreaker cascade (1 = best); null for independents. |
| `conf_champ` | Boolean | Whether the team is its conference's champion - the CONF_CHAMP game winner when one was played, otherwise the conference's rank-1 team; always false for independents. |
| `seed` | Int64 | College Football Playoff seed under the straight-seeding rule (1 = best); null for teams outside the field. From 2026 the ACC / Big Ten / Big 12 / SEC champions, the highest-ranked Group-of-6 team (champion or not) and a Notre Dame ranked within the field size are selected before at-large teams; before 2026, the five highest-ranked conference champions. Automatic qualifiers are capped at the field size. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `rankings` has duplicate team rows. |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import cfb_standings, cfb_playoff_seeds
st = cfb_standings(games, teams)
seeded = cfb_playoff_seeds(st, rankings=ranks_df, playoff_seeds=12)
print(seeded.filter(pl.col("seed").is_not_null()))

# The 2024-2025 rule (5 highest-ranked champions)

seeded = cfb_playoff_seeds(st, rankings=ranks_df, season=2025)
```

### cfb_resume {#cfb_resume}

`cfb_resume(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Rating-based résumé metrics: SoS, quality wins, game control, wins-above-bubble.

For each team, joins every played opponent to its `cfb_ratings.cfb_ratings`
strength and rolls the games up into:

- `sos` -- mean opponent `adj_net` over played games (rating-based strength
  of schedule; complements the record-based SOV/SOS in `cfb_standings`).
- `quality_wins` -- count of wins over opponents with `adj_net` at or above
  the era `quality_win_threshold`.
- `game_control` -- mean postgame win expectancy `Phi(actual_margin /
  margin_sd)`, i.e. how *dominant* the results were, not just win/loss.
- `wab` -- wins above bubble: actual wins minus the expected wins of a
  bubble-quality team (`bubble_adj_net`) playing the same schedule, using the
  Phase-2 predictors with the HFA applied on the team's actual home/away side.
- reference-team schedule strength, record difficulty and
  `wins_above_bubble`: the win probability of a fixed reference team (the
  5th, 25th or 12th-best rated FBS team, or an average one) on the team's
  schedule, `Phi((net_points_scale * (r_ref - r_opp) + h * hfa_points) /
  margin_sd)`. `as_of_date` splits completed from remaining games for these
  columns only; the columns above use every completed game. The reference
  ratings come from the ratings passed in (pooled when several seasons are).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season or list of seasons. |
| `as_of_date` | `date \| None` | `None` | Leakage boundary forwarded to `cfb_ratings.cfb_ratings` (ratings use only games before this date). `None` uses the full season. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per team: `season`, `team_id` (Utf8), `sos`, `sos_rank` (Int64 dense rank, best = 1), `quality_wins` (Int64), `game_control` (Float64), `wab` (Float64), then the reference-team columns (Float64): for each reference `top5` / `top25` / `bubble` (the 5th / 25th / 12th-best rated FBS team) / `average` (rating 0), `sos_<ref>_past` / `_remaining` / `_season` (the reference team's mean win probability over completed, unplayed and all games, at the team's site, an FCS opponent a sure win) and `record_difficulty_<ref>` (the probability it wins at least as many of the completed games as the team did), and `wins_above_bubble` (wins minus the 12th-best team's expected wins). Zero-row (typed) when no games are available.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the résumé covers (null for a pooled multi-season fit). |
| `team_id` | String | Team ESPN id (character join key). |
| `sos` | Float64 | Rating-based strength of schedule - mean opponent adj_net over played games. |
| `sos_rank` | Int64 | Dense rank on sos descending (toughest schedule = 1). |
| `quality_wins` | Int64 | Count of wins over opponents with adj_net at or above the era quality-win threshold. |
| `game_control` | Float64 | Mean postgame win expectancy Phi(actual_margin / margin_sd) across played games - how dominant the results were, not just win/loss. |
| `wab` | Float64 | Wins above bubble - actual wins minus a bubble-quality team's expected wins over the same schedule. |
| `sos_top5_past` | Float64 | Mean probability that the 5th-best rated FBS team would win the team's completed games (before as_of_date when it is set) at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_top5_remaining` | Float64 | Mean probability that the 5th-best rated FBS team would win the team's games not yet played (null when none remain) at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_top5_season` | Float64 | Mean probability that the 5th-best rated FBS team would win the team's every scheduled game at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_top25_past` | Float64 | Mean probability that the 25th-best rated FBS team would win the team's completed games (before as_of_date when it is set) at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_top25_remaining` | Float64 | Mean probability that the 25th-best rated FBS team would win the team's games not yet played (null when none remain) at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_top25_season` | Float64 | Mean probability that the 25th-best rated FBS team would win the team's every scheduled game at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_bubble_past` | Float64 | Mean probability that the 12th-best rated FBS team (the last at-large place in a 12-team playoff) would win the team's completed games (before as_of_date when it is set) at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_bubble_remaining` | Float64 | Mean probability that the 12th-best rated FBS team (the last at-large place in a 12-team playoff) would win the team's games not yet played (null when none remain) at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_bubble_season` | Float64 | Mean probability that the 12th-best rated FBS team (the last at-large place in a 12-team playoff) would win the team's every scheduled game at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_average_past` | Float64 | Mean probability that an average team (rating 0) would win the team's completed games (before as_of_date when it is set) at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_average_remaining` | Float64 | Mean probability that an average team (rating 0) would win the team's games not yet played (null when none remain) at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `sos_average_season` | Float64 | Mean probability that an average team (rating 0) would win the team's every scheduled game at the team's site; an unrated (FCS) opponent counts as a sure win. |
| `record_difficulty_top5` | Float64 | Probability that the 5th-best rated FBS team would win at least as many of the team's completed games as the team did (exact Poisson-binomial; lower = harder record). |
| `record_difficulty_top25` | Float64 | Probability that the 25th-best rated FBS team would win at least as many of the team's completed games as the team did (exact Poisson-binomial; lower = harder record). |
| `record_difficulty_bubble` | Float64 | Probability that the 12th-best rated FBS team (the last at-large place in a 12-team playoff) would win at least as many of the team's completed games as the team did (exact Poisson-binomial; lower = harder record). |
| `record_difficulty_average` | Float64 | Probability that an average team (rating 0) would win at least as many of the team's completed games as the team did (exact Poisson-binomial; lower = harder record). |
| `wins_above_bubble` | Float64 | Wins minus the expected wins of the 12th-best rated FBS team over the team's completed games (an FCS opponent counts as a sure win). Differs from wab in its reference and home-field term. |

**Example**

```python
from sportsdataverse.cfb.cfb_resume import cfb_resume
resume = cfb_resume(2023)
resume.sort("sos_rank").head()
```

### cfb_returning_production {#cfb_returning_production}

`cfb_returning_production(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Returning production per team-season (offense / defense / overall).

For each requested season S, computes the fraction of season S-1 unit
production attributable to players on the season-S roster (Bill Connelly's
returning-production concept; unit weights from `get_constants`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Target season or list of seasons (production is drawn from S-1). |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per `(season, team_id)`: `off_returning`, `def_returning`, `overall_returning` (Float64 fractions in [0, 1]), `n_returning` (Int64 count of returning contributors), `def_basis`, `overall_basis` (Utf8, below), `is_estimated` (Boolean). `team_id` is the ESPN team id as Utf8 -- BREAKING vs the previous release, which emitted a normalized team NAME under `team` and joined at 57.7%. Zero-row (typed) when the box data is unavailable. `is_estimated` is True for **2004 only**, where season-2003 production is parsed from CFBD play text because ESPN's player box starts in 2004. Back-tested on 2005, where both sources exist, that route tracks the box route at r=0.73 (MAE 0.11) but reads about 0.085 LOW. It ranks teams well; it is not on the same level as its neighbours, so filter on this flag before comparing 2004 against another season. 2004 also carries a null `def_returning` -- 2003 play text has no defensive ids. `def_basis` names the defensive measure: `"participants"` when the production season is 2014+ (tackles, assists, tackles for loss, shared sacks, passes defended -- 92-100% of teams), `"pbp_splash"` for 2004-2013 (sacks, interceptions, pass breakups, forced fumbles; no tackle volume, so not on the same scale), `"box"` when that source's release is missing, null with `def_returning`. `overall_basis` is `"offense+defense"`, or `"offense"` for a team with no defensive value, whose overall then equals `off_returning`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the returning fractions describe (production drawn from the prior season). |
| `team_id` | String | ESPN team id as a string (integer-origin). |
| `off_returning` | Float64 | Fraction of prior-season attributed offensive yardage (passing + rushing + receiving) returning on the current roster. |
| `def_returning` | Float64 | Fraction of prior-season weighted defensive production returning on the current roster; the measure is named in def_basis. |
| `overall_returning` | Float64 | Unit fractions combined with the fitted returning_prod_weights (FBS offense 0.49 / defense 0.51, the 2018-2025 fit in fit_returning_weights.py). |
| `n_returning` | Int64 | Count of prior-season contributors present on the current roster. |
| `def_basis` | String | Defensive measure behind def_returning: participants (production season 2014+: tackles, assists, tackles for loss, shared sacks, passes defended), pbp_splash (2004-2013: sacks, interceptions, pass breakups, forced fumbles; no tackle volume, so not on the same scale), box (the source release was missing), or null with def_returning. |
| `overall_basis` | String | Units in overall_returning: offense+defense, or offense for a team with no defensive value, whose overall then equals off_returning. |
| `is_estimated` | Boolean | True for 2004 only, where season-2003 production is parsed from play text; it reads about 0.085 low against the box route. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If the season-S CFBD roster cannot be resolved to team ids above the crosswalk match floor (the ESPN roster carries `team_id` and needs no resolution; the two are unioned, see `_roster_keys`). |

**Example**

```python
from sportsdataverse.cfb import cfb_returning_production
rp = cfb_returning_production(2023)
rp.sort("overall_returning", descending=True).head(10)
```

### cfb_season_odds {#cfb_season_odds}

`cfb_season_odds(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, n_sims: 'int' = 10000, playoff_seeds: 'int' = 12, seed: 'int' = 0, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Ratings-driven season Monte Carlo: conference / playoff / championship odds.

Thin wrapper over `cfb_simulations.cfb_simulations` -- it builds the ratings
with `cfb_ratings.cfb_ratings`, converts the schedule to the engine format
with `cfb_standings.cfb_games_from_schedule` (re-keyed on ESPN `team_id` so
the ratings align), and feeds `make_ratings_compute_results` as the sampler.
All season / standings / bracket machinery is reused; unplayed games are simulated,
played games (before `as_of_date`) are kept. Only FBS programs (schedule
`division == "fbs"`) enter the simulated universe; non-FBS opponents stay in the
game set -- their games still count toward FBS records -- but can never reach the
standings, the playoff field, or the output.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (an `int`, or a one-element list). Multiple seasons raise `ValueError` -- the simulation engine is single-season. |
| `as_of_date` | `date \| None` | `None` | Leakage boundary applied to BOTH the ratings vintage and the game set. Ratings are fit only on plays from games with `date < as_of_date` (`cfb_ratings.cfb_ratings`), and schedule results from `start_date` on/after `as_of_date` are masked so those games are simulated instead of replayed; masked postseason rows are dropped (the matchup is itself an outcome) and regenerated from each sim's own standings. `None` uses the full season as-is. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `playoff_seeds` | `int` | `12` | CFP field size. |
| `seed` | `int` | `0` | RNG seed for reproducibility. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per team: `season`, `team_id` (Utf8), `exp_wins`, `conf_title_prob`, `playoff_prob`, `first_round_bye_prob`, `cfp_champ_prob` (Float64 probabilities in [0, 1]). Zero-row (typed) when no ratings/schedule are available.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season simulated (null for a pooled multi-season call). |
| `team_id` | String | Team ESPN id (character join key). |
| `exp_wins` | Float64 | Mean wins per simulated season. |
| `conf_title_prob` | Float64 | Share of simulations in which the team won its conference. |
| `playoff_prob` | Float64 | Share of simulations in which the team made the College Football Playoff field. |
| `first_round_bye_prob` | Float64 | Share of simulations in which the team earned a CFP first-round bye. |
| `cfp_champ_prob` | Float64 | Share of simulations in which the team won the College Football Playoff national championship. |

**Example**

```python
from sportsdataverse.cfb.cfb_season_odds import cfb_season_odds
odds = cfb_season_odds(2023, n_sims=2000)
odds.sort("cfp_champ_prob", descending=True).head()
```

### cfb_transfer_impact {#cfb_transfer_impact}

`cfb_transfer_impact(target_season: 'int | list[int]', *, division: 'str' = 'fbs', alpha: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Net transfer talent and its projected win-total impact per team-season.

`pred_win_delta` comes from an on-demand ridge of realized win deltas on
`net_transfer_talent` fitted over strictly-prior seasons (the as-of
boundary is enforced internally per target season).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int \| list[int]` |  | Season (or list) to score. |
| `division` | `str` | `'fbs'` | Division slug for the star-points constants. |
| `alpha` | `float` | `1.0` | Ridge L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per (season, team_id): `net_transfer_talent` (Float64), `pred_win_delta` (Float64). Zero-row (typed) when no data.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season the net transfer talent describes. |
| `team_id` | String | ESPN team id as a string (rosters team_id, cast Int64 to Utf8). |
| `net_transfer_talent` | Float64 | Incoming minus outgoing transfer talent points for the season. |
| `pred_win_delta` | Float64 | Ridge-projected win-total change from net transfer talent (as-of fit; weak observed validity - see the strict-xfail gate). |

**Example**

```python
from sportsdataverse.cfb import cfb_transfer_impact
imp = cfb_transfer_impact(2024)
imp.sort("net_transfer_talent", descending=True).head(10)
```

### cfb_transfer_moves {#cfb_transfer_moves}

`cfb_transfer_moves(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Transfer moves inferred from year-over-year roster diffs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Destination season(s) to extract moves for (each compares S-1 -> S). |
| `division` | `str` | `'fbs'` | Division slug for the star-points constants. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per move side: `season` (Int64, the destination season), `team_id` (Utf8 ESPN team id), `player_id` (Utf8 ESPN athlete id), `direction` ("in" | "out"), `prior_team_id` (Utf8 ESPN team id of the season S-1 team), `talent_points` (Float64, name-joined to the `cfb_recruits` release; the 0-star default when the player has no recruit rating). Zero-row (typed) when rosters are unavailable.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Destination season of the move (compares rosters S-1 to S). |
| `team_id` | String | ESPN team id (as a string) of the side this row describes (destination for "in", origin for "out"). |
| `player_id` | String | ESPN athlete id as a string. |
| `direction` | String | Move side - "in" (arriving at team_id) or "out" (leaving team_id). |
| `prior_team_id` | String | ESPN team id (as a string) of the season S-1 team. |
| `talent_points` | Float64 | Recruit-star talent points (name-matched to the 247 recruit record; 0-star default when unrated). |

**Example**

```python
from sportsdataverse.cfb import cfb_transfer_moves
moves = cfb_transfer_moves(2024)
moves.filter(pl.col("direction") == "in").group_by("team_id").len()
```

### create_drive_summary {#create_drive_summary}

`create_drive_summary(drives: 'list[dict] | dict', frame: 'pl.DataFrame', home_id: 'str | int', away_id: 'str | int', periods: 'set[int] | str | None' = None) -> 'dict | None'`

Build the StatBroadcast-style drive summary, chart, and long-play lists.

A drive belongs to the quarter it STARTED in. On a windowed build the
full drive sequence still provides context (running score, the previous
drive for OBTAINED and points-off-turnovers), but only in-window drives
are counted, charted, or listed. `largest_lead` and the time-leading /
time-tied split window too: the score state is read from the whole
regulation play sequence and then clipped to the window's clock intervals
(one per contiguous run of quarters, so a gapped set never charges the
quarter it skipped). Under `"ot"` the clock has no axis to integrate over
and only `largest_lead` ships.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drives` | `list[dict]` |  | the ESPN drives grouping, in game order (`previous` plus the in-progress `current` drive, if any). |
| `frame` | `pl.DataFrame` |  | the enriched plays frame from `CFBPlayProcess.run_processing_pipeline` (`plays_frame`). |
| `home_id` | `str \| int` |  | ESPN home team id. |
| `away_id` | `str \| int` |  | ESPN away team id. |
| `periods` | `set[int] \| str \| None` | `None` | optional window -- a set of quarter numbers (e.g. `{1, 2}`) or the string `"ot"` (every period > 4). `None` = full game. |

**Returns**

`{"teams": {...}, "chart": [...], "scores": [...], "longPlays": {...}}` keyed by team id, or `None` when the inputs are unusable (no drives, empty frame, or an empty window).

**Example**

```python
summary = create_drive_summary(drives, game.plays_frame, "52", "61")
```

### create_situational_stats {#create_situational_stats}

`create_situational_stats(frame: 'pl.DataFrame', home_id: 'str | int', away_id: 'str | int', window_expr: 'pl.Expr | None' = None) -> 'dict | None'`

Build the situational team-stats block from a plays frame.

`two_minute` and `middle_8` are omitted from a windowed build: both name
a clock window of their own, so intersecting them with another window
describes neither (middle-8 inside Q1 is empty). Every other section,
`pace` and `non_garbage` included, is computed on the windowed slice and
ships with it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `frame` | `pl.DataFrame` |  | the enriched plays frame from `CFBPlayProcess.run_processing_pipeline` (`plays_frame`). |
| `home_id` | `str \| int` |  | ESPN home team id. |
| `away_id` | `str \| int` |  | ESPN away team id. |
| `window_expr` | `Expr \| None` | `None` | optional polars filter expression windowing the windowable sections to that slice (e.g. `pl.col("period") == 3`). `None` = full game. |

**Returns**

`{"teams": {<team_id>: {<section>: ...}}}` or `None` when the frame is unusable or the window is empty.

**Example**

```python
stats = create_situational_stats(game.plays_frame, "52", "61")
```

### make_ratings_compute_results {#make_ratings_compute_results}

`make_ratings_compute_results(ratings: 'pl.DataFrame', *, era: 'str' = 'modern') -> 'ComputeResultsFn'`

Build a `cfb_simulations` `compute_results` closure from fixed ratings.

The returned closure implements the engine's results contract -- `(teams, games,
week_num, *, rng, **kwargs) -> {"teams", "games"}` -- filling every unplayed
`week == week_num` game's `result` with a sampled home margin
`round(Normal(exp_margin, margin_sd))`, where `exp_margin` is
`cfb_game_predict.predict_margin` on the two teams' `adj_net` (home-field
applied unless `neutral`). Unlike the default elo sampler the ratings are
**fixed**, so `teams` passes through unchanged (no elo update). Postseason games
(`game_type != "REG"`) re-break a sampled tie by win probability.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | A `cfb_ratings.cfb_ratings`-style frame with `team_id` and `adj_net`. Teams absent from it are treated as league-average (0.0). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |

**Returns**

A `compute_results` callable suitable for `cfb_simulations(..., compute_results=...)`.

**Example**

```python
import numpy as np, polars as pl
from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results
cr = make_ratings_compute_results(pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.3, -0.3]}))
teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
games = pl.DataFrame({"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"],
                      "neutral": [0], "result": [None]})
cr(teams, games, 1, rng=np.random.default_rng(0))["games"]
```

### play_type_family_expr {#play_type_family_expr}

`play_type_family_expr(source: 'str' = 'play_type_canonical') -> 'pl.Expr'`

Build the polars expression mapping a canonical type to its phase family.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source` | `str` | `'play_type_canonical'` | Name of the canonical play-type column. |

**Returns**

A `pl.Expr` aliased `play_type_family`; unmapped values yield null.

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | At collection time if `source` is absent from the frame. |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import add_play_type_canonical

pbp = pl.DataFrame({"type.text": ["Rush", "Timeout"]})
add_play_type_canonical(pbp).filter(
    pl.col("play_type_family") != "administrative"
)
```
