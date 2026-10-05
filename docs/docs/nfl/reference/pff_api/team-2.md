---
title: "NFL — PFF Developer API (api.pff.com, API key) — Team (2)"
sidebar_label: "Team (2)"
sidebar_position: 15
description: "NFL — PFF Developer API (api.pff.com, API key) — Team (2) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Team (2)

## pff_api_team_leaders

A team's leaders for one position group, with rank and percentile

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/leaders`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/leaders?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/leaders?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |
| `group` | `group` |  |  | `Y` | Which position group the leaders come from — receiving (the default), passing, rushing or defense. |

### Returns {#pff_api_team_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
**receiving**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `name` | character | Player's display name as PFF lists it. |
| `position` | character | PFF position code (e.g. QB, HB, WR, TE, ED, DI, LB, CB, S); qualification floors and ranking pools are per position. |
| `jersey` | character | Jersey number as a string; leading zeros are meaningful (e.g. "01"). |
| `games` | integer | Games played in the requested season part (PFF column '#G'). |
| `games_rank` | integer | Player's rank on games played among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `games_rank_of` | integer | Number of qualified players at his position league-wide ranked on games played, the pool games_rank is out of; null when the player is below his position's qualifying volume. |
| `games_percentile` | integer | Player's percentile (0-100, higher = better) on games played among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `targets` | integer | Passes thrown to the player (targets) over the requested season part. |
| `targets_rank` | integer | Player's rank on targets among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `targets_rank_of` | integer | Number of qualified players at his position league-wide ranked on targets, the pool targets_rank is out of; null when the player is below his position's qualifying volume. |
| `targets_percentile` | integer | Player's percentile (0-100, higher = better) on targets among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `receptions` | integer | Passes caught by the player over the requested season part. |
| `receptions_rank` | integer | Player's rank on receptions among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `receptions_rank_of` | integer | Number of qualified players at his position league-wide ranked on receptions, the pool receptions_rank is out of; null when the player is below his position's qualifying volume. |
| `receptions_percentile` | integer | Player's percentile (0-100, higher = better) on receptions among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `reception_pct` | numeric | Receptions per target (catch rate), as a fraction (0-1). |
| `reception_pct_rank` | integer | Player's rank on catch rate among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `reception_pct_rank_of` | integer | Number of qualified players at his position league-wide ranked on catch rate, the pool reception_pct_rank is out of; null when the player is below his position's qualifying volume. |
| `reception_pct_percentile` | integer | Player's percentile (0-100, higher = better) on catch rate among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `receiving_yards` | integer | Receiving yards gained by the player. |
| `receiving_yards_rank` | integer | Player's rank on receiving yards among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `receiving_yards_rank_of` | integer | Number of qualified players at his position league-wide ranked on receiving yards, the pool receiving_yards_rank is out of; null when the player is below his position's qualifying volume. |
| `receiving_yards_percentile` | integer | Player's percentile (0-100, higher = better) on receiving yards among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `yards_per_reception` | numeric | Receiving yards per reception (receiving_yards / receptions). |
| `yards_per_reception_rank` | integer | Player's rank on yards per reception among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `yards_per_reception_rank_of` | integer | Number of qualified players at his position league-wide ranked on yards per reception, the pool yards_per_reception_rank is out of; null when the player is below his position's qualifying volume. |
| `yards_per_reception_percentile` | integer | Player's percentile (0-100, higher = better) on yards per reception among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `receiving_td` | integer | Receiving touchdowns scored by the player. |
| `receiving_td_rank` | integer | Player's rank on receiving touchdowns among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `receiving_td_rank_of` | integer | Number of qualified players at his position league-wide ranked on receiving touchdowns, the pool receiving_td_rank is out of; null when the player is below his position's qualifying volume. |
| `receiving_td_percentile` | integer | Player's percentile (0-100, higher = better) on receiving touchdowns among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_offense` | numeric | PFF overall offense grade (0-100). |
| `grade_offense_rank` | integer | Player's rank on PFF offense grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_offense_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF offense grade, the pool grade_offense_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_offense_percentile` | integer | Player's percentile (0-100, higher = better) on PFF offense grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_receiving` | numeric | PFF receiving grade (0-100). |
| `grade_receiving_rank` | integer | Player's rank on PFF receiving grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_receiving_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF receiving grade, the pool grade_receiving_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_receiving_percentile` | integer | Player's percentile (0-100, higher = better) on PFF receiving grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_drop` | numeric | PFF hands/drop grade (0-100). |
| `grade_drop_rank` | integer | Player's rank on PFF hands/drop grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_drop_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF hands/drop grade, the pool grade_drop_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_drop_percentile` | integer | Player's percentile (0-100, higher = better) on PFF hands/drop grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_fumble` | numeric | PFF ball-security (hands/fumble) grade (0-100). |
| `grade_fumble_rank` | integer | Player's rank on PFF ball-security grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_fumble_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF ball-security grade, the pool grade_fumble_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_fumble_percentile` | integer | Player's percentile (0-100, higher = better) on PFF ball-security grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_pass_block` | numeric | PFF pass-blocking grade (0-100); null when the player has no graded pass-blocking snaps. |
| `grade_pass_block_rank` | integer | Player's rank on PFF pass-blocking grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_pass_block_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF pass-blocking grade, the pool grade_pass_block_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_pass_block_percentile` | integer | Player's percentile (0-100, higher = better) on PFF pass-blocking grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |

**passing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `name` | character | Player's display name as PFF lists it. |
| `position` | character | PFF position code (e.g. QB, HB, WR, TE, ED, DI, LB, CB, S); qualification floors and ranking pools are per position. |
| `jersey` | character | Jersey number as a string; leading zeros are meaningful (e.g. "01"). |
| `games` | integer | Games played in the requested season part (PFF column '#G'). |
| `games_rank` | integer | Player's rank on games played among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `games_rank_of` | integer | Number of qualified players at his position league-wide ranked on games played, the pool games_rank is out of; null when the player is below his position's qualifying volume. |
| `games_percentile` | integer | Player's percentile (0-100, higher = better) on games played among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `pass_attempts` | integer | Pass attempts thrown by the player. |
| `pass_attempts_rank` | integer | Player's rank on pass attempts among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `pass_attempts_rank_of` | integer | Number of qualified players at his position league-wide ranked on pass attempts, the pool pass_attempts_rank is out of; null when the player is below his position's qualifying volume. |
| `pass_attempts_percentile` | integer | Player's percentile (0-100, higher = better) on pass attempts among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `completions` | integer | Passes the player completed. |
| `completions_rank` | integer | Player's rank on completions among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `completions_rank_of` | integer | Number of qualified players at his position league-wide ranked on completions, the pool completions_rank is out of; null when the player is below his position's qualifying volume. |
| `completions_percentile` | integer | Player's percentile (0-100, higher = better) on completions among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `completion_pct` | numeric | Completion percentage as a fraction (0-1). |
| `completion_pct_rank` | integer | Player's rank on completion percentage among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `completion_pct_rank_of` | integer | Number of qualified players at his position league-wide ranked on completion percentage, the pool completion_pct_rank is out of; null when the player is below his position's qualifying volume. |
| `completion_pct_percentile` | integer | Player's percentile (0-100, higher = better) on completion percentage among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `passing_yards` | integer | Passing yards gained by the player. |
| `passing_yards_rank` | integer | Player's rank on passing yards among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `passing_yards_rank_of` | integer | Number of qualified players at his position league-wide ranked on passing yards, the pool passing_yards_rank is out of; null when the player is below his position's qualifying volume. |
| `passing_yards_percentile` | integer | Player's percentile (0-100, higher = better) on passing yards among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `yards_per_attempt` | numeric | Passing yards per pass attempt (passing_yards / pass_attempts). |
| `yards_per_attempt_rank` | integer | Player's rank on yards per pass attempt among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `yards_per_attempt_rank_of` | integer | Number of qualified players at his position league-wide ranked on yards per pass attempt, the pool yards_per_attempt_rank is out of; null when the player is below his position's qualifying volume. |
| `yards_per_attempt_percentile` | integer | Player's percentile (0-100, higher = better) on yards per pass attempt among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `passing_td` | integer | Passing touchdowns thrown by the player. |
| `passing_td_rank` | integer | Player's rank on passing touchdowns among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `passing_td_rank_of` | integer | Number of qualified players at his position league-wide ranked on passing touchdowns, the pool passing_td_rank is out of; null when the player is below his position's qualifying volume. |
| `passing_td_percentile` | integer | Player's percentile (0-100, higher = better) on passing touchdowns among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `interceptions` | integer | Interceptions: thrown by the passer in the passing group, made by the defender in the defense group. |
| `interceptions_rank` | integer | Player's rank on interceptions among qualified players at his position league-wide (1 = best: fewest thrown in the passing group, most made in the defense group; tied players share a rank); null when the player is below his position's qualifying volume. |
| `interceptions_rank_of` | integer | Number of qualified players at his position league-wide ranked on interceptions, the pool interceptions_rank is out of; null when the player is below his position's qualifying volume. |
| `interceptions_percentile` | integer | Player's percentile (0-100, higher = better) on interceptions among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_offense` | numeric | PFF overall offense grade (0-100). |
| `grade_offense_rank` | integer | Player's rank on PFF offense grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_offense_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF offense grade, the pool grade_offense_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_offense_percentile` | integer | Player's percentile (0-100, higher = better) on PFF offense grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_pass` | numeric | PFF passing grade (0-100). |
| `grade_pass_rank` | integer | Player's rank on PFF passing grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_pass_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF passing grade, the pool grade_pass_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_pass_percentile` | integer | Player's percentile (0-100, higher = better) on PFF passing grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_run` | numeric | PFF rushing grade (0-100). |
| `grade_run_rank` | integer | Player's rank on PFF rushing grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_run_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF rushing grade, the pool grade_run_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_run_percentile` | integer | Player's percentile (0-100, higher = better) on PFF rushing grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_fumble` | numeric | PFF ball-security (hands/fumble) grade (0-100). |
| `grade_fumble_rank` | integer | Player's rank on PFF ball-security grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_fumble_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF ball-security grade, the pool grade_fumble_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_fumble_percentile` | integer | Player's percentile (0-100, higher = better) on PFF ball-security grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |

**rushing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `name` | character | Player's display name as PFF lists it. |
| `position` | character | PFF position code (e.g. QB, HB, WR, TE, ED, DI, LB, CB, S); qualification floors and ranking pools are per position. |
| `jersey` | character | Jersey number as a string; leading zeros are meaningful (e.g. "01"). |
| `games` | integer | Games played in the requested season part (PFF column '#G'). |
| `games_rank` | integer | Player's rank on games played among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `games_rank_of` | integer | Number of qualified players at his position league-wide ranked on games played, the pool games_rank is out of; null when the player is below his position's qualifying volume. |
| `games_percentile` | integer | Player's percentile (0-100, higher = better) on games played among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `rush_attempts` | integer | Rushing attempts (carries) by the player. |
| `rush_attempts_rank` | integer | Player's rank on rushing attempts among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `rush_attempts_rank_of` | integer | Number of qualified players at his position league-wide ranked on rushing attempts, the pool rush_attempts_rank is out of; null when the player is below his position's qualifying volume. |
| `rush_attempts_percentile` | integer | Player's percentile (0-100, higher = better) on rushing attempts among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `rushing_yards` | integer | Rushing yards gained by the player. |
| `rushing_yards_rank` | integer | Player's rank on rushing yards among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `rushing_yards_rank_of` | integer | Number of qualified players at his position league-wide ranked on rushing yards, the pool rushing_yards_rank is out of; null when the player is below his position's qualifying volume. |
| `rushing_yards_percentile` | integer | Player's percentile (0-100, higher = better) on rushing yards among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `yards_per_carry` | numeric | Rushing yards per carry (rushing_yards / rush_attempts). |
| `yards_per_carry_rank` | integer | Player's rank on yards per carry among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `yards_per_carry_rank_of` | integer | Number of qualified players at his position league-wide ranked on yards per carry, the pool yards_per_carry_rank is out of; null when the player is below his position's qualifying volume. |
| `yards_per_carry_percentile` | integer | Player's percentile (0-100, higher = better) on yards per carry among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `yards_after_contact_per_attempt` | numeric | Rushing yards after contact per attempt (PFF column 'YAC/ATT'). |
| `yards_after_contact_per_attempt_rank` | integer | Player's rank on yards after contact per attempt among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `yards_after_contact_per_attempt_rank_of` | integer | Number of qualified players at his position league-wide ranked on yards after contact per attempt, the pool yards_after_contact_per_attempt_rank is out of; null when the player is below his position's qualifying volume. |
| `yards_after_contact_per_attempt_percentile` | integer | Player's percentile (0-100, higher = better) on yards after contact per attempt among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `rushing_td` | integer | Rushing touchdowns scored by the player. |
| `rushing_td_rank` | integer | Player's rank on rushing touchdowns among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `rushing_td_rank_of` | integer | Number of qualified players at his position league-wide ranked on rushing touchdowns, the pool rushing_td_rank is out of; null when the player is below his position's qualifying volume. |
| `rushing_td_percentile` | integer | Player's percentile (0-100, higher = better) on rushing touchdowns among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `fumbles` | integer | Fumbles by the player (rushing group). |
| `fumbles_rank` | integer | Player's rank on fumbles among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `fumbles_rank_of` | integer | Number of qualified players at his position league-wide ranked on fumbles, the pool fumbles_rank is out of; null when the player is below his position's qualifying volume. |
| `fumbles_percentile` | integer | Player's percentile (0-100, higher = better) on fumbles among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_offense` | numeric | PFF overall offense grade (0-100). |
| `grade_offense_rank` | integer | Player's rank on PFF offense grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_offense_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF offense grade, the pool grade_offense_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_offense_percentile` | integer | Player's percentile (0-100, higher = better) on PFF offense grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_run` | numeric | PFF rushing grade (0-100). |
| `grade_run_rank` | integer | Player's rank on PFF rushing grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_run_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF rushing grade, the pool grade_run_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_run_percentile` | integer | Player's percentile (0-100, higher = better) on PFF rushing grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_fumble` | numeric | PFF ball-security (hands/fumble) grade (0-100). |
| `grade_fumble_rank` | integer | Player's rank on PFF ball-security grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_fumble_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF ball-security grade, the pool grade_fumble_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_fumble_percentile` | integer | Player's percentile (0-100, higher = better) on PFF ball-security grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_receiving` | numeric | PFF receiving grade (0-100). |
| `grade_receiving_rank` | integer | Player's rank on PFF receiving grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_receiving_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF receiving grade, the pool grade_receiving_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_receiving_percentile` | integer | Player's percentile (0-100, higher = better) on PFF receiving grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_pass_block` | numeric | PFF pass-blocking grade (0-100); null when the player has no graded pass-blocking snaps. |
| `grade_pass_block_rank` | integer | Player's rank on PFF pass-blocking grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_pass_block_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF pass-blocking grade, the pool grade_pass_block_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_pass_block_percentile` | integer | Player's percentile (0-100, higher = better) on PFF pass-blocking grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |

**defense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id (integer; matches the /players id and every player_id join key). |
| `name` | character | Player's display name as PFF lists it. |
| `position` | character | PFF position code (e.g. QB, HB, WR, TE, ED, DI, LB, CB, S); qualification floors and ranking pools are per position. |
| `jersey` | character | Jersey number as a string; leading zeros are meaningful (e.g. "01"). |
| `games` | integer | Games played in the requested season part (PFF column '#G'). |
| `games_rank` | integer | Player's rank on games played among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `games_rank_of` | integer | Number of qualified players at his position league-wide ranked on games played, the pool games_rank is out of; null when the player is below his position's qualifying volume. |
| `games_percentile` | integer | Player's percentile (0-100, higher = better) on games played among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `tackles` | integer | Tackles made by the player, as charted by PFF (assisted tackles are reported in assists). |
| `tackles_rank` | integer | Player's rank on tackles among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `tackles_rank_of` | integer | Number of qualified players at his position league-wide ranked on tackles, the pool tackles_rank is out of; null when the player is below his position's qualifying volume. |
| `tackles_percentile` | integer | Player's percentile (0-100, higher = better) on tackles among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `assists` | integer | Assisted tackles credited to the player. |
| `assists_rank` | integer | Player's rank on assisted tackles among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `assists_rank_of` | integer | Number of qualified players at his position league-wide ranked on assisted tackles, the pool assists_rank is out of; null when the player is below his position's qualifying volume. |
| `assists_percentile` | integer | Player's percentile (0-100, higher = better) on assisted tackles among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `sacks` | numeric | Sacks recorded by the defender. |
| `sacks_rank` | integer | Player's rank on sacks among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `sacks_rank_of` | integer | Number of qualified players at his position league-wide ranked on sacks, the pool sacks_rank is out of; null when the player is below his position's qualifying volume. |
| `sacks_percentile` | integer | Player's percentile (0-100, higher = better) on sacks among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `qb_hurries` | integer | Quarterback hurries recorded by the defender. |
| `qb_hurries_rank` | integer | Player's rank on quarterback hurries among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `qb_hurries_rank_of` | integer | Number of qualified players at his position league-wide ranked on quarterback hurries, the pool qb_hurries_rank is out of; null when the player is below his position's qualifying volume. |
| `qb_hurries_percentile` | integer | Player's percentile (0-100, higher = better) on quarterback hurries among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `interceptions` | integer | Interceptions: thrown by the passer in the passing group, made by the defender in the defense group. |
| `interceptions_rank` | integer | Player's rank on interceptions among qualified players at his position league-wide (1 = best: fewest thrown in the passing group, most made in the defense group; tied players share a rank); null when the player is below his position's qualifying volume. |
| `interceptions_rank_of` | integer | Number of qualified players at his position league-wide ranked on interceptions, the pool interceptions_rank is out of; null when the player is below his position's qualifying volume. |
| `interceptions_percentile` | integer | Player's percentile (0-100, higher = better) on interceptions among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `passes_defended` | integer | Passes defended credited to the defender (PFF column 'PD'). |
| `passes_defended_rank` | integer | Player's rank on passes defended among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `passes_defended_rank_of` | integer | Number of qualified players at his position league-wide ranked on passes defended, the pool passes_defended_rank is out of; null when the player is below his position's qualifying volume. |
| `passes_defended_percentile` | integer | Player's percentile (0-100, higher = better) on passes defended among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_defense` | numeric | PFF overall defense grade (0-100). |
| `grade_defense_rank` | integer | Player's rank on PFF defense grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_defense_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF defense grade, the pool grade_defense_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_defense_percentile` | integer | Player's percentile (0-100, higher = better) on PFF defense grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_run_defense` | numeric | PFF run-defense grade (0-100). |
| `grade_run_defense_rank` | integer | Player's rank on PFF run-defense grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_run_defense_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF run-defense grade, the pool grade_run_defense_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_run_defense_percentile` | integer | Player's percentile (0-100, higher = better) on PFF run-defense grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_tackle` | numeric | PFF tackling grade (0-100). |
| `grade_tackle_rank` | integer | Player's rank on PFF tackling grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_tackle_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF tackling grade, the pool grade_tackle_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_tackle_percentile` | integer | Player's percentile (0-100, higher = better) on PFF tackling grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_pass_rush` | numeric | PFF pass-rush grade (0-100). |
| `grade_pass_rush_rank` | integer | Player's rank on PFF pass-rush grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_pass_rush_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF pass-rush grade, the pool grade_pass_rush_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_pass_rush_percentile` | integer | Player's percentile (0-100, higher = better) on PFF pass-rush grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |
| `grade_coverage` | numeric | PFF coverage grade (0-100). |
| `grade_coverage_rank` | integer | Player's rank on PFF coverage grade among qualified players at his position league-wide (1 = best; tied players share a rank); null when the player is below his position's qualifying volume. |
| `grade_coverage_rank_of` | integer | Number of qualified players at his position league-wide ranked on PFF coverage grade, the pool grade_coverage_rank is out of; null when the player is below his position's qualifying volume. |
| `grade_coverage_percentile` | integer | Player's percentile (0-100, higher = better) on PFF coverage grade among qualified players at his position league-wide; null when the player is below his position's qualifying volume. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_leaders-example}

```python
pff_api_team_leaders(league='nfl', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._

## pff_api_team_rushing_direction

A team's rushing by direction, one row per rusher and gap, plus totals

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/reports/rushing-direction`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/reports/rushing-direction?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/reports/rushing-direction?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |

### Returns {#pff_api_team_rushing_direction-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | PFF player id of the rusher (integer; matches the /players id and every player_id join key). |
| `player` | character | Rusher's display name as PFF lists it. |
| `position` | character | PFF position code of the rusher (e.g. HB, QB, WR). |
| `direction` | character | Run direction as PFF charts it, i.e. the gap or edge the carry went to (e.g. LE = left end, LT = left tackle, LG = left guard); one row per rusher per direction. |
| `attempts` | integer | Carries in this direction (PFF column 'ATT'). |
| `yards` | integer | Rushing yards on carries in this direction. |
| `ypa` | numeric | Rushing yards per carry in this direction (PFF column 'YPA'). |
| `touchdowns` | integer | Rushing touchdowns on carries in this direction. |
| `first_downs` | integer | Rushing first downs gained on carries in this direction. |
| `explosive` | integer | Carries in this direction that gained 10 or more yards (PFF column '10+'). |
| `yards_after_contact` | integer | Rushing yards gained after first contact on carries in this direction (PFF column 'YCO'). |
| `yco_attempt` | numeric | Yards after contact per carry in this direction (PFF column 'YCO/A'). |
| `longest` | integer | Longest carry in this direction, in yards. |
| `avoided_tackles` | integer | Missed tackles forced on carries in this direction (PFF column 'MTF'). |
| `fumbles` | integer | Fumbles on carries in this direction. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_team_rushing_direction-example}

```python
pff_api_team_rushing_direction(league='nfl', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._
