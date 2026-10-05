---
title: "NFL — PFF Developer API (api.pff.com, API key) — Player: passing–rushing"
sidebar_label: "Player: passing–rushing"
sidebar_position: 10
description: "NFL — PFF Developer API (api.pff.com, API key) — Player: passing–rushing — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Player: passing–rushing

## pff_api_player_passing_pressure

Passing under pressure for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/passing/pressure`

**Valid URL:** [https://api.pff.com/v1/player/passing/pressure?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/passing/pressure?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_passing_pressure-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `no_blitz_completion_percent` | numeric | Percentage of pass attempts completed when not blitzed. |
| `grades_offense` | numeric | PFF overall offense grade for the player (0-100). |
| `no_pressure_scrambles` | numeric | Number of scrambles from a clean pocket (no pressure). |
| `blitz_touchdowns` | numeric | Number of passing touchdowns thrown when blitzed. |
| `pressure_yards` | numeric | Passing yards gained when under pressure. |
| `no_pressure_spikes` | numeric | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | numeric | Yards gained per pass attempt when blitzed. |
| `no_blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when not blitzed. |
| `blitz_qb_rating` | numeric | Traditional NFL passer rating when blitzed. |
| `no_pressure_thrown_aways` | numeric | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `no_blitz_drops` | numeric | Number of catchable passes dropped by receivers when not blitzed. |
| `no_pressure_completion_percent` | numeric | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when blitzed, as charted by PFF. |
| `pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) when under pressure. |
| `no_blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `pressure_completions` | numeric | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | numeric | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | numeric | Number of clock-stopping spikes when blitzed. |
| `no_pressure_completions` | numeric | Number of completed passes from a clean pocket (no pressure). |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `no_pressure_passing_snaps` | numeric | Number of passing snaps played from a clean pocket (no pressure). |
| `no_blitz_first_downs` | numeric | Number of passing first downs gained when not blitzed. |
| `blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when blitzed. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when under pressure, as charted by PFF. |
| `blitz_sacks` | numeric | Number of sacks taken when blitzed. |
| `no_pressure_interceptions` | numeric | Number of passes intercepted from a clean pocket (no pressure). |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `blitz_completions` | numeric | Number of completed passes when blitzed. |
| `blitz_attempts` | numeric | Number of pass attempts when blitzed. |
| `pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | numeric | Number of sacks taken when under pressure. |
| `no_blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | numeric | Yards gained per pass attempt from a clean pocket (no pressure). |
| `pressure_passing_snaps` | numeric | Number of passing snaps played when under pressure. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `blitz_thrown_aways` | numeric | Number of intentional throwaways when blitzed. |
| `no_pressure_drops` | numeric | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `pressure_scrambles` | numeric | Number of scrambles when under pressure. |
| `blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when blitzed. |
| `pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `blitz_completion_percent` | numeric | Percentage of pass attempts completed when blitzed. |
| `no_pressure_first_downs` | numeric | Number of passing first downs gained from a clean pocket (no pressure). |
| `blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `no_blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `blitz_interceptions` | numeric | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | numeric | Number of dropbacks when not blitzed. |
| `no_blitz_grades_pass` | numeric | PFF passing grade (0-100) when not blitzed. |
| `no_blitz_scrambles` | numeric | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when under pressure. |
| `no_blitz_yards` | numeric | Passing yards gained when not blitzed. |
| `base_dropbacks` | numeric | Number of dropbacks across all splits, the baseline total for this facet. |
| `pressure_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `no_blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `no_pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds from a clean pocket (no pressure). |
| `pressure_dropbacks` | numeric | Number of dropbacks when under pressure. |
| `no_blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `no_blitz_touchdowns` | numeric | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when not blitzed. |
| `no_pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | numeric | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | numeric | Number of clock-stopping spikes when not blitzed. |
| `no_pressure_dropbacks` | numeric | Number of dropbacks from a clean pocket (no pressure). |
| `blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when under pressure. |
| `blitz_first_downs` | numeric | Number of passing first downs gained when blitzed. |
| `no_blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `no_pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `no_blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when not blitzed. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `no_pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | numeric | PFF rushing grade for the player (0-100). |
| `no_pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when under pressure. |
| `pressure_completion_percent` | numeric | Percentage of pass attempts completed when under pressure. |
| `pressure_avg_depth_of_target` | numeric | Average depth of target in air yards when under pressure. |
| `blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `pressure_drops` | numeric | Number of catchable passes dropped by receivers when under pressure. |
| `no_blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `pressure_attempts` | numeric | Number of pass attempts when under pressure. |
| `pressure_big_time_throws` | numeric | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | numeric | Number of intentional throwaways when not blitzed. |
| `blitz_drops` | numeric | Number of catchable passes dropped by receivers when blitzed. |
| `no_pressure_touchdowns` | numeric | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_pressure_sacks` | numeric | Number of sacks taken from a clean pocket (no pressure). |
| `blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when blitzed. |
| `pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when under pressure. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `no_pressure_attempts` | numeric | Number of pass attempts from a clean pocket (no pressure). |
| `no_blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when not blitzed. |
| `blitz_scrambles` | numeric | Number of scrambles when blitzed. |
| `blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `no_pressure_qb_rating` | numeric | Traditional NFL passer rating from a clean pocket (no pressure). |
| `no_blitz_passing_snaps` | numeric | Number of passing snaps played when not blitzed. |
| `no_blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when not blitzed, as charted by PFF. |
| `blitz_yards` | numeric | Passing yards gained when blitzed. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `no_blitz_attempts` | numeric | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `no_blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `no_blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_blitz_ypa` | numeric | Yards gained per pass attempt when not blitzed. |
| `blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when blitzed. |
| `pressure_interceptions` | numeric | Number of passes intercepted when under pressure. |
| `blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `no_pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when blitzed. |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `pressure_grades_pass` | numeric | PFF passing grade (0-100) when under pressure. |
| `no_blitz_qb_rating` | numeric | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `pressure_ypa` | numeric | Yards gained per pass attempt when under pressure. |
| `blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds when under pressure. |
| `no_blitz_completions` | numeric | Number of completed passes when not blitzed. |
| `no_pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `player` | character | Player's display name as PFF lists it. |
| `no_blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `no_pressure_pressure_to_sack_rate` | character | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `no_pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | numeric | Number of passing first downs gained when under pressure. |
| `blitz_passing_snaps` | numeric | Number of passing snaps played when blitzed. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `no_pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_blitz_sacks` | numeric | Number of sacks taken when not blitzed. |
| `no_blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `no_pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `no_blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `no_pressure_grades_pass` | numeric | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `no_pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `pressure_spikes` | numeric | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `pressure_qb_rating` | numeric | Traditional NFL passer rating when under pressure. |
| `blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | numeric | Number of passing touchdowns thrown when under pressure. |
| `pressure_thrown_aways` | numeric | Number of intentional throwaways when under pressure. |
| `no_blitz_interceptions` | numeric | Number of passes intercepted when not blitzed. |
| `blitz_dropbacks` | numeric | Number of dropbacks when blitzed. |
| `blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `no_blitz_big_time_throws` | numeric | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_pressure_avg_depth_of_target` | numeric | Average depth of target in air yards from a clean pocket (no pressure). |
| `no_pressure_yards` | numeric | Passing yards gained from a clean pocket (no pressure). |
| `blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `no_pressure_big_time_throws` | numeric | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when not blitzed. |
| `blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when under pressure. |
| `pressure_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when under pressure. |
| `no_blitz_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when not blitzed. |
| `no_blitz_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when not blitzed. |
| `blitz_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when blitzed. |
| `no_pressure_grades_defense` | numeric | PFF overall defense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when not blitzed. |
| `blitz_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when blitzed. |
| `blitz_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when blitzed. |
| `no_pressure_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when under pressure. |
| `pressure_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) when under pressure. |
| `blitz_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) when blitzed. |
| `no_pressure_grades_tackle` | numeric | PFF tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when blitzed. |
| `blitz_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when not blitzed. |
| `pressure_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when not blitzed. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_passing_pressure-example}

```python
pff_api_player_passing_pressure(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## pff_api_player_rushing_direction

Rushing by direction for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/rushing/direction`

**Valid URL:** [https://api.pff.com/v1/player/rushing/direction?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/rushing/direction?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_rushing_direction-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_rushing_direction-example}

```python
pff_api_player_rushing_direction(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## pff_api_player_rushing_summary

Rushing summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/rushing/summary`

**Valid URL:** [https://api.pff.com/v1/player/rushing/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/rushing/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_rushing_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_rushing_summary-example}

```python
pff_api_player_rushing_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._
