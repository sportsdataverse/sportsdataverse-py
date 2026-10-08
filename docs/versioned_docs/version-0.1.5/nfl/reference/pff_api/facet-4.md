---
title: "NFL — PFF Developer API (api.pff.com, API key) — Facet: passing–receiving"
sidebar_label: "Facet: passing–receiving"
sidebar_position: 4
description: "NFL — PFF Developer API (api.pff.com, API key) — Facet: passing–receiving — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Facet: passing–receiving

## pff_api_facet_passing_pressure

League-wide passing-under-pressure leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/pressure`

**Valid URL:** [https://api.pff.com/v1/facet/passing/pressure?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/pressure?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_passing_pressure-returns}

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
| `blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when blitzed, as charted by PFF. |
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
| `blitz_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, when blitzed. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when under pressure, as charted by PFF. |
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
| `no_pressure_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, from a clean pocket (no pressure). |
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
| `no_blitz_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, when not blitzed. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `no_pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) from a clean pocket (no pressure), as charted by PFF. |
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
| `no_blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when not blitzed, as charted by PFF. |
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
| `pressure_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, when under pressure. |
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

### Example {#pff_api_facet_passing_pressure-example}

```python
pff_api_facet_passing_pressure(league='nfl', season='2022')
```

_Last validated n/a._

## pff_api_facet_passing_summary

League-wide passing summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/summary`

**Valid URL:** [https://api.pff.com/v1/facet/passing/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_passing_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `twp_rate` | numeric | Turnover-worthy-play rate. |
| `btt_rate` | numeric | Big-time-throw rate. |
| `spikes` | numeric | Clock-stopping spike plays. |
| `dropbacks` | numeric | Total quarterback dropbacks. |
| `thrown_aways` | numeric | Passes intentionally thrown away. |
| `draft_season` | numeric | Draft class (year) of the player. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `hit_as_threw` | numeric | Plays where the quarterback was hit as he threw. |
| `first_downs` | numeric | Passing first downs. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `sack_percent` | numeric | Sack rate (sacks per dropback). |
| `bats` | numeric | Passes batted at the line. |
| `sacks` | numeric | Times the passer was sacked. |
| `player_game_count` | numeric | Games with at least one qualifying dropback in the requested range. |
| `eligible_season` | numeric | First eligible season for the player. |
| `completions` | numeric | Completed passes by the passer. |
| `yards` | numeric | Total passing yards gained. |
| `accuracy_percent` | numeric | Charted accuracy percentage. |
| `scrambles` | numeric | Scramble plays. |
| `interceptions` | numeric | Interceptions thrown. |
| `drop_rate` | numeric | Receiver drop rate on the quarterback's throws. |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `qb_rating` | numeric | NFL passer rating. |
| `completion_percent` | numeric | Completion percentage. |
| `penalties` | numeric | Penalties charged. |
| `attempts` | numeric | Pass attempts thrown by the passer. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Declined penalties. |
| `passing_snaps` | numeric | Number of passing snaps played. |
| `pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | numeric | Yards gained per pass attempt. |
| `drops` | numeric | Passes dropped by the passer's receivers. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release. |
| `big_time_throws` | numeric | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | character | Player's display name as PFF lists it. |
| `positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `avg_depth_of_target` | numeric | Average depth of target in air yards. |
| `turnover_worthy_plays` | numeric | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | numeric | Expected points added per play on the passer's dropbacks, as computed by PFF (an average such as 0.14, not a total). |
| `aimed_passes` | numeric | Aimed passes: attempts excluding throwaways, spikes, batted passes and throws made while hit (the accuracy_percent denominator). |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Number of passing touchdowns thrown. |
| `def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_passing_summary-example}

```python
pff_api_facet_passing_summary(league='nfl', season='2022')
```

_Last validated n/a._

## pff_api_facet_receiving_concept

League-wide receiving-by-concept leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/concept`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/concept?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/concept?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_receiving_concept-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `screen_caught_percent` | numeric | Percentage of targets caught on screen concepts. |
| `screen_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on screen concepts. |
| `slot_grades_pass_route` | numeric | PFF route-running (receiving) grade when aligned in the slot, 0-100. |
| `slot_avg_depth_of_target` | numeric | Average depth of target in yards downfield when aligned in the slot. |
| `slot_positive_epa_percent` | numeric | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added when aligned in the slot. |
| `screen_yprr` | numeric | Yards per route run on screen concepts. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `slot_routes` | numeric | Pass routes run by the player when aligned in the slot. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `screen_grades_hands_drop` | numeric | PFF hands/drop grade on screen concepts, 0-100. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `screen_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on screen concepts. |
| `slot_yards_per_reception` | numeric | Average yards per reception when aligned in the slot. |
| `screen_interceptions` | numeric | Interceptions thrown on passes targeting the player on screen concepts. |
| `slot_targets_percent` | numeric | Share of the team's targets thrown to the player when aligned in the slot. |
| `screen_longest` | numeric | Longest reception in yards on screen concepts. |
| `slot_avoided_tackles` | numeric | Tackles avoided after the catch when aligned in the slot. |
| `slot_yards_after_catch` | numeric | Yards gained after the catch when aligned in the slot. |
| `slot_grades_hands_drop` | numeric | PFF hands/drop grade when aligned in the slot, 0-100. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `screen_contested_targets` | numeric | PFF-charted contested targets on screen concepts. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `screen_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on screen concepts. |
| `screen_yards` | numeric | Receiving yards gained on screen concepts. |
| `slot_route_rate` | numeric | Share of pass-play snaps on which the player ran a route when aligned in the slot. |
| `screen_drop_rate` | numeric | Share of catchable targets the player dropped on screen concepts. |
| `screen_epa` | numeric | Total expected points added on targets to the player on screen concepts. |
| `screen_grades_pass_route` | numeric | PFF route-running (receiving) grade on screen concepts, 0-100. |
| `screen_drops` | numeric | PFF-charted drops on screen concepts. |
| `screen_fumbles` | numeric | Fumbles by the player after the catch on screen concepts. |
| `slot_interceptions` | numeric | Interceptions thrown on passes targeting the player when aligned in the slot. |
| `screen_yards_after_catch` | numeric | Yards gained after the catch on screen concepts. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in yards downfield on screen concepts. |
| `screen_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on screen concepts. |
| `slot_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking when aligned in the slot. |
| `slot_yprr` | numeric | Yards per route run when aligned in the slot. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all concepts. |
| `slot_longest` | numeric | Longest reception in yards when aligned in the slot. |
| `slot_drops` | numeric | PFF-charted drops when aligned in the slot. |
| `screen_routes` | numeric | Pass routes run by the player on screen concepts. |
| `slot_fumbles` | numeric | Fumbles by the player after the catch when aligned in the slot. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `slot_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught when aligned in the slot. |
| `slot_pass_plays` | numeric | Pass-play snaps when aligned in the slot. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `screen_positive_epa_percent` | numeric | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added on screen concepts. |
| `slot_first_downs` | numeric | Receptions that converted a first down when aligned in the slot. |
| `screen_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on screen concepts. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `screen_pass_blocks` | numeric | Pass-play snaps spent pass blocking on screen concepts. |
| `slot_targets` | numeric | Pass targets to the player when aligned in the slot. |
| `slot_pass_blocks` | numeric | Pass-play snaps spent pass blocking when aligned in the slot. |
| `slot_receptions` | numeric | Receptions made when aligned in the slot. |
| `screen_first_downs` | numeric | Receptions that converted a first down on screen concepts. |
| `slot_caught_percent` | numeric | Percentage of targets caught when aligned in the slot. |
| `screen_avoided_tackles` | numeric | Tackles avoided after the catch on screen concepts. |
| `player` | character | Player's display name as PFF lists it. |
| `slot_epa` | numeric | Total expected points added on targets to the player when aligned in the slot. |
| `slot_drop_rate` | numeric | Share of catchable targets the player dropped when aligned in the slot. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `slot_touchdowns` | numeric | Receiving touchdowns scored when aligned in the slot. |
| `slot_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception when aligned in the slot. |
| `screen_receptions` | numeric | Receptions made on screen concepts. |
| `slot_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player when aligned in the slot. |
| `slot_contested_targets` | numeric | PFF-charted contested targets when aligned in the slot. |
| `screen_yards_per_reception` | numeric | Average yards per reception on screen concepts. |
| `slot_contested_receptions` | numeric | Catches made on PFF-charted contested targets when aligned in the slot. |
| `screen_pass_plays` | numeric | Pass-play snaps on screen concepts. |
| `screen_contested_receptions` | numeric | Catches made on PFF-charted contested targets on screen concepts. |
| `screen_touchdowns` | numeric | Receiving touchdowns scored on screen concepts. |
| `screen_targets` | numeric | Pass targets to the player on screen concepts. |
| `screen_targets_percent` | numeric | Share of the team's targets thrown to the player on screen concepts. |
| `slot_yards` | numeric | Receiving yards gained when aligned in the slot. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_receiving_concept-example}

```python
pff_api_facet_receiving_concept(league='nfl', season='2022')
```

_Last validated n/a._

## pff_api_facet_receiving_coverage

League-wide receiving-versus-coverage leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/coverage`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/coverage?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/coverage?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns {#pff_api_facet_receiving_coverage-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**defenders**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | integer | Passes broken up in the row's coverage matchups. |
| `drops` | integer | Dropped passes in the row's coverage matchups. |
| `first_downs` | integer | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_coverage_defense` | numeric | The player's PFF coverage grade (0-100). |
| `grades_defense` | numeric | The player's PFF overall defense grade (0-100). |
| `grades_defense_penalty` | numeric | The player's PFF defensive penalty grade (0-100). |
| `grades_overall` | numeric | The player's PFF overall grade (0-100) over the requested filters. |
| `grades_overall_tackle` | numeric | The player's PFF overall tackling grade (0-100). |
| `grades_pass_rush_defense` | numeric | The player's PFF pass-rush grade (0-100). |
| `grades_run_defense` | numeric | The player's PFF run-defense grade (0-100). |
| `grades_tackle` | numeric | The player's PFF tackling grade (0-100). |
| `interceptions` | integer | Interceptions on throws in the row's coverage matchups. |
| `longest` | integer | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_game_count` | integer | Games the player appeared in over the requested filters. |
| `player_id` | integer | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | integer | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | integer | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | integer | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | integer | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | integer | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | numeric | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_offense` | numeric | The player's PFF overall offense grade (0-100). |
| `grades_offense_penalty` | numeric | The player's PFF offensive penalty grade (0-100). |
| `grades_run_block` | numeric | The player's PFF run-blocking grade (0-100). |
| `grades_pass_block` | numeric | The player's PFF pass-blocking grade (0-100). |
| `grades_hands_drop` | numeric | The player's PFF hands (drop) grade (0-100). |
| `grades_hands_fumble` | numeric | The player's PFF ball-security (fumble) grade (0-100). |
| `grades_pass_route` | numeric | The player's PFF receiving (route-running) grade (0-100). |
| `grades_pass` | numeric | The player's PFF passing grade (0-100). |
| `grades_run` | numeric | The player's PFF rushing grade (0-100). |

**receivers**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | integer | Passes broken up in the row's coverage matchups. |
| `drops` | integer | Dropped passes in the row's coverage matchups. |
| `first_downs` | integer | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_hands_drop` | numeric | The player's PFF hands (drop) grade (0-100). |
| `grades_hands_fumble` | numeric | The player's PFF ball-security (fumble) grade (0-100). |
| `grades_offense` | numeric | The player's PFF overall offense grade (0-100). |
| `grades_offense_penalty` | numeric | The player's PFF offensive penalty grade (0-100). |
| `grades_overall` | numeric | The player's PFF overall grade (0-100) over the requested filters. |
| `grades_pass_route` | numeric | The player's PFF receiving (route-running) grade (0-100). |
| `grades_run_block` | numeric | The player's PFF run-blocking grade (0-100). |
| `grades_screen_block` | numeric | The player's PFF screen-blocking grade (0-100). |
| `interceptions` | integer | Interceptions on throws in the row's coverage matchups. |
| `longest` | integer | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_game_count` | integer | Games the player appeared in over the requested filters. |
| `player_id` | integer | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | integer | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | integer | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | integer | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | integer | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | integer | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | numeric | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_pass` | numeric | The player's PFF passing grade (0-100). |
| `grades_pass_block` | numeric | The player's PFF pass-blocking grade (0-100). |
| `grades_run` | numeric | The player's PFF rushing grade (0-100). |
| `grades_defense` | numeric | The player's PFF overall defense grade (0-100). |
| `grades_defense_penalty` | numeric | The player's PFF defensive penalty grade (0-100). |
| `grades_pass_rush_defense` | numeric | The player's PFF pass-rush grade (0-100). |
| `grades_run_defense` | numeric | The player's PFF run-defense grade (0-100). |
| `grades_coverage_defense` | numeric | The player's PFF coverage grade (0-100). |
| `grades_overall_tackle` | numeric | The player's PFF overall tackling grade (0-100). |
| `grades_tackle` | numeric | The player's PFF tackling grade (0-100). |
| `grades_snap` | numeric | PFF grade reported as grades_snap (0-100); PFF does not document it, and the capture shows it only in the ncaa receivers frame. |

**versus**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | integer | Passes broken up in the row's coverage matchups. |
| `coverage_player_id` | integer | PFF player id of the defender covering the receiver (versus only). |
| `drops` | integer | Dropped passes in the row's coverage matchups. |
| `first_downs` | integer | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `interceptions` | integer | Interceptions on throws in the row's coverage matchups. |
| `longest` | integer | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_id` | integer | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | integer | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | integer | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | integer | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | integer | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | integer | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | numeric | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_facet_receiving_coverage-example}

```python
pff_api_facet_receiving_coverage(league='nfl', season='2022')
```

_Last validated n/a._
