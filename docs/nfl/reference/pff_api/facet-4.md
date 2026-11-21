# NFL — PFF Developer API (api.pff.com, API key) — Facet: passing–receiving

> NFL — PFF Developer API (api.pff.com, API key) — Facet: passing–receiving — function reference in sdv-py, the SportsDataverse Python package.

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
| `no_blitz_completion_percent` | Float64 | Percentage of pass attempts completed when not blitzed. |
| `grades_offense` | Float64 | PFF overall offense grade for the player (0-100). |
| `no_pressure_scrambles` | Float64 | Number of scrambles from a clean pocket (no pressure). |
| `blitz_touchdowns` | Float64 | Number of passing touchdowns thrown when blitzed. |
| `pressure_yards` | Float64 | Passing yards gained when under pressure. |
| `no_pressure_spikes` | Float64 | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | Float64 | Yards gained per pass attempt when blitzed. |
| `no_blitz_grades_run` | Float64 | PFF rushing grade for the player (0-100) when not blitzed. |
| `blitz_qb_rating` | Float64 | Traditional NFL passer rating when blitzed. |
| `no_pressure_thrown_aways` | Float64 | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_blitz_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `no_blitz_drops` | Float64 | Number of catchable passes dropped by receivers when not blitzed. |
| `no_pressure_completion_percent` | Float64 | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when blitzed, as charted by PFF. |
| `pressure_grades_run` | Float64 | PFF rushing grade for the player (0-100) when under pressure. |
| `no_blitz_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `pressure_completions` | Float64 | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | Float64 | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | Float64 | Number of clock-stopping spikes when blitzed. |
| `no_pressure_completions` | Float64 | Number of completed passes from a clean pocket (no pressure). |
| `draft_season` | Float64 | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `no_pressure_passing_snaps` | Float64 | Number of passing snaps played from a clean pocket (no pressure). |
| `no_blitz_first_downs` | Float64 | Number of passing first downs gained when not blitzed. |
| `blitz_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, when blitzed. |
| `no_pressure_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `pressure_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when under pressure, as charted by PFF. |
| `blitz_sacks` | Float64 | Number of sacks taken when blitzed. |
| `no_pressure_interceptions` | Float64 | Number of passes intercepted from a clean pocket (no pressure). |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `pressure_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `blitz_completions` | Float64 | Number of completed passes when blitzed. |
| `blitz_attempts` | Float64 | Number of pass attempts when blitzed. |
| `pressure_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | Float64 | Number of sacks taken when under pressure. |
| `no_blitz_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | Float64 | Yards gained per pass attempt from a clean pocket (no pressure). |
| `pressure_passing_snaps` | Float64 | Number of passing snaps played when under pressure. |
| `grades_pass` | Float64 | PFF passing grade (0-100). |
| `pressure_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `blitz_thrown_aways` | Float64 | Number of intentional throwaways when blitzed. |
| `no_pressure_drops` | Float64 | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `pressure_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `pressure_scrambles` | Float64 | Number of scrambles when under pressure. |
| `blitz_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers when blitzed. |
| `pressure_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `blitz_completion_percent` | Float64 | Percentage of pass attempts completed when blitzed. |
| `no_pressure_first_downs` | Float64 | Number of passing first downs gained from a clean pocket (no pressure). |
| `blitz_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `no_blitz_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `blitz_interceptions` | Float64 | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | Float64 | Number of dropbacks when not blitzed. |
| `no_blitz_grades_pass` | Float64 | PFF passing grade (0-100) when not blitzed. |
| `no_blitz_scrambles` | Float64 | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers when under pressure. |
| `no_blitz_yards` | Float64 | Passing yards gained when not blitzed. |
| `base_dropbacks` | Float64 | Number of dropbacks across all splits, the baseline total for this facet. |
| `pressure_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `no_blitz_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `no_pressure_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, from a clean pocket (no pressure). |
| `pressure_dropbacks` | Float64 | Number of dropbacks when under pressure. |
| `no_blitz_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_run` | Float64 | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `player_game_count` | Float64 | Number of games the player appeared in during the period covered. |
| `blitz_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `no_blitz_touchdowns` | Float64 | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | Float64 | Average depth of target in air yards when not blitzed. |
| `no_pressure_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | Float64 | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | Float64 | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `blitz_avg_depth_of_target` | Float64 | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | Float64 | Number of clock-stopping spikes when not blitzed. |
| `no_pressure_dropbacks` | Float64 | Number of dropbacks from a clean pocket (no pressure). |
| `blitz_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_pressure_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `pressure_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added when under pressure. |
| `blitz_first_downs` | Float64 | Number of passing first downs gained when blitzed. |
| `no_blitz_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `pressure_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `no_pressure_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `no_blitz_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, when not blitzed. |
| `no_blitz_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added when not blitzed. |
| `pressure_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `no_pressure_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | Float64 | PFF rushing grade for the player (0-100). |
| `no_pressure_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `pressure_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) when under pressure. |
| `pressure_completion_percent` | Float64 | Percentage of pass attempts completed when under pressure. |
| `pressure_avg_depth_of_target` | Float64 | Average depth of target in air yards when under pressure. |
| `blitz_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `pressure_drops` | Float64 | Number of catchable passes dropped by receivers when under pressure. |
| `no_blitz_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `pressure_attempts` | Float64 | Number of pass attempts when under pressure. |
| `pressure_big_time_throws` | Float64 | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | Float64 | Number of intentional throwaways when not blitzed. |
| `blitz_drops` | Float64 | Number of catchable passes dropped by receivers when blitzed. |
| `no_pressure_touchdowns` | Float64 | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_pressure_sacks` | Float64 | Number of sacks taken from a clean pocket (no pressure). |
| `blitz_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack when blitzed. |
| `pressure_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack when under pressure. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `no_pressure_attempts` | Float64 | Number of pass attempts from a clean pocket (no pressure). |
| `no_blitz_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) when not blitzed. |
| `blitz_scrambles` | Float64 | Number of scrambles when blitzed. |
| `blitz_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `no_pressure_qb_rating` | Float64 | Traditional NFL passer rating from a clean pocket (no pressure). |
| `no_blitz_passing_snaps` | Float64 | Number of passing snaps played when not blitzed. |
| `no_blitz_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) when not blitzed, as charted by PFF. |
| `blitz_yards` | Float64 | Passing yards gained when blitzed. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `no_blitz_attempts` | Float64 | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `no_blitz_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `pressure_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `blitz_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `declined_penalties` | Float64 | Number of declined penalties committed by the player. |
| `no_blitz_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_blitz_ypa` | Float64 | Yards gained per pass attempt when not blitzed. |
| `blitz_grades_run` | Float64 | PFF rushing grade for the player (0-100) when blitzed. |
| `pressure_interceptions` | Float64 | Number of passes intercepted when under pressure. |
| `blitz_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `no_pressure_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `blitz_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) when blitzed. |
| `grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `pressure_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `pressure_grades_pass` | Float64 | PFF passing grade (0-100) when under pressure. |
| `no_blitz_qb_rating` | Float64 | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `pressure_ypa` | Float64 | Yards gained per pass attempt when under pressure. |
| `blitz_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `pressure_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, when under pressure. |
| `no_blitz_completions` | Float64 | Number of completed passes when not blitzed. |
| `no_pressure_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `player` | String | Player's display name as PFF lists it. |
| `no_blitz_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `no_pressure_pressure_to_sack_rate` | String | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `no_pressure_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | Float64 | Number of passing first downs gained when under pressure. |
| `blitz_passing_snaps` | Float64 | Number of passing snaps played when blitzed. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `no_pressure_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_blitz_sacks` | Float64 | Number of sacks taken when not blitzed. |
| `no_blitz_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `no_pressure_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `no_blitz_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `no_pressure_grades_pass` | Float64 | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `no_pressure_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `pressure_spikes` | Float64 | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `pressure_qb_rating` | Float64 | Traditional NFL passer rating when under pressure. |
| `blitz_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | Float64 | Number of passing touchdowns thrown when under pressure. |
| `pressure_thrown_aways` | Float64 | Number of intentional throwaways when under pressure. |
| `no_blitz_interceptions` | Float64 | Number of passes intercepted when not blitzed. |
| `blitz_dropbacks` | Float64 | Number of dropbacks when blitzed. |
| `blitz_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `no_blitz_big_time_throws` | Float64 | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_pressure_avg_depth_of_target` | Float64 | Average depth of target in air yards from a clean pocket (no pressure). |
| `no_pressure_yards` | Float64 | Passing yards gained from a clean pocket (no pressure). |
| `blitz_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `no_pressure_big_time_throws` | Float64 | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `pressure_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) when not blitzed. |
| `blitz_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `pressure_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) when under pressure. |
| `pressure_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) when under pressure. |
| `no_blitz_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) when not blitzed. |
| `no_blitz_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) when not blitzed. |
| `blitz_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) when blitzed. |
| `no_pressure_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) when not blitzed. |
| `blitz_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) when blitzed. |
| `blitz_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) when blitzed. |
| `no_pressure_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) when under pressure. |
| `pressure_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) when under pressure. |
| `blitz_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) when blitzed. |
| `no_pressure_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) when blitzed. |
| `blitz_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) when not blitzed. |
| `pressure_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_pass_rush_defense` | String | PFF pass-rush grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) when not blitzed. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_passing_pressure-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_passing_pressure-example}

```python
pff_api_facet_passing_pressure(league='nfl', season='2022')
```

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
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `twp_rate` | Float64 | Turnover-worthy-play rate. |
| `btt_rate` | Float64 | Big-time-throw rate. |
| `spikes` | Float64 | Clock-stopping spike plays. |
| `dropbacks` | Float64 | Total quarterback dropbacks. |
| `thrown_aways` | Float64 | Passes intentionally thrown away. |
| `draft_season` | Float64 | Draft class (year) of the player. |
| `team_name` | String | Team name/abbreviation the player is credited to for the range. |
| `grades_pass` | Float64 | PFF passing grade (0-100). |
| `hit_as_threw` | Float64 | Plays where the quarterback was hit as he threw. |
| `first_downs` | Float64 | Passing first downs. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `sack_percent` | Float64 | Sack rate (sacks per dropback). |
| `bats` | Float64 | Passes batted at the line. |
| `sacks` | Float64 | Times the passer was sacked. |
| `player_game_count` | Float64 | Games with at least one qualifying dropback in the requested range. |
| `eligible_season` | Float64 | First eligible season for the player. |
| `completions` | Float64 | Completed passes by the passer. |
| `yards` | Float64 | Total passing yards gained. |
| `accuracy_percent` | Float64 | Charted accuracy percentage. |
| `scrambles` | Float64 | Scramble plays. |
| `interceptions` | Float64 | Interceptions thrown. |
| `drop_rate` | Float64 | Receiver drop rate on the quarterback's throws. |
| `grades_run` | Float64 | PFF rushing grade (0-100). |
| `qb_rating` | Float64 | NFL passer rating. |
| `completion_percent` | Float64 | Completion percentage. |
| `penalties` | Float64 | Penalties charged. |
| `attempts` | Float64 | Pass attempts thrown by the passer. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Declined penalties. |
| `passing_snaps` | Float64 | Number of passing snaps played. |
| `pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | Float64 | Yards gained per pass attempt. |
| `drops` | Float64 | Passes dropped by the passer's receivers. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release. |
| `big_time_throws` | Float64 | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | String | Player's display name as PFF lists it. |
| `positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `avg_depth_of_target` | Float64 | Average depth of target in air yards. |
| `turnover_worthy_plays` | Float64 | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | Float64 | Expected points added per play on the passer's dropbacks, as computed by PFF (an average such as 0.14, not a total). |
| `aimed_passes` | Float64 | Aimed passes: attempts excluding throwaways, spikes, batted passes and throws made while hit (the accuracy_percent denominator). |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | Float64 | Number of passing touchdowns thrown. |
| `def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_passing_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_passing_summary-example}

```python
pff_api_facet_passing_summary(league='nfl', season='2022')
```

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
| `screen_caught_percent` | Float64 | Percentage of targets caught on screen concepts. |
| `screen_targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player on screen concepts. |
| `slot_grades_pass_route` | Float64 | PFF route-running (receiving) grade when aligned in the slot, 0-100. |
| `slot_avg_depth_of_target` | Float64 | Average depth of target in yards downfield when aligned in the slot. |
| `slot_positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added when aligned in the slot. |
| `screen_yprr` | Float64 | Yards per route run on screen concepts. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `slot_routes` | Float64 | Pass routes run by the player when aligned in the slot. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `screen_grades_hands_drop` | Float64 | PFF hands/drop grade on screen concepts, 0-100. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `screen_contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught on screen concepts. |
| `slot_yards_per_reception` | Float64 | Average yards per reception when aligned in the slot. |
| `screen_interceptions` | Float64 | Interceptions thrown on passes targeting the player on screen concepts. |
| `slot_targets_percent` | Float64 | Share of the team's targets thrown to the player when aligned in the slot. |
| `screen_longest` | Float64 | Longest reception in yards on screen concepts. |
| `slot_avoided_tackles` | Float64 | Tackles avoided after the catch when aligned in the slot. |
| `slot_yards_after_catch` | Float64 | Yards gained after the catch when aligned in the slot. |
| `slot_grades_hands_drop` | Float64 | PFF hands/drop grade when aligned in the slot, 0-100. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `screen_contested_targets` | Float64 | PFF-charted contested targets on screen concepts. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `screen_yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception on screen concepts. |
| `screen_yards` | Float64 | Receiving yards gained on screen concepts. |
| `slot_route_rate` | Float64 | Share of pass-play snaps on which the player ran a route when aligned in the slot. |
| `screen_drop_rate` | Float64 | Share of catchable targets the player dropped on screen concepts. |
| `screen_epa` | Float64 | Total expected points added on targets to the player on screen concepts. |
| `screen_grades_pass_route` | Float64 | PFF route-running (receiving) grade on screen concepts, 0-100. |
| `screen_drops` | Float64 | PFF-charted drops on screen concepts. |
| `screen_fumbles` | Float64 | Fumbles by the player after the catch on screen concepts. |
| `slot_interceptions` | Float64 | Interceptions thrown on passes targeting the player when aligned in the slot. |
| `screen_yards_after_catch` | Float64 | Yards gained after the catch on screen concepts. |
| `screen_avg_depth_of_target` | Float64 | Average depth of target in yards downfield on screen concepts. |
| `screen_pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking on screen concepts. |
| `slot_pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking when aligned in the slot. |
| `slot_yprr` | Float64 | Yards per route run when aligned in the slot. |
| `base_targets` | Float64 | Total targets from the facet's unsplit base row, across all concepts. |
| `slot_longest` | Float64 | Longest reception in yards when aligned in the slot. |
| `slot_drops` | Float64 | PFF-charted drops when aligned in the slot. |
| `screen_routes` | Float64 | Pass routes run by the player on screen concepts. |
| `slot_fumbles` | Float64 | Fumbles by the player after the catch when aligned in the slot. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `slot_contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught when aligned in the slot. |
| `slot_pass_plays` | Float64 | Pass-play snaps when aligned in the slot. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `screen_positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added on screen concepts. |
| `slot_first_downs` | Float64 | Receptions that converted a first down when aligned in the slot. |
| `screen_route_rate` | Float64 | Share of pass-play snaps on which the player ran a route on screen concepts. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `screen_pass_blocks` | Float64 | Pass-play snaps spent pass blocking on screen concepts. |
| `slot_targets` | Float64 | Pass targets to the player when aligned in the slot. |
| `slot_pass_blocks` | Float64 | Pass-play snaps spent pass blocking when aligned in the slot. |
| `slot_receptions` | Float64 | Receptions made when aligned in the slot. |
| `screen_first_downs` | Float64 | Receptions that converted a first down on screen concepts. |
| `slot_caught_percent` | Float64 | Percentage of targets caught when aligned in the slot. |
| `screen_avoided_tackles` | Float64 | Tackles avoided after the catch on screen concepts. |
| `player` | String | Player's display name as PFF lists it. |
| `slot_epa` | Float64 | Total expected points added on targets to the player when aligned in the slot. |
| `slot_drop_rate` | Float64 | Share of catchable targets the player dropped when aligned in the slot. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `slot_touchdowns` | Float64 | Receiving touchdowns scored when aligned in the slot. |
| `slot_yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception when aligned in the slot. |
| `screen_receptions` | Float64 | Receptions made on screen concepts. |
| `slot_targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player when aligned in the slot. |
| `slot_contested_targets` | Float64 | PFF-charted contested targets when aligned in the slot. |
| `screen_yards_per_reception` | Float64 | Average yards per reception on screen concepts. |
| `slot_contested_receptions` | Float64 | Catches made on PFF-charted contested targets when aligned in the slot. |
| `screen_pass_plays` | Float64 | Pass-play snaps on screen concepts. |
| `screen_contested_receptions` | Float64 | Catches made on PFF-charted contested targets on screen concepts. |
| `screen_touchdowns` | Float64 | Receiving touchdowns scored on screen concepts. |
| `screen_targets` | Float64 | Pass targets to the player on screen concepts. |
| `screen_targets_percent` | Float64 | Share of the team's targets thrown to the player on screen concepts. |
| `slot_yards` | Float64 | Receiving yards gained when aligned in the slot. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_receiving_concept-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_receiving_concept-example}

```python
pff_api_facet_receiving_concept(league='nfl', season='2022')
```

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
| `broken_up_passes` | Int64 | Passes broken up in the row's coverage matchups. |
| `drops` | Int64 | Dropped passes in the row's coverage matchups. |
| `first_downs` | Int64 | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_coverage_defense` | Float64 | The player's PFF coverage grade (0-100). |
| `grades_defense` | Float64 | The player's PFF overall defense grade (0-100). |
| `grades_defense_penalty` | Float64 | The player's PFF defensive penalty grade (0-100). |
| `grades_overall` | Float64 | The player's PFF overall grade (0-100) over the requested filters. |
| `grades_overall_tackle` | Float64 | The player's PFF overall tackling grade (0-100). |
| `grades_pass_rush_defense` | Float64 | The player's PFF pass-rush grade (0-100). |
| `grades_run_defense` | Float64 | The player's PFF run-defense grade (0-100). |
| `grades_tackle` | Float64 | The player's PFF tackling grade (0-100). |
| `interceptions` | Int64 | Interceptions on throws in the row's coverage matchups. |
| `longest` | Int64 | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_game_count` | Int64 | Games the player appeared in over the requested filters. |
| `player_id` | Int64 | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | Int64 | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | Int64 | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | Int64 | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | Int64 | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | Int64 | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | Float64 | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_offense` | Float64 | The player's PFF overall offense grade (0-100). |
| `grades_offense_penalty` | Float64 | The player's PFF offensive penalty grade (0-100). |
| `grades_run_block` | Float64 | The player's PFF run-blocking grade (0-100). |
| `grades_pass_block` | Float64 | The player's PFF pass-blocking grade (0-100). |
| `grades_hands_drop` | Float64 | The player's PFF hands (drop) grade (0-100). |
| `grades_hands_fumble` | Float64 | The player's PFF ball-security (fumble) grade (0-100). |
| `grades_pass_route` | Float64 | The player's PFF receiving (route-running) grade (0-100). |
| `grades_pass` | Float64 | The player's PFF passing grade (0-100). |
| `grades_run` | Float64 | The player's PFF rushing grade (0-100). |

**receivers**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | Int64 | Passes broken up in the row's coverage matchups. |
| `drops` | Int64 | Dropped passes in the row's coverage matchups. |
| `first_downs` | Int64 | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_hands_drop` | Float64 | The player's PFF hands (drop) grade (0-100). |
| `grades_hands_fumble` | Float64 | The player's PFF ball-security (fumble) grade (0-100). |
| `grades_offense` | Float64 | The player's PFF overall offense grade (0-100). |
| `grades_offense_penalty` | Float64 | The player's PFF offensive penalty grade (0-100). |
| `grades_overall` | Float64 | The player's PFF overall grade (0-100) over the requested filters. |
| `grades_pass_route` | Float64 | The player's PFF receiving (route-running) grade (0-100). |
| `grades_run_block` | Float64 | The player's PFF run-blocking grade (0-100). |
| `grades_screen_block` | Float64 | The player's PFF screen-blocking grade (0-100). |
| `interceptions` | Int64 | Interceptions on throws in the row's coverage matchups. |
| `longest` | Int64 | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_game_count` | Int64 | Games the player appeared in over the requested filters. |
| `player_id` | Int64 | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | Int64 | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | Int64 | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | Int64 | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | Int64 | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | Int64 | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | Float64 | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `grades_pass` | Float64 | The player's PFF passing grade (0-100). |
| `grades_pass_block` | Float64 | The player's PFF pass-blocking grade (0-100). |
| `grades_run` | Float64 | The player's PFF rushing grade (0-100). |
| `grades_defense` | Float64 | The player's PFF overall defense grade (0-100). |
| `grades_defense_penalty` | Float64 | The player's PFF defensive penalty grade (0-100). |
| `grades_pass_rush_defense` | Float64 | The player's PFF pass-rush grade (0-100). |
| `grades_run_defense` | Float64 | The player's PFF run-defense grade (0-100). |
| `grades_coverage_defense` | Float64 | The player's PFF coverage grade (0-100). |
| `grades_overall_tackle` | Float64 | The player's PFF overall tackling grade (0-100). |
| `grades_tackle` | Float64 | The player's PFF tackling grade (0-100). |
| `grades_snap` | Float64 | PFF grade reported as grades_snap (0-100); PFF does not document it, and the capture shows it only in the ncaa receivers frame. |

**versus**

| col_name | type | description |
|---|---|---|
| `broken_up_passes` | Int64 | Passes broken up in the row's coverage matchups. |
| `coverage_player_id` | Int64 | PFF player id of the defender covering the receiver (versus only). |
| `drops` | Int64 | Dropped passes in the row's coverage matchups. |
| `first_downs` | Int64 | Receiving first downs (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `interceptions` | Int64 | Interceptions on throws in the row's coverage matchups. |
| `longest` | Int64 | Longest reception, in yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `player_id` | Int64 | PFF player id: the covering defender (defenders) or the receiver (receivers, versus). |
| `receptions` | Int64 | Receptions (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `targets` | Int64 | Targets (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `touchdowns` | Int64 | Receiving touchdowns (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards` | Int64 | Receiving yards (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_after_catch` | Int64 | Yards after the catch (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |
| `yards_per_reception` | Float64 | Yards per reception (defenders: allowed in the defender's coverage; receivers / versus: by the receiver). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_facet_receiving_coverage-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_facet_receiving_coverage-example}

```python
pff_api_facet_receiving_coverage(league='nfl', season='2022')
```
