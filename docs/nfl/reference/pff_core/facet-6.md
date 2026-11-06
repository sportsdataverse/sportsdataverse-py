# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: passing–receiving

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: passing–receiving — function reference in sdv-py, the SportsDataverse Python package.

## pff_facet_passing_pressure

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/pressure`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/pressure](https://premium.pff.com/api/v1/facet/passing/pressure)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_passing_pressure-returns}

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_passing_pressure-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_passing_pressure-example}

```python
pff_facet_passing_pressure()
```

## pff_facet_receiving_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/summary](https://premium.pff.com/api/v1/facet/receiving/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_receiving_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | Float64 | Times targeted. |
| `grades_pass_block` | Float64 | PFF pass-blocking grade, 0-100. |
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception. |
| `grades_pass_route` | Float64 | PFF receiving/route grade (0-100). |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `yprr` | Float64 | Yards per route run. |
| `wide_snaps` | Float64 | Receiving snaps aligned out wide. |
| `fumbles` | Float64 | Fumbles by the player after the catch. |
| `first_downs` | Float64 | Receptions that converted a first down. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `inline_snaps` | Float64 | Receiving snaps aligned inline, tight to the formation. |
| `contested_targets` | Float64 | Contested targets. |
| `player_game_count` | Float64 | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `inline_rate` | Float64 | Share of receiving snaps aligned inline, tight to the formation. |
| `contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught. |
| `yards` | Float64 | Receiving yards. |
| `receptions` | Float64 | Passes caught by the receiver. |
| `targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player. |
| `interceptions` | Float64 | Interceptions on passes targeting the player. |
| `caught_percent` | Float64 | Percentage of targets caught. |
| `drop_rate` | Float64 | Share of catchable targets the player dropped. |
| `grades_hands_drop` | Float64 | PFF hands/drop grade (0-100). |
| `slot_rate` | Float64 | Share of receiving snaps aligned in the slot. |
| `slot_snaps` | Float64 | Receiving snaps aligned in the slot. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `wide_rate` | Float64 | Share of receiving snaps aligned out wide. |
| `pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `route_rate` | Float64 | Share of pass-play snaps on which the player ran a route. |
| `drops` | Float64 | Dropped passes. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | Float64 | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | Float64 | Longest reception in yards. |
| `pass_blocks` | Float64 | Pass-play snaps spent pass blocking. |
| `routes` | Float64 | Pass routes run by the receiver. |
| `pass_plays` | Float64 | Pass-play snaps. |
| `yards_per_reception` | Float64 | Average yards per reception. |
| `player` | String | Player's display name as PFF lists it. |
| `positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `contested_receptions` | Float64 | Contested catches made. |
| `yards_after_catch` | Float64 | Yards after the catch. |
| `avg_depth_of_target` | Float64 | Average depth of target in yards downfield. |
| `epa` | Float64 | Expected points added per target to the player, as computed by PFF (an average such as -0.27, not a total). |
| `avoided_tackles` | Float64 | Tackles avoided after the catch. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | Float64 | Receiving touchdowns. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_receiving_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_receiving_summary-example}

```python
pff_facet_receiving_summary()
```

## pff_facet_return_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/return/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/return/summary](https://premium.pff.com/api/v1/facet/return/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_return_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `grades_kick_return` | Float64 | PFF kickoff-return grade, 0-100. |
| `grades_return` | Float64 | PFF overall return grade, 0-100. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `kickoff_attempts` | Float64 | Kickoff returns attempted. |
| `kickoff_fair_catches` | Float64 | Kickoffs fair-caught by the player. |
| `kickoff_long` | Float64 | Longest kickoff return in yards. |
| `kickoff_muffed_returns` | Float64 | Kickoff returns the player muffed. |
| `kickoff_touchdowns` | Float64 | Kickoff returns scoring a touchdown. |
| `kickoff_yards` | Float64 | Total kickoff-return yards. |
| `kickoff_ypa` | Float64 | Average yards per kickoff return. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `player` | String | Player's display name as PFF lists it. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `punt_attempts` | Float64 | Punt returns attempted. |
| `punt_fair_catches` | Float64 | Punts fair-caught by the player. |
| `punt_long` | Float64 | Longest punt return in yards. |
| `punt_muffed_returns` | Float64 | Punt returns the player muffed. |
| `punt_touchdowns` | Float64 | Punt returns scoring a touchdown. |
| `punt_yards` | Float64 | Total punt-return yards. |
| `punt_ypa` | Float64 | Average yards per punt return. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `total_attempts` | Float64 | Total return attempts, kickoffs and punts combined. |
| `grades_punt_return` | Float64 | PFF punt-return grade, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_return_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_return_summary-example}

```python
pff_facet_return_summary()
```

## pff_facet_rushing_direction_stats

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/rushing/direction`

**Valid URL:** [https://premium.pff.com/api/v1/facet/rushing/direction](https://premium.pff.com/api/v1/facet/rushing/direction)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_rushing_direction_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `directions` | List | Nested per-direction rushing splits (attempts and results by run direction) as returned by the PFF API. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `player` | String | Player's display name as PFF lists it. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `total_attempts` | Float64 | Total rushing attempts across all run directions. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_rushing_direction_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_rushing_direction_stats-example}

```python
pff_facet_rushing_direction_stats()
```

## pff_facet_receiving_coverage_stats

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`

**Valid URL:** [https://premium.pff.com/api/v1/facet/defense/coverage_matchup](https://premium.pff.com/api/v1/facet/defense/coverage_matchup)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_receiving_coverage_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_receiving_coverage_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_receiving_coverage_stats-example}

```python
pff_facet_receiving_coverage_stats()
```

## pff_facet_rushing_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/rushing/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/rushing/summary](https://premium.pff.com/api/v1/facet/rushing/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_rushing_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | Float64 | Passes thrown to the ball carrier (targets). |
| `grades_pass_block` | Float64 | PFF pass-blocking grade, 0-100. |
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `yards_after_contact` | Float64 | Yards after contact. |
| `explosive` | Float64 | Runs PFF designates as explosive. |
| `grades_pass_route` | Float64 | PFF route-running (receiving) grade, 0-100. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `elu_rush_mtf` | Float64 | Missed tackles forced as a rusher, an input to PFF's elusive rating. |
| `breakaway_attempts` | Float64 | Runs of 15 or more yards, PFF's breakaway designation. |
| `designed_yards` | Float64 | Rushing yards gained on designed runs, excluding scrambles. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `yprr` | Float64 | Yards per route run. |
| `breakaway_percent` | Float64 | Share of rushing yards gained on breakaway runs of 15 or more yards. |
| `fumbles` | Float64 | Fumbles by the ball carrier. |
| `first_downs` | Float64 | Rushing first downs. |
| `elusive_rating` | Float64 | PFF elusive rating. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `breakaway_yards` | Float64 | Breakaway (long-run) yards. |
| `player_game_count` | Float64 | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `total_touches` | Float64 | Combined carries and receptions. |
| `scramble_yards` | Float64 | Rushing yards gained on scrambles. |
| `yco_attempt` | Float64 | Average yards after contact per rushing attempt. |
| `yards` | Float64 | Total rushing yards gained. |
| `grades_run_block` | Float64 | PFF run-blocking grade, 0-100. |
| `receptions` | Float64 | Passes caught by the ball carrier. |
| `zone_attempts` | Float64 | Rushing attempts on zone-scheme runs. |
| `scrambles` | Float64 | Quarterback scrambles. |
| `grades_run` | Float64 | PFF rushing grade (0-100). |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `attempts` | Float64 | Rushing attempts (carries) by the runner. |
| `elu_yco` | Float64 | Yards-after-contact component used in PFF's elusive rating. |
| `elu_recv_mtf` | Float64 | Missed tackles forced as a receiver, an input to PFF's elusive rating. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `ypa` | Float64 | Average yards per rushing attempt. |
| `drops` | Float64 | Passes dropped by the ball carrier. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `grades_hands_fumble` | Float64 | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | Float64 | Longest run in yards. |
| `routes` | Float64 | Pass routes run by the player. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `rec_yards` | Float64 | Receiving yards gained by the ball carrier (the rushing report also carries his receiving line). |
| `gap_attempts` | Float64 | Rushing attempts on gap-scheme runs. |
| `run_plays` | Float64 | Run-play snaps. |
| `avoided_tackles` | Float64 | Missed tackles forced. |
| `grades_offense_penalty` | Float64 | PFF offensive penalty grade, 0-100. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | Float64 | Rushing touchdowns. |
| `grades_pass` | Float64 | PFF passing grade, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_rushing_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_rushing_summary-example}

```python
pff_facet_rushing_summary()
```

## pff_facet_slot_coverages

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`

**Valid URL:** [https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage](https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_slot_coverages-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `coverage_snaps` | Float64 | Coverage snaps played while covering the slot. |
| `coverage_snaps_per_reception` | Float64 | Coverage snaps played per reception allowed while covering the slot. |
| `coverage_snaps_per_target` | Float64 | Coverage snaps played per target into the player's coverage while covering the slot. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `interceptions` | Float64 | Interceptions made by the player while covering the slot. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `player` | String | Player's display name as PFF lists it. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `qb_rating_against` | Float64 | NFL passer rating allowed on targets into the player's coverage while covering the slot. |
| `receptions` | Float64 | Receptions allowed in the player's coverage while covering the slot. |
| `targets` | Float64 | Passes thrown into the player's coverage (targets allowed) while covering the slot. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `touchdowns` | Float64 | Touchdowns allowed into the player's coverage while covering the slot. |
| `yards` | Float64 | Receiving yards allowed in the player's coverage while covering the slot. |
| `yards_after_catch` | Float64 | Yards after the catch allowed in the player's coverage while covering the slot. |
| `yards_per_coverage_snap` | Float64 | Receiving yards allowed per coverage snap while covering the slot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_slot_coverages-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_slot_coverages-example}

```python
pff_facet_slot_coverages()
```

## pff_facet_pbes

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`

**Valid URL:** [https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line](https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_pbes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `attempts` | Float64 | Pass plays the team's offensive line blocked on over the covered span, as counted by PFF (equal to pass_snaps in the captured rows). |
| `franchise_id` | Float64 | PFF franchise (team) id of the team whose offensive line the row describes (integer join key). |
| `hits_allowed` | Float64 | Quarterback hits allowed. |
| `hurries_allowed` | Float64 | Quarterback hurries allowed. |
| `pass_snaps` | Float64 | Pass-play snaps. |
| `pbe` | Float64 | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `pressures_allowed` | Float64 | Total pressures allowed (sacks, hits, and hurries). |
| `sacks_allowed` | Float64 | Sacks allowed by the team's offensive line over the covered span. |
| `season_id` | Float64 | Season (year) the row covers (e.g. 2022). |
| `team_name` | String | Abbreviation of the team whose offensive line the row describes (e.g. "ATL"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_pbes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_pbes-example}

```python
pff_facet_pbes()
```

## pff_facet_prps

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`

**Valid URL:** [https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush](https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_prps-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `lhs_sacks` | Float64 | Sacks recorded when rushing from the left side. |
| `rhs_hits` | Float64 | Quarterback hits recorded when rushing from the right side. |
| `rhs_prp` | Float64 | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks when rushing from the right side. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `pass_snaps` | Float64 | Pass-play snaps. |
| `lhs_hurries` | Float64 | Quarterback hurries recorded when rushing from the left side. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `prp` | Float64 | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `tackles` | Float64 | Tackles made by the player, as charted by PFF. |
| `rhs_pressures` | Float64 | Total pressures generated (sacks, hits, and hurries) when rushing from the right side. |
| `rhs_pass_rush_percent` | Float64 | Share of pass-play snaps spent rushing the passer when rushing from the right side. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `lhs_pass_rush_percent` | Float64 | Share of pass-play snaps spent rushing the passer when rushing from the left side. |
| `lhs_pass_rush_snaps` | Float64 | Pass-rush snaps played when rushing from the left side. |
| `sacks` | Float64 | Sacks recorded by the pass rusher. |
| `lhs_assists` | Float64 | Assisted tackles when rushing from the left side. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `lhs_pressures` | Float64 | Total pressures generated (sacks, hits, and hurries) when rushing from the left side. |
| `pass_rush_snaps` | Float64 | Pass-rush snaps played. |
| `hurries` | Float64 | Quarterback hurries recorded. |
| `rhs_pass_rush_snaps` | Float64 | Pass-rush snaps played when rushing from the right side. |
| `lhs_prp` | Float64 | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks when rushing from the left side. |
| `hits` | Float64 | Quarterback hits recorded by the pass rusher. |
| `lhs_hits` | Float64 | Quarterback hits recorded when rushing from the left side. |
| `rhs_tackles` | Float64 | Tackles made when rushing from the right side. |
| `lhs_stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense when rushing from the left side. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense. |
| `rhs_misses` | Float64 | Missed tackles when rushing from the right side. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `pressures` | Float64 | Total pressures generated (sacks, hits, and hurries). |
| `misses` | Float64 | Missed tackles. |
| `player` | String | Player's display name as PFF lists it. |
| `rhs_sacks` | Float64 | Sacks recorded when rushing from the right side. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `pass_rush_percent` | Float64 | Share of pass-play snaps spent rushing the passer. |
| `rhs_hurries` | Float64 | Quarterback hurries recorded when rushing from the right side. |
| `rhs_assists` | Float64 | Assisted tackles when rushing from the right side. |
| `rhs_stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense when rushing from the right side. |
| `assists` | Float64 | Assisted tackles credited to the player. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `lhs_tackles` | Float64 | Tackles made when rushing from the left side. |
| `lhs_misses` | Float64 | Missed tackles when rushing from the left side. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_prps-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_prps-example}

```python
pff_facet_prps()
```

## pff_facet_receiving_scheme

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/scheme`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/scheme](https://premium.pff.com/api/v1/facet/receiving/scheme)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_receiving_scheme-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `man_contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught against man coverage. |
| `man_touchdowns` | Float64 | Receiving touchdowns scored against man coverage. |
| `zone_targets_percent` | Float64 | Share of the team's targets thrown to the player against zone coverage. |
| `man_interceptions` | Float64 | Interceptions thrown on passes targeting the player against man coverage. |
| `zone_epa` | Float64 | Total expected points added on targets to the player against zone coverage. |
| `man_avg_depth_of_target` | Float64 | Average depth of target in yards downfield against man coverage. |
| `zone_pass_blocks` | Float64 | Pass-play snaps spent pass blocking against zone coverage. |
| `zone_avoided_tackles` | Float64 | Tackles avoided after the catch against zone coverage. |
| `man_targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player against man coverage. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `zone_positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added against zone coverage. |
| `man_targets_percent` | Float64 | Share of the team's targets thrown to the player against man coverage. |
| `man_yards_per_reception` | Float64 | Average yards per reception against man coverage. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `man_drop_rate` | Float64 | Share of catchable targets the player dropped against man coverage. |
| `zone_grades_pass_route` | Float64 | PFF route-running (receiving) grade against zone coverage, 0-100. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `zone_fumbles` | Float64 | Fumbles by the player after the catch against zone coverage. |
| `man_pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking against man coverage. |
| `zone_yards` | Float64 | Receiving yards gained against zone coverage. |
| `man_yprr` | Float64 | Yards per route run against man coverage. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `zone_drops` | Float64 | PFF-charted drops against zone coverage. |
| `zone_receptions` | Float64 | Receptions made against zone coverage. |
| `man_pass_plays` | Float64 | Pass-play snaps against man coverage. |
| `man_epa` | Float64 | Total expected points added on targets to the player against man coverage. |
| `zone_yards_per_reception` | Float64 | Average yards per reception against zone coverage. |
| `man_contested_targets` | Float64 | PFF-charted contested targets against man coverage. |
| `man_longest` | Float64 | Longest reception in yards against man coverage. |
| `zone_yards_after_catch` | Float64 | Yards gained after the catch against zone coverage. |
| `man_receptions` | Float64 | Receptions made against man coverage. |
| `man_avoided_tackles` | Float64 | Tackles avoided after the catch against man coverage. |
| `man_first_downs` | Float64 | Receptions that converted a first down against man coverage. |
| `base_targets` | Float64 | Total targets from the facet's unsplit base row, across all coverage schemes. |
| `zone_contested_catch_rate` | Float64 | Percentage of PFF-charted contested targets caught against zone coverage. |
| `zone_targeted_qb_rating` | Float64 | NFL passer rating on throws targeting the player against zone coverage. |
| `man_routes` | Float64 | Pass routes run by the player against man coverage. |
| `zone_grades_hands_drop` | Float64 | PFF hands/drop grade against zone coverage, 0-100. |
| `man_route_rate` | Float64 | Share of pass-play snaps on which the player ran a route against man coverage. |
| `zone_pass_block_rate` | Float64 | Share of pass-play snaps spent pass blocking against zone coverage. |
| `man_grades_pass_route` | Float64 | PFF route-running (receiving) grade against man coverage, 0-100. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `zone_first_downs` | Float64 | Receptions that converted a first down against zone coverage. |
| `zone_yprr` | Float64 | Yards per route run against zone coverage. |
| `man_drops` | Float64 | PFF-charted drops against man coverage. |
| `zone_caught_percent` | Float64 | Percentage of targets caught against zone coverage. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `man_fumbles` | Float64 | Fumbles by the player after the catch against man coverage. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | Float64 | Yards gained after the catch against man coverage. |
| `man_yards` | Float64 | Receiving yards gained against man coverage. |
| `zone_pass_plays` | Float64 | Pass-play snaps against zone coverage. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `man_targets` | Float64 | Pass targets to the player against man coverage. |
| `man_grades_hands_drop` | Float64 | PFF hands/drop grade against man coverage, 0-100. |
| `man_pass_blocks` | Float64 | Pass-play snaps spent pass blocking against man coverage. |
| `zone_touchdowns` | Float64 | Receiving touchdowns scored against zone coverage. |
| `zone_route_rate` | Float64 | Share of pass-play snaps on which the player ran a route against zone coverage. |
| `zone_yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception against zone coverage. |
| `zone_avg_depth_of_target` | Float64 | Average depth of target in yards downfield against zone coverage. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `zone_contested_targets` | Float64 | PFF-charted contested targets against zone coverage. |
| `zone_contested_receptions` | Float64 | Catches made on PFF-charted contested targets against zone coverage. |
| `man_contested_receptions` | Float64 | Catches made on PFF-charted contested targets against man coverage. |
| `man_yards_after_catch_per_reception` | Float64 | Average yards after the catch per reception against man coverage. |
| `zone_targets` | Float64 | Pass targets to the player against zone coverage. |
| `zone_longest` | Float64 | Longest reception in yards against zone coverage. |
| `man_caught_percent` | Float64 | Percentage of targets caught against man coverage. |
| `zone_drop_rate` | Float64 | Share of catchable targets the player dropped against zone coverage. |
| `zone_interceptions` | Float64 | Interceptions thrown on passes targeting the player against zone coverage. |
| `man_positive_epa_percent` | Float64 | Percentage of the receiver's plays with an EPA value (in practice their routes run) producing positive expected points added against man coverage. |
| `zone_routes` | Float64 | Pass routes run by the player against zone coverage. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_receiving_scheme-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_receiving_scheme-example}

```python
pff_facet_receiving_scheme()
```
