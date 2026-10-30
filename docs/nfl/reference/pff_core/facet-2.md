# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: passing–coverage

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: passing–coverage — function reference in sdv-py, the SportsDataverse Python package.

## pff_facet_passing_concept

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/passing/concept`

**Valid URL:** [https://premium.pff.com/api/v1/facet/passing/concept](https://premium.pff.com/api/v1/facet/passing/concept)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_passing_concept-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `comp_pct_diff` | Float64 | Difference in completion percentage between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `pa_grades_pass` | Float64 | PFF passing grade (0-100) on play-action dropbacks. |
| `no_screen_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) excluding screen passes. |
| `pa_qb_rating` | Float64 | Traditional NFL passer rating on play-action dropbacks. |
| `no_screen_qb_rating` | Float64 | Traditional NFL passer rating excluding screen passes. |
| `pa_completions` | Float64 | Number of completed passes on play-action dropbacks. |
| `pa_thrown_aways` | Float64 | Number of intentional throwaways on play-action dropbacks. |
| `pa_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came on play-action dropbacks, expressed as a percentage. |
| `ypa_diff` | Float64 | Difference in yards per attempt between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `no_screen_drops` | Float64 | Number of catchable passes dropped by receivers excluding screen passes. |
| `screen_completion_percent` | Float64 | Percentage of pass attempts completed on screen passes. |
| `npa_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage on non-play-action dropbacks. |
| `no_screen_thrown_aways` | Float64 | Number of intentional throwaways excluding screen passes. |
| `pa_grades_run` | Float64 | PFF rushing grade for the player (0-100) on play-action dropbacks. |
| `screen_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack on screen passes. |
| `pa_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers on play-action dropbacks. |
| `screen_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) on screen passes. |
| `dropbacks` | Float64 | Number of dropbacks. |
| `npa_thrown_aways` | Float64 | Number of intentional throwaways on non-play-action dropbacks. |
| `no_screen_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks excluding screen passes, as charted by PFF. |
| `screen_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on screen passes. |
| `pa_touchdowns` | Float64 | Number of passing touchdowns thrown on play-action dropbacks. |
| `npa_ypa` | Float64 | Yards gained per pass attempt on non-play-action dropbacks. |
| `draft_season` | Float64 | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `screen_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, on screen passes. |
| `screen_thrown_aways` | Float64 | Number of intentional throwaways on screen passes. |
| `npa_sacks` | Float64 | Number of sacks taken on non-play-action dropbacks. |
| `npa_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on non-play-action dropbacks, as charted by PFF. |
| `no_screen_completions` | Float64 | Number of completed passes excluding screen passes. |
| `no_screen_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `screen_spikes` | Float64 | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | Float64 | Number of passing first downs gained on play-action dropbacks. |
| `pa_big_time_throws` | Float64 | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `pa_spikes` | Float64 | Number of clock-stopping spikes on play-action dropbacks. |
| `pa_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `screen_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `no_screen_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `npa_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, on non-play-action dropbacks. |
| `screen_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `screen_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) on screen passes. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `no_screen_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage excluding screen passes. |
| `npa_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) excluding screen passes. |
| `screen_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks on screen passes, as charted by PFF. |
| `pa_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack on play-action dropbacks. |
| `screen_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack on screen passes. |
| `no_screen_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_interceptions` | Float64 | Number of passes intercepted on screen passes. |
| `no_screen_passing_snaps` | Float64 | Number of passing snaps played excluding screen passes. |
| `no_screen_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) excluding screen passes. |
| `screen_scrambles` | Float64 | Number of scrambles on screen passes. |
| `screen_grades_pass` | Float64 | PFF passing grade (0-100) on screen passes. |
| `npa_qb_rating` | Float64 | Traditional NFL passer rating on non-play-action dropbacks. |
| `no_screen_grades_pass` | Float64 | PFF passing grade (0-100) excluding screen passes. |
| `pa_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, on play-action dropbacks. |
| `screen_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `player_game_count` | Float64 | Number of games the player appeared in during the period covered. |
| `npa_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF on non-play-action dropbacks. |
| `eligible_season` | Float64 | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `npa_drops` | Float64 | Number of catchable passes dropped by receivers on non-play-action dropbacks. |
| `screen_yards` | Float64 | Passing yards gained on screen passes. |
| `no_screen_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers excluding screen passes. |
| `no_screen_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) excluding screen passes. |
| `npa_passing_snaps` | Float64 | Number of passing snaps played on non-play-action dropbacks. |
| `screen_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers on screen passes. |
| `screen_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks on screen passes. |
| `screen_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) on screen passes. |
| `screen_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) on screen passes. |
| `npa_spikes` | Float64 | Number of clock-stopping spikes on non-play-action dropbacks. |
| `screen_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw on screen passes, as charted by PFF. |
| `no_screen_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) excluding screen passes, as charted by PFF. |
| `screen_drops` | Float64 | Number of catchable passes dropped by receivers on screen passes. |
| `screen_ypa` | Float64 | Yards gained per pass attempt on screen passes. |
| `npa_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF on screen passes. |
| `npa_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw on non-play-action dropbacks, as charted by PFF. |
| `pa_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF on play-action dropbacks. |
| `pa_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks on play-action dropbacks, as charted by PFF. |
| `screen_avg_depth_of_target` | Float64 | Average depth of target in air yards on screen passes. |
| `pa_sacks` | Float64 | Number of sacks taken on play-action dropbacks. |
| `screen_passing_snaps` | Float64 | Number of passing snaps played on screen passes. |
| `no_screen_grades_run` | Float64 | PFF rushing grade for the player (0-100) excluding screen passes. |
| `no_screen_first_downs` | Float64 | Number of passing first downs gained excluding screen passes. |
| `pa_ypa` | Float64 | Yards gained per pass attempt on play-action dropbacks. |
| `npa_scrambles` | Float64 | Number of scrambles on non-play-action dropbacks. |
| `npa_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_screen_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on non-play-action dropbacks. |
| `screen_completions` | Float64 | Number of completed passes on screen passes. |
| `screen_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `npa_grades_run` | Float64 | PFF rushing grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_interceptions` | Float64 | Number of passes intercepted excluding screen passes. |
| `npa_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_sacks` | Float64 | Number of sacks taken excluding screen passes. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `no_screen_big_time_throws` | Float64 | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `npa_attempts` | Float64 | Number of pass attempts on non-play-action dropbacks. |
| `screen_qb_rating` | Float64 | Traditional NFL passer rating on screen passes. |
| `npa_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | Float64 | Number of pass attempts on play-action dropbacks. |
| `npa_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `no_screen_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, excluding screen passes. |
| `pa_yards` | Float64 | Passing yards gained on play-action dropbacks. |
| `npa_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `no_screen_scrambles` | Float64 | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `pa_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on play-action dropbacks, as charted by PFF. |
| `declined_penalties` | Float64 | Number of declined penalties committed by the player. |
| `pa_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) on play-action dropbacks. |
| `screen_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added on screen passes. |
| `no_screen_dropbacks` | Float64 | Number of dropbacks excluding screen passes. |
| `no_screen_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came excluding screen passes, expressed as a percentage. |
| `npa_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) on non-play-action dropbacks. |
| `npa_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added on play-action dropbacks. |
| `pa_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) on play-action dropbacks. |
| `no_screen_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays excluding screen passes, plays PFF charts as deserving of a turnover. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `pa_grades_pass_route` | Float64 | PFF receiving (route) grade for the player (0-100) on play-action dropbacks. |
| `npa_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks on non-play-action dropbacks. |
| `no_screen_avg_depth_of_target` | Float64 | Average depth of target in air yards excluding screen passes. |
| `pa_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on play-action dropbacks. |
| `no_screen_completion_percent` | Float64 | Percentage of pass attempts completed excluding screen passes. |
| `pa_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw on play-action dropbacks, as charted by PFF. |
| `pa_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came on non-play-action dropbacks, expressed as a percentage. |
| `npa_grades_pass` | Float64 | PFF passing grade (0-100) on non-play-action dropbacks. |
| `screen_grades_run` | Float64 | PFF rushing grade for the player (0-100) on screen passes. |
| `screen_first_downs` | Float64 | Number of passing first downs gained on screen passes. |
| `npa_completion_percent` | Float64 | Percentage of pass attempts completed on non-play-action dropbacks. |
| `no_screen_grades_run_block` | Float64 | PFF run-blocking grade for the player (0-100) excluding screen passes. |
| `no_screen_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw excluding screen passes, as charted by PFF. |
| `screen_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays on screen passes, plays PFF charts as deserving of a turnover. |
| `npa_avg_depth_of_target` | Float64 | Average depth of target in air yards on non-play-action dropbacks. |
| `npa_dropbacks` | Float64 | Number of dropbacks on non-play-action dropbacks. |
| `player` | String | Player's display name as PFF lists it. |
| `pa_drops` | Float64 | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `pa_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `screen_big_time_throws` | Float64 | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on screen passes, as charted by PFF. |
| `npa_touchdowns` | Float64 | Number of passing touchdowns thrown on non-play-action dropbacks. |
| `screen_attempts` | Float64 | Number of pass attempts on screen passes. |
| `screen_dropbacks` | Float64 | Number of dropbacks on screen passes. |
| `no_screen_yards` | Float64 | Passing yards gained excluding screen passes. |
| `pa_scrambles` | Float64 | Number of scrambles on play-action dropbacks. |
| `pa_completion_percent` | Float64 | Percentage of pass attempts completed on play-action dropbacks. |
| `pa_avg_depth_of_target` | Float64 | Average depth of target in air yards on play-action dropbacks. |
| `pa_interceptions` | Float64 | Number of passes intercepted on play-action dropbacks. |
| `no_screen_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF excluding screen passes. |
| `no_screen_spikes` | Float64 | Number of clock-stopping spikes excluding screen passes. |
| `pa_dropbacks` | Float64 | Number of dropbacks on play-action dropbacks. |
| `npa_big_time_throws` | Float64 | Number of big-time throws on non-play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `no_screen_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack excluding screen passes. |
| `pa_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks on play-action dropbacks. |
| `no_screen_attempts` | Float64 | Number of pass attempts excluding screen passes. |
| `pa_passing_snaps` | Float64 | Number of passing snaps played on play-action dropbacks. |
| `npa_completions` | Float64 | Number of completed passes on non-play-action dropbacks. |
| `screen_touchdowns` | Float64 | Number of passing touchdowns thrown on screen passes. |
| `npa_first_downs` | Float64 | Number of passing first downs gained on non-play-action dropbacks. |
| `no_screen_touchdowns` | Float64 | Number of passing touchdowns thrown excluding screen passes. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `npa_yards` | Float64 | Passing yards gained on non-play-action dropbacks. |
| `no_screen_ypa` | Float64 | Yards gained per pass attempt excluding screen passes. |
| `npa_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays on non-play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `npa_interceptions` | Float64 | Number of passes intercepted on non-play-action dropbacks. |
| `no_screen_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack excluding screen passes. |
| `screen_sacks` | Float64 | Number of sacks taken on screen passes. |
| `screen_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) on screen passes. |
| `screen_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) excluding screen passes. |
| `pa_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_pass_block` | Float64 | PFF pass-blocking grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_hands_drop` | Float64 | PFF hands (drop) grade for the player (0-100) excluding screen passes. |
| `pa_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) excluding screen passes. |
| `npa_grades_screen_block` | Float64 | PFF screen-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `npa_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) on screen passes. |
| `npa_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) on screen passes. |
| `screen_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) on screen passes. |
| `no_screen_grades_defense` | Float64 | PFF overall defense grade for the player (0-100) excluding screen passes. |
| `pa_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) on play-action dropbacks. |
| `no_screen_grades_defense_penalty` | Float64 | PFF defensive penalty grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_coverage_defense` | Float64 | PFF coverage grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) excluding screen passes. |
| `pa_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_pass_rush_defense` | String | PFF pass-rush grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_run_defense` | String | PFF run-defense grade for the player (0-100) on screen passes. |
| `pa_grades_run_defense` | String | PFF run-defense grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_overall_tackle` | Float64 | PFF overall tackling grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) excluding screen passes. |
| `npa_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_tackle` | String | PFF tackling grade for the player (0-100) on screen passes. |
| `no_screen_grades_tackle` | Float64 | PFF tackling grade for the player (0-100) excluding screen passes. |
| `npa_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_run_defense` | Float64 | PFF run-defense grade for the player (0-100) excluding screen passes. |
| `screen_grades_overall_tackle` | String | PFF overall tackling grade for the player (0-100) on screen passes. |
| `npa_grades_run_defense` | Float64 | PFF run-defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_pass_rush_defense` | Float64 | PFF pass-rush grade for the player (0-100) on screen passes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_passing_concept-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_passing_concept-example}

```python
pff_facet_passing_concept()
```

## pff_facet_coverage_scheme

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`

**Valid URL:** [https://premium.pff.com/api/v1/facet/defense/coverage_scheme](https://premium.pff.com/api/v1/facet/defense/coverage_scheme)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_coverage_scheme-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `man_touchdowns` | Float64 | Touchdowns allowed into the player's coverage when in man coverage. |
| `man_interceptions` | Float64 | Interceptions made in coverage when in man coverage. |
| `zone_snap_counts_coverage_percent` | Float64 | Share of the player's coverage snaps played when in zone coverage. |
| `man_avg_depth_of_target` | Float64 | Average depth of targets into the player's coverage, in yards downfield when in man coverage. |
| `man_dropped_ints` | Float64 | Interception chances PFF charted as dropped when in man coverage. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `zone_coverage_snaps_per_target` | Float64 | Coverage snaps played per target into the player's coverage when in zone coverage. |
| `man_yards_per_reception` | Float64 | Average yards allowed per reception when in man coverage. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `man_snap_counts_coverage` | Float64 | Coverage snaps played when in man coverage. |
| `man_tackles` | Float64 | Tackles made when in man coverage. |
| `zone_stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense when in zone coverage. |
| `zone_snap_counts_coverage` | Float64 | Coverage snaps played when in zone coverage. |
| `zone_yards` | Float64 | Receiving yards allowed when in zone coverage. |
| `man_coverage_snaps_per_target` | Float64 | Coverage snaps played per target into the player's coverage when in man coverage. |
| `zone_coverage_percent` | Float64 | Share of pass-play snaps spent in coverage when in zone coverage. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `zone_receptions` | Float64 | Receptions allowed into the player's coverage when in zone coverage. |
| `zone_forced_incompletion_rate` | Float64 | Share of targets into the player's coverage with a PFF-charted forced incompletion when in zone coverage. |
| `man_snap_counts_pass_play` | Float64 | Pass-play snaps when in man coverage. |
| `zone_yards_per_reception` | Float64 | Average yards allowed per reception when in zone coverage. |
| `man_longest` | Float64 | Longest completion allowed, in yards when in man coverage. |
| `man_assists` | Float64 | Assisted tackles when in man coverage. |
| `zone_yards_after_catch` | Float64 | Yards after the catch allowed when in zone coverage. |
| `man_stops` | Float64 | Stops, PFF's tackles that constitute a failed play for the offense when in man coverage. |
| `man_receptions` | Float64 | Receptions allowed into the player's coverage when in man coverage. |
| `zone_tackles` | Float64 | Tackles made when in zone coverage. |
| `man_coverage_percent` | Float64 | Share of pass-play snaps spent in coverage when in man coverage. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `zone_yards_per_coverage_snap` | Float64 | Receiving yards allowed per coverage snap when in zone coverage. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `man_catch_rate` | Float64 | Completion percentage allowed on targets into the player's coverage when in man coverage. |
| `man_grades_coverage_defense` | Float64 | PFF coverage grade when in man coverage, 0-100. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | Float64 | Yards after the catch allowed when in man coverage. |
| `man_pass_break_ups` | Float64 | Passes broken up when in man coverage. |
| `man_yards` | Float64 | Receiving yards allowed when in man coverage. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `man_targets` | Float64 | Targets into the player's coverage when in man coverage. |
| `man_missed_tackle_rate` | Float64 | Share of tackle attempts the player missed when in man coverage. |
| `zone_assists` | Float64 | Assisted tackles when in zone coverage. |
| `zone_snap_counts_pass_play` | Float64 | Pass-play snaps when in zone coverage. |
| `zone_missed_tackle_rate` | Float64 | Share of tackle attempts the player missed when in zone coverage. |
| `zone_touchdowns` | Float64 | Touchdowns allowed into the player's coverage when in zone coverage. |
| `zone_coverage_snaps_per_reception` | Float64 | Coverage snaps played per reception allowed when in zone coverage. |
| `man_coverage_snaps_per_reception` | Float64 | Coverage snaps played per reception allowed when in man coverage. |
| `zone_avg_depth_of_target` | Float64 | Average depth of targets into the player's coverage, in yards downfield when in zone coverage. |
| `man_snap_counts_coverage_percent` | Float64 | Share of the player's coverage snaps played when in man coverage. |
| `player` | String | Player's display name as PFF lists it. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `zone_forced_incompletes` | Float64 | Incompletions forced by the player's coverage, per PFF charting when in zone coverage. |
| `man_forced_incompletion_rate` | Float64 | Share of targets into the player's coverage with a PFF-charted forced incompletion when in man coverage. |
| `zone_targets` | Float64 | Targets into the player's coverage when in zone coverage. |
| `zone_longest` | Float64 | Longest completion allowed, in yards when in zone coverage. |
| `zone_dropped_ints` | Float64 | Interception chances PFF charted as dropped when in zone coverage. |
| `zone_interceptions` | Float64 | Interceptions made in coverage when in zone coverage. |
| `man_forced_incompletes` | Float64 | Incompletions forced by the player's coverage, per PFF charting when in man coverage. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `zone_missed_tackles` | Float64 | Missed tackles when in zone coverage. |
| `base_snap_counts_coverage` | Float64 | Coverage snaps from the facet's unsplit base row, covering all coverage schemes. |
| `man_missed_tackles` | Float64 | Missed tackles when in man coverage. |
| `zone_grades_coverage_defense` | Float64 | PFF coverage grade when in zone coverage, 0-100. |
| `man_qb_rating_against` | Float64 | NFL passer rating allowed on targets into the player's coverage when in man coverage. |
| `zone_pass_break_ups` | Float64 | Passes broken up when in zone coverage. |
| `zone_qb_rating_against` | Float64 | NFL passer rating allowed on targets into the player's coverage when in zone coverage. |
| `man_yards_per_coverage_snap` | Float64 | Receiving yards allowed per coverage snap when in man coverage. |
| `zone_catch_rate` | Float64 | Completion percentage allowed on targets into the player's coverage when in zone coverage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_coverage_scheme-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_coverage_scheme-example}

```python
pff_facet_coverage_scheme()
```
