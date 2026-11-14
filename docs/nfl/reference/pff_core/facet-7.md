# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: time–special

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: time–special — function reference in sdv-py, the SportsDataverse Python package.

## pff_facet_time_in_pockets

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`

**Valid URL:** [https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket](https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_time_in_pockets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `more_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts on dropbacks with time in pocket of 2.5 seconds or more, per PFF charting. |
| `more_grades_run` | Float64 | PFF rushing grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_ypa` | Float64 | Yards gained per pass attempt on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_passing_snaps` | Float64 | Number of passing snaps played on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `dropbacks` | Float64 | Number of dropbacks. |
| `less_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack on dropbacks with time in pocket under 2.5 seconds. |
| `less_first_downs` | Float64 | Number of passing first downs gained on dropbacks with time in pocket under 2.5 seconds. |
| `less_ypa` | Float64 | Yards gained per pass attempt on dropbacks with time in pocket under 2.5 seconds. |
| `draft_season` | Float64 | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `more_grades_pass_route` | String | PFF receiving (route) grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_pressure_to_sack_rate` | Float64 | Percentage of pressured dropbacks that ended in a sack on dropbacks with time in pocket under 2.5 seconds. |
| `less_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `more_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came on dropbacks with time in pocket of 2.5 seconds or more, expressed as a percentage. |
| `less_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added on dropbacks with time in pocket under 2.5 seconds. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `avg_ttt_scrambles` | Float64 | Average time in the pocket in seconds on dropbacks ending in a scramble. |
| `more_yards` | Float64 | Passing yards gained on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_attempts` | Float64 | Number of pass attempts on dropbacks with time in pocket under 2.5 seconds. |
| `less_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers on dropbacks with time in pocket under 2.5 seconds. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `more_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays on dropbacks with time in pocket of 2.5 seconds or more, plays PFF charts as deserving of a turnover. |
| `more_interceptions` | Float64 | Number of passes intercepted on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_sack_percent` | Float64 | Percentage of dropbacks that ended in a sack on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts on dropbacks with time in pocket of 2.5 seconds or more, per PFF charting. |
| `less_completions` | Float64 | Number of completed passes on dropbacks with time in pocket under 2.5 seconds. |
| `more_thrown_aways` | Float64 | Number of intentional throwaways on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_btt_rate` | Float64 | Big-time throws as a percentage of qualifying attempts on dropbacks with time in pocket under 2.5 seconds, per PFF charting. |
| `more_big_time_throws` | Float64 | Number of big-time throws on dropbacks with time in pocket of 2.5 seconds or more, per PFF's highest-value, highest-difficulty throw designation. |
| `less_qb_rating` | Float64 | Traditional NFL passer rating on dropbacks with time in pocket under 2.5 seconds. |
| `less_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `more_attempts` | Float64 | Number of pass attempts on dropbacks with time in pocket of 2.5 seconds or more. |
| `player_game_count` | Float64 | Number of games the player appeared in during the period covered. |
| `less_spikes` | Float64 | Number of clock-stopping spikes on dropbacks with time in pocket under 2.5 seconds. |
| `eligible_season` | Float64 | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `less_dropbacks_percent` | Float64 | Share of the player's total dropbacks that came on dropbacks with time in pocket under 2.5 seconds, expressed as a percentage. |
| `more_qb_rating` | Float64 | Traditional NFL passer rating on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_dropbacks` | Float64 | Number of dropbacks on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_avg_depth_of_target` | Float64 | Average depth of target in air yards on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_scrambles` | Float64 | Number of scrambles on dropbacks with time in pocket under 2.5 seconds. |
| `more_positive_epa_percent` | Float64 | Percentage of dropbacks with positive expected points added on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_sacks` | Float64 | Number of sacks taken on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_pass` | Float64 | PFF passing grade (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_drops` | Float64 | Number of catchable passes dropped by receivers on dropbacks with time in pocket under 2.5 seconds. |
| `more_sacks` | Float64 | Number of sacks taken on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_first_downs` | Float64 | Number of passing first downs gained on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_big_time_throws` | Float64 | Number of big-time throws on dropbacks with time in pocket under 2.5 seconds, per PFF's highest-value, highest-difficulty throw designation. |
| `avg_ttt_attempts` | Float64 | Average time from snap to release in seconds on dropbacks ending in a pass attempt. |
| `more_aimed_passes` | Float64 | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `less_accuracy_percent` | Float64 | Percentage of aimed passes charted as accurate by PFF on dropbacks with time in pocket under 2.5 seconds. |
| `more_scrambles` | Float64 | Number of scrambles on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks on dropbacks with time in pocket under 2.5 seconds. |
| `more_spikes` | Float64 | Number of clock-stopping spikes on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_pass_route` | String | PFF receiving (route) grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `avg_ttt_sacks` | Float64 | Average time from snap to sack in seconds on dropbacks ending in a sack. |
| `more_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `less_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_run` | Float64 | PFF rushing grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `less_touchdowns` | Float64 | Number of passing touchdowns thrown on dropbacks with time in pocket under 2.5 seconds. |
| `less_yards` | Float64 | Passing yards gained on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_run_block` | String | PFF run-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_hands_fumble` | Float64 | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_hit_as_threw` | Float64 | Number of attempts on which the passer was hit as he threw on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `more_epa` | Float64 | Total expected points added (EPA) on the player's dropbacks on dropbacks with time in pocket of 2.5 seconds or more. |
| `avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release. |
| `less_turnover_worthy_plays` | Float64 | Number of turnover-worthy plays on dropbacks with time in pocket under 2.5 seconds, plays PFF charts as deserving of a turnover. |
| `less_completion_percent` | Float64 | Percentage of pass attempts completed on dropbacks with time in pocket under 2.5 seconds. |
| `more_drops` | Float64 | Number of catchable passes dropped by receivers on dropbacks with time in pocket of 2.5 seconds or more. |
| `player` | String | Player's display name as PFF lists it. |
| `more_touchdowns` | Float64 | Number of passing touchdowns thrown on dropbacks with time in pocket of 2.5 seconds or more. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `more_drop_rate` | Float64 | Percentage of catchable passes dropped by receivers on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_twp_rate` | Float64 | Turnover-worthy plays as a percentage of qualifying attempts on dropbacks with time in pocket under 2.5 seconds, per PFF charting. |
| `more_grades_offense` | Float64 | PFF overall offense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_thrown_aways` | Float64 | Number of intentional throwaways on dropbacks with time in pocket under 2.5 seconds. |
| `less_avg_depth_of_target` | Float64 | Average depth of target in air yards on dropbacks with time in pocket under 2.5 seconds. |
| `less_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_offense_penalty` | Float64 | PFF offensive penalty grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_interceptions` | Float64 | Number of passes intercepted on dropbacks with time in pocket under 2.5 seconds. |
| `more_def_gen_pressures` | Float64 | Number of defense-generated pressures on the player's dropbacks on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `more_avg_time_to_throw` | Float64 | Average time to throw per dropback, in seconds from snap to release, on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_dropbacks` | Float64 | Number of dropbacks on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_pass` | Float64 | PFF passing grade (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `less_passing_snaps` | Float64 | Number of passing snaps played on dropbacks with time in pocket under 2.5 seconds. |
| `more_completions` | Float64 | Number of completed passes on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_run_block` | String | PFF run-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_bats` | Float64 | Number of pass attempts batted down at the line of scrimmage on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_completion_percent` | Float64 | Percentage of pass attempts completed on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_hands_drop` | String | PFF hands (drop) grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_hands_drop` | String | PFF hands (drop) grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_pass_block` | String | PFF pass-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_pass_block` | String | PFF pass-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_screen_block` | String | PFF screen-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_screen_block` | String | PFF screen-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_defense_penalty` | String | PFF defensive penalty grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_coverage_defense` | String | PFF coverage grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_defense_penalty` | String | PFF defensive penalty grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_defense` | String | PFF overall defense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_coverage_defense` | String | PFF coverage grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_defense` | String | PFF overall defense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_overall_tackle` | String | PFF overall tackling grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_pass_rush_defense` | String | PFF pass-rush grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_run_defense` | String | PFF run-defense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_tackle` | String | PFF tackling grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_pass_rush_defense` | String | PFF pass-rush grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_tackle` | String | PFF tackling grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_overall_tackle` | String | PFF overall tackling grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_run_defense` | String | PFF run-defense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_time_in_pockets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_time_in_pockets-example}

```python
pff_facet_time_in_pockets()
```

## pff_facet_receiving_concept

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/receiving/concept`

**Valid URL:** [https://premium.pff.com/api/v1/facet/receiving/concept](https://premium.pff.com/api/v1/facet/receiving/concept)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_receiving_concept-returns}

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_receiving_concept-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_receiving_concept-example}

```python
pff_facet_receiving_concept()
```

## pff_facet_special_teams_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/facet/special/summary`

**Valid URL:** [https://premium.pff.com/api/v1/facet/special/summary](https://premium.pff.com/api/v1/facet/special/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchiseId` | `franchise_id` |  |  | `Y` | PFF franchise (team) id; filters a report 'By Team'. |
| `gameId` | `game_id` |  |  | `Y` | PFF game id; filters a report 'By Game'. |
| `division` | `division` |  |  | `Y` | Division filter (NCAA). |

### Returns {#pff_facet_special_teams_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | Float64 | Assisted tackles credited to the player on special-teams plays. |
| `declined_penalties` | Float64 | Penalties committed by the player that were declined. |
| `draft_season` | Float64 | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | Float64 | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `grades_fgep_kicker` | Float64 | PFF field-goal and extra-point kicking grade, 0-100. |
| `grades_kickoff_kicker` | Float64 | PFF kickoff kicking grade, 0-100. |
| `grades_misc_st` | Float64 | PFF miscellaneous special-teams grade, 0-100. |
| `grades_special_teams_penalty` | Float64 | PFF special-teams penalty grade, 0-100. |
| `jersey_number` | String | Jersey number (string; zero-padded, e.g. "09"). |
| `missed_tackles` | Float64 | Missed tackles on special-teams plays. |
| `penalties` | Float64 | Penalties charged to the player over the covered span. |
| `player` | String | Player's display name as PFF lists it. |
| `player_game_count` | Float64 | Number of games the player appeared in over the covered span. |
| `player_id` | Float64 | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | String | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `snap_counts_field_goal` | Float64 | Snaps on the field-goal and extra-point unit. |
| `snap_counts_field_goal_blocking` | Float64 | Snaps on the field-goal and extra-point block unit. |
| `snap_counts_kickoff` | Float64 | Snaps on the kickoff coverage unit. |
| `snap_counts_kickoff_return` | Float64 | Snaps on the kickoff return unit. |
| `snap_counts_punt_coverage` | Float64 | Snaps on the punt coverage unit. |
| `snap_counts_punt_return` | Float64 | Snaps on the punt return unit. |
| `tackles` | Float64 | Tackles made by the player on special-teams plays. |
| `team` | String | Team abbreviation the player is credited to for the range. |
| `team_name` | String | Team abbreviation the player is credited to for the range. |
| `grades_fgep_defense` | Float64 | PFF grade on field-goal and extra-point defense, 0-100. |
| `grades_fgep_offense` | Float64 | PFF grade on the field-goal and extra-point protection unit, 0-100. |
| `grades_long_snap` | Float64 | PFF long-snapping grade, 0-100. |
| `grades_punter` | Float64 | PFF punting grade, 0-100. |
| `grades_kick_return` | Float64 | PFF kickoff-return grade, 0-100. |
| `grades_punt_return` | Float64 | PFF punt-return grade, 0-100. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_facet_special_teams_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_facet_special_teams_summary-example}

```python
pff_facet_special_teams_summary()
```
