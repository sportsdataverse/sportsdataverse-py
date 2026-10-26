---
title: "NFL — PFF Developer API (api.pff.com, API key) — Signature"
sidebar_label: "Signature"
sidebar_position: 13
description: "NFL — PFF Developer API (api.pff.com, API key) — Signature — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Signature

## pff_api_signature_passing_time_in_pocket

Signature stat: time in pocket

**Endpoint URL:** `GET https://api.pff.com/v1/facet/signature/passing/time_in_pocket`

**Valid URL:** [https://api.pff.com/v1/facet/signature/passing/time_in_pocket?league=nfl&season=2022&week=1](https://api.pff.com/v1/facet/signature/passing/time_in_pocket?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, and the SECOND positional argument of the four signature commands. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of the four signature commands. |

### Returns {#pff_api_signature_passing_time_in_pocket-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_signature_passing_time_in_pocket-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_signature_passing_time_in_pocket-example}

```python
pff_api_signature_passing_time_in_pocket(league='nfl', season='2022', week='1')
```

## pff_api_signature_pass_blocking_efficiency_line

Signature stat: pass-blocking efficiency, by line

**Endpoint URL:** `GET https://api.pff.com/v1/facet/signature/pass-blocking/efficiency/line`

**Valid URL:** [https://api.pff.com/v1/facet/signature/pass-blocking/efficiency/line?league=nfl&season=2022&week=1](https://api.pff.com/v1/facet/signature/pass-blocking/efficiency/line?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, and the SECOND positional argument of the four signature commands. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of the four signature commands. |

### Returns {#pff_api_signature_pass_blocking_efficiency_line-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_signature_pass_blocking_efficiency_line-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_signature_pass_blocking_efficiency_line-example}

```python
pff_api_signature_pass_blocking_efficiency_line(league='nfl', season='2022', week='1')
```

## pff_api_signature_defense_outside_pass_rush

Signature stat: outside pass rush

**Endpoint URL:** `GET https://api.pff.com/v1/facet/signature/defense/outside_pass_rush`

**Valid URL:** [https://api.pff.com/v1/facet/signature/defense/outside_pass_rush?league=nfl&season=2022&week=1](https://api.pff.com/v1/facet/signature/defense/outside_pass_rush?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, and the SECOND positional argument of the four signature commands. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of the four signature commands. |

### Returns {#pff_api_signature_defense_outside_pass_rush-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_signature_defense_outside_pass_rush-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_signature_defense_outside_pass_rush-example}

```python
pff_api_signature_defense_outside_pass_rush(league='nfl', season='2022', week='1')
```

## pff_api_signature_defense_slot_coverage

Signature stat: slot coverage

**Endpoint URL:** `GET https://api.pff.com/v1/facet/signature/defense/slot_coverage`

**Valid URL:** [https://api.pff.com/v1/facet/signature/defense/slot_coverage?league=nfl&season=2022&week=1](https://api.pff.com/v1/facet/signature/defense/slot_coverage?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, and the SECOND positional argument of the four signature commands. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of the four signature commands. |

### Returns {#pff_api_signature_defense_slot_coverage-returns}

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_signature_defense_slot_coverage-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_signature_defense_slot_coverage-example}

```python
pff_api_signature_defense_slot_coverage(league='nfl', season='2022', week='1')
```
