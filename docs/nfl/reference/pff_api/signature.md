# NFL — PFF Developer API (api.pff.com, API key) — Signature

> NFL — PFF Developer API (api.pff.com, API key) — Signature — function reference in sdv-py, the SportsDataverse Python package.

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
| `more_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on dropbacks with time in pocket of 2.5 seconds or more, per PFF charting. |
| `more_grades_run` | numeric | PFF rushing grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_ypa` | numeric | Yards gained per pass attempt on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_passing_snaps` | numeric | Number of passing snaps played on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `dropbacks` | numeric | Number of dropbacks. |
| `less_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on dropbacks with time in pocket under 2.5 seconds. |
| `less_first_downs` | numeric | Number of passing first downs gained on dropbacks with time in pocket under 2.5 seconds. |
| `less_ypa` | numeric | Yards gained per pass attempt on dropbacks with time in pocket under 2.5 seconds. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `more_grades_pass_route` | character | PFF receiving (route) grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on dropbacks with time in pocket under 2.5 seconds. |
| `less_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `more_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on dropbacks with time in pocket of 2.5 seconds or more, expressed as a percentage. |
| `less_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on dropbacks with time in pocket under 2.5 seconds. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `avg_ttt_scrambles` | numeric | Average time in the pocket in seconds on dropbacks ending in a scramble. |
| `more_yards` | numeric | Passing yards gained on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_attempts` | numeric | Number of pass attempts on dropbacks with time in pocket under 2.5 seconds. |
| `less_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on dropbacks with time in pocket under 2.5 seconds. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `more_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on dropbacks with time in pocket of 2.5 seconds or more, plays PFF charts as deserving of a turnover. |
| `more_interceptions` | numeric | Number of passes intercepted on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on dropbacks with time in pocket of 2.5 seconds or more, per PFF charting. |
| `less_completions` | numeric | Number of completed passes on dropbacks with time in pocket under 2.5 seconds. |
| `more_thrown_aways` | numeric | Number of intentional throwaways on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on dropbacks with time in pocket under 2.5 seconds, per PFF charting. |
| `more_big_time_throws` | numeric | Number of big-time throws on dropbacks with time in pocket of 2.5 seconds or more, per PFF's highest-value, highest-difficulty throw designation. |
| `less_qb_rating` | numeric | Traditional NFL passer rating on dropbacks with time in pocket under 2.5 seconds. |
| `less_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `more_attempts` | numeric | Number of pass attempts on dropbacks with time in pocket of 2.5 seconds or more. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `less_spikes` | numeric | Number of clock-stopping spikes on dropbacks with time in pocket under 2.5 seconds. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `less_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on dropbacks with time in pocket under 2.5 seconds, expressed as a percentage. |
| `more_qb_rating` | numeric | Traditional NFL passer rating on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_dropbacks` | numeric | Number of dropbacks on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_avg_depth_of_target` | numeric | Average depth of target in air yards on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_scrambles` | numeric | Number of scrambles on dropbacks with time in pocket under 2.5 seconds. |
| `more_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_sacks` | numeric | Number of sacks taken on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_pass` | numeric | PFF passing grade (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_drops` | numeric | Number of catchable passes dropped by receivers on dropbacks with time in pocket under 2.5 seconds. |
| `more_sacks` | numeric | Number of sacks taken on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_first_downs` | numeric | Number of passing first downs gained on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_big_time_throws` | numeric | Number of big-time throws on dropbacks with time in pocket under 2.5 seconds, per PFF's highest-value, highest-difficulty throw designation. |
| `avg_ttt_attempts` | numeric | Average time from snap to release in seconds on dropbacks ending in a pass attempt. |
| `more_aimed_passes` | numeric | Number of aimed passes (attempts excluding throwaways, spikes, batted passes and throws made while hit) on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `less_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on dropbacks with time in pocket under 2.5 seconds. |
| `more_scrambles` | numeric | Number of scrambles on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on dropbacks with time in pocket under 2.5 seconds. |
| `more_spikes` | numeric | Number of clock-stopping spikes on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_pass_route` | character | PFF receiving (route) grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `avg_ttt_sacks` | numeric | Average time from snap to sack in seconds on dropbacks ending in a sack. |
| `more_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `less_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_run` | numeric | PFF rushing grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `less_touchdowns` | numeric | Number of passing touchdowns thrown on dropbacks with time in pocket under 2.5 seconds. |
| `less_yards` | numeric | Passing yards gained on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_run_block` | character | PFF run-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `more_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on dropbacks with time in pocket of 2.5 seconds or more. |
| `avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release. |
| `less_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on dropbacks with time in pocket under 2.5 seconds, plays PFF charts as deserving of a turnover. |
| `less_completion_percent` | numeric | Percentage of pass attempts completed on dropbacks with time in pocket under 2.5 seconds. |
| `more_drops` | numeric | Number of catchable passes dropped by receivers on dropbacks with time in pocket of 2.5 seconds or more. |
| `player` | character | Player's display name as PFF lists it. |
| `more_touchdowns` | numeric | Number of passing touchdowns thrown on dropbacks with time in pocket of 2.5 seconds or more. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `more_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on dropbacks with time in pocket under 2.5 seconds, per PFF charting. |
| `more_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_thrown_aways` | numeric | Number of intentional throwaways on dropbacks with time in pocket under 2.5 seconds. |
| `less_avg_depth_of_target` | numeric | Average depth of target in air yards on dropbacks with time in pocket under 2.5 seconds. |
| `less_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_interceptions` | numeric | Number of passes intercepted on dropbacks with time in pocket under 2.5 seconds. |
| `more_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `more_avg_time_to_throw` | numeric | Average time to throw per dropback, in seconds from snap to release, on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_dropbacks` | numeric | Number of dropbacks on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_pass` | numeric | PFF passing grade (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `less_passing_snaps` | numeric | Number of passing snaps played on dropbacks with time in pocket under 2.5 seconds. |
| `more_completions` | numeric | Number of completed passes on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_run_block` | character | PFF run-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_completion_percent` | numeric | Percentage of pass attempts completed on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_hands_drop` | character | PFF hands (drop) grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_hands_drop` | character | PFF hands (drop) grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_pass_block` | character | PFF pass-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_pass_block` | character | PFF pass-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_screen_block` | character | PFF screen-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_screen_block` | character | PFF screen-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_coverage_defense` | character | PFF coverage grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_defense` | character | PFF overall defense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_coverage_defense` | character | PFF coverage grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_defense` | character | PFF overall defense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_tackle` | character | PFF tackling grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_tackle` | character | PFF tackling grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_signature_passing_time_in_pocket-example}

```python
pff_api_signature_passing_time_in_pocket(league='nfl', season='2022', week='1')
```

_Last validated n/a._

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
| `attempts` | numeric | Pass plays the team's offensive line blocked on over the covered span, as counted by PFF (equal to pass_snaps in the captured rows). |
| `franchise_id` | numeric | PFF franchise (team) id of the team whose offensive line the row describes (integer join key). |
| `hits_allowed` | numeric | Quarterback hits allowed. |
| `hurries_allowed` | numeric | Quarterback hurries allowed. |
| `pass_snaps` | numeric | Pass-play snaps. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries). |
| `sacks_allowed` | numeric | Sacks allowed by the team's offensive line over the covered span. |
| `season_id` | numeric | Season (year) the row covers (e.g. 2022). |
| `team_name` | character | Abbreviation of the team whose offensive line the row describes (e.g. "ATL"). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_signature_pass_blocking_efficiency_line-example}

```python
pff_api_signature_pass_blocking_efficiency_line(league='nfl', season='2022', week='1')
```

_Last validated n/a._

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
| `lhs_sacks` | numeric | Sacks recorded when rushing from the left side. |
| `rhs_hits` | numeric | Quarterback hits recorded when rushing from the right side. |
| `rhs_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks when rushing from the right side. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `pass_snaps` | numeric | Pass-play snaps. |
| `lhs_hurries` | numeric | Quarterback hurries recorded when rushing from the left side. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `tackles` | numeric | Tackles made by the player, as charted by PFF. |
| `rhs_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) when rushing from the right side. |
| `rhs_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer when rushing from the right side. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `lhs_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer when rushing from the left side. |
| `lhs_pass_rush_snaps` | numeric | Pass-rush snaps played when rushing from the left side. |
| `sacks` | numeric | Sacks recorded by the pass rusher. |
| `lhs_assists` | numeric | Assisted tackles when rushing from the left side. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `lhs_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) when rushing from the left side. |
| `pass_rush_snaps` | numeric | Pass-rush snaps played. |
| `hurries` | numeric | Quarterback hurries recorded. |
| `rhs_pass_rush_snaps` | numeric | Pass-rush snaps played when rushing from the right side. |
| `lhs_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks when rushing from the left side. |
| `hits` | numeric | Quarterback hits recorded by the pass rusher. |
| `lhs_hits` | numeric | Quarterback hits recorded when rushing from the left side. |
| `rhs_tackles` | numeric | Tackles made when rushing from the right side. |
| `lhs_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when rushing from the left side. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `rhs_misses` | numeric | Missed tackles when rushing from the right side. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `pressures` | numeric | Total pressures generated (sacks, hits, and hurries). |
| `misses` | numeric | Missed tackles. |
| `player` | character | Player's display name as PFF lists it. |
| `rhs_sacks` | numeric | Sacks recorded when rushing from the right side. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer. |
| `rhs_hurries` | numeric | Quarterback hurries recorded when rushing from the right side. |
| `rhs_assists` | numeric | Assisted tackles when rushing from the right side. |
| `rhs_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when rushing from the right side. |
| `assists` | numeric | Assisted tackles credited to the player. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `lhs_tackles` | numeric | Tackles made when rushing from the left side. |
| `lhs_misses` | numeric | Missed tackles when rushing from the left side. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_signature_defense_outside_pass_rush-example}

```python
pff_api_signature_defense_outside_pass_rush(league='nfl', season='2022', week='1')
```

_Last validated n/a._

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
| `coverage_snaps` | numeric | Coverage snaps played while covering the slot. |
| `coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed while covering the slot. |
| `coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage while covering the slot. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `interceptions` | numeric | Interceptions made by the player while covering the slot. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `player` | character | Player's display name as PFF lists it. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage while covering the slot. |
| `receptions` | numeric | Receptions allowed in the player's coverage while covering the slot. |
| `targets` | numeric | Passes thrown into the player's coverage (targets allowed) while covering the slot. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage while covering the slot. |
| `yards` | numeric | Receiving yards allowed in the player's coverage while covering the slot. |
| `yards_after_catch` | numeric | Yards after the catch allowed in the player's coverage while covering the slot. |
| `yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap while covering the slot. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_signature_defense_slot_coverage-example}

```python
pff_api_signature_defense_slot_coverage(league='nfl', season='2022', week='1')
```

_Last validated n/a._
