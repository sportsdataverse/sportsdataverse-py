# NFL — PFF Developer API (api.pff.com, API key) — Player: passing–rushing

> NFL — PFF Developer API (api.pff.com, API key) — Player: passing–rushing — function reference in sdv-py, the SportsDataverse Python package.

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

### Raises {#pff_api_player_passing_pressure-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_passing_pressure-example}

```python
pff_api_player_passing_pressure(league='nfl', player_id=28022, season=2022)
```

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `attempts` | Int64 | Rushing attempts in this direction. |
| `direction` | String | Run direction / gap of the split, PFF's own vocabulary (e.g. "LT", "LG", "ML", "RG", "RE", "QBSc"). |
| `explosive` | Int64 | Runs in this direction PFF designates as explosive. |
| `first_downs` | Int64 | Rushing first downs gained in this direction. |
| `franchise_id` | Int64 | PFF franchise id of the runner's team. |
| `fumbles` | Int64 | Fumbles on runs in this direction. |
| `long` | Int64 | Longest run in this direction, in yards. |
| `missed_tackles` | Int64 | Missed tackles forced on runs in this direction. |
| `player_id` | Int64 | PFF player id of the runner. |
| `team_name` | String | Abbreviation of the runner's team (e.g. "BUF"). |
| `touchdowns` | Int64 | Rushing touchdowns in this direction. |
| `yards` | Int64 | Rushing yards gained in this direction. |
| `yards_after_contact` | Int64 | Yards after contact on runs in this direction. |
| `yco_attempt` | Float64 | Average yards after contact per attempt in this direction. |
| `ypa` | Float64 | Yards per rushing attempt in this direction. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_rushing_direction-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_rushing_direction-example}

```python
pff_api_player_rushing_direction(league='nfl', player_id=28022, season=2022)
```

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `targets` | Int64 | Passes thrown to the ball carrier (targets). |
| `grades_offense` | Float64 | PFF overall offense grade (0-100). |
| `game_id` | Int64 | PFF game id of the game (integer join key). |
| `yards_after_contact` | Int64 | Yards after contact. |
| `explosive` | Int64 | Runs PFF designates as explosive. |
| `week` | Int64 | Week number of the game, as PFF numbers weeks. |
| `elu_rush_mtf` | Int64 | Missed tackles forced as a rusher, an input to PFF's elusive rating. |
| `breakaway_attempts` | Int64 | Runs of 15 or more yards, PFF's breakaway designation. |
| `status` | String | "S" when the player started the game; PFF owns the value set. |
| `designed_yards` | Int64 | Rushing yards gained on designed runs, excluding scrambles. |
| `yprr` | Float64 | Yards per route run. |
| `breakaway_percent` | Float64 | Share of rushing yards gained on breakaway runs of 15 or more yards. |
| `fumbles` | Int64 | Fumbles by the ball carrier. |
| `first_downs` | Int64 | Rushing first downs. |
| `elusive_rating` | Float64 | PFF elusive rating. |
| `jersey_number` | String | Jersey number the player wore in the game (string; zero-padded, e.g. "09"). |
| `breakaway_yards` | Int64 | Breakaway (long-run) yards. |
| `away_team_name` | String | Abbreviation of the away team (e.g. "LV"). |
| `total_touches` | Int64 | Combined carries and receptions. |
| `scramble_yards` | Int64 | Rushing yards gained on scrambles. |
| `yco_attempt` | Float64 | Average yards after contact per rushing attempt. |
| `yards` | Int64 | Total rushing yards gained. |
| `player_franchise_id` | Int64 | PFF franchise id of the team the player played for in the game. |
| `grades_run_block` | Float64 | PFF run-blocking grade, 0-100. |
| `receptions` | Int64 | Passes caught by the ball carrier. |
| `zone_attempts` | Int64 | Rushing attempts on zone-scheme runs. |
| `scrambles` | Int64 | Quarterback scrambles. |
| `grades_run` | Float64 | PFF rushing grade (0-100). |
| `penalties` | Int64 | Penalties charged to the player over the covered span. |
| `attempts` | Int64 | Rushing attempts (carries) by the runner. |
| `elu_yco` | Int64 | Yards-after-contact component used in PFF's elusive rating. |
| `elu_recv_mtf` | Int64 | Missed tackles forced as a receiver, an input to PFF's elusive rating. |
| `declined_penalties` | Int64 | Penalties committed by the player that were declined. |
| `ypa` | Float64 | Average yards per rushing attempt. |
| `drops` | Int64 | Passes dropped by the ball carrier. |
| `position` | String | PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `grades_hands_fumble` | Float64 | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | Int64 | Longest run in yards. |
| `away_franchise_id` | Int64 | PFF franchise id of the away team. |
| `count_of_yards` | Int64 | Rushing attempts counted toward the yardage figures (equal to attempts in the captured rows). |
| `routes` | Int64 | Pass routes run by the player. |
| `rec_yards` | Int64 | Receiving yards gained by the ball carrier (the rushing report also carries his receiving line). |
| `home_franchise_id` | Int64 | PFF franchise id of the home team. |
| `gap_attempts` | Int64 | Rushing attempts on gap-scheme runs. |
| `run_plays` | Int64 | Run-play snaps. |
| `home_team_name` | String | Abbreviation of the home team (e.g. "NE"). |
| `avoided_tackles` | Int64 | Missed tackles forced. |
| `grades_offense_penalty` | Float64 | PFF offensive penalty grade, 0-100. |
| `player_id` | Int64 | PFF player id (integer join key) of the player the report is about. |
| `touchdowns` | Int64 | Rushing touchdowns. |
| `game_away_franchise_id` | Int64 | Repeats away_franchise_id from the row's nested game object: PFF franchise id of the away team. |
| `game_away_team_name` | String | Repeats away_team_name from the row's nested game object: Abbreviation of the away team (e.g. "LV"). |
| `game_game_id` | Int64 | Repeats game_id from the row's nested game object: PFF game id of the game (integer join key). |
| `game_home_franchise_id` | Int64 | Repeats home_franchise_id from the row's nested game object: PFF franchise id of the home team. |
| `game_home_team_name` | String | Repeats home_team_name from the row's nested game object: Abbreviation of the home team (e.g. "NE"). |
| `game_player_franchise_id` | Int64 | Repeats player_franchise_id from the row's nested game object: PFF franchise id of the team the player played for in the game. |
| `game_position` | String | Repeats position from the row's nested game object: PFF position code the player was charted at in the game (e.g. QB, HB, WR, T, LB, K). |
| `game_status` | String | Repeats status from the row's nested game object: "S" when the player started the game; PFF owns the value set. |
| `game_week` | Int64 | Repeats week from the row's nested game object: Week number of the game, as PFF numbers weeks. |
| `league_id` | Int64 | PFF league id (1 = NFL, 2 = NCAA), filled from the report's subject. |
| `season` | Int64 | Season (starting year) of the report, filled from the report's subject. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_player_rushing_summary-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_player_rushing_summary-example}

```python
pff_api_player_rushing_summary(league='nfl', player_id=28022, season=2022)
```
