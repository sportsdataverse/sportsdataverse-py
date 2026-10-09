# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: coverage

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Facet: coverage — function reference in sdv-py, the SportsDataverse Python package.

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
| `man_touchdowns` | numeric | Touchdowns allowed into the player's coverage when in man coverage. |
| `man_interceptions` | numeric | Interceptions made in coverage when in man coverage. |
| `zone_snap_counts_coverage_percent` | numeric | Share of the player's coverage snaps played when in zone coverage. |
| `man_avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield when in man coverage. |
| `man_dropped_ints` | numeric | Interception chances PFF charted as dropped when in man coverage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `zone_coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage when in zone coverage. |
| `man_yards_per_reception` | numeric | Average yards allowed per reception when in man coverage. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `man_snap_counts_coverage` | numeric | Coverage snaps played when in man coverage. |
| `man_tackles` | numeric | Tackles made when in man coverage. |
| `zone_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when in zone coverage. |
| `zone_snap_counts_coverage` | numeric | Coverage snaps played when in zone coverage. |
| `zone_yards` | numeric | Receiving yards allowed when in zone coverage. |
| `man_coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage when in man coverage. |
| `zone_coverage_percent` | numeric | Share of pass-play snaps spent in coverage when in zone coverage. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `zone_receptions` | numeric | Receptions allowed into the player's coverage when in zone coverage. |
| `zone_forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion when in zone coverage. |
| `man_snap_counts_pass_play` | numeric | Pass-play snaps when in man coverage. |
| `zone_yards_per_reception` | numeric | Average yards allowed per reception when in zone coverage. |
| `man_longest` | numeric | Longest completion allowed, in yards when in man coverage. |
| `man_assists` | numeric | Assisted tackles when in man coverage. |
| `zone_yards_after_catch` | numeric | Yards after the catch allowed when in zone coverage. |
| `man_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when in man coverage. |
| `man_receptions` | numeric | Receptions allowed into the player's coverage when in man coverage. |
| `zone_tackles` | numeric | Tackles made when in zone coverage. |
| `man_coverage_percent` | numeric | Share of pass-play snaps spent in coverage when in man coverage. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `zone_yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap when in zone coverage. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `man_catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage when in man coverage. |
| `man_grades_coverage_defense` | numeric | PFF coverage grade when in man coverage, 0-100. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | numeric | Yards after the catch allowed when in man coverage. |
| `man_pass_break_ups` | numeric | Passes broken up when in man coverage. |
| `man_yards` | numeric | Receiving yards allowed when in man coverage. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `man_targets` | numeric | Targets into the player's coverage when in man coverage. |
| `man_missed_tackle_rate` | numeric | Share of tackle attempts the player missed when in man coverage. |
| `zone_assists` | numeric | Assisted tackles when in zone coverage. |
| `zone_snap_counts_pass_play` | numeric | Pass-play snaps when in zone coverage. |
| `zone_missed_tackle_rate` | numeric | Share of tackle attempts the player missed when in zone coverage. |
| `zone_touchdowns` | numeric | Touchdowns allowed into the player's coverage when in zone coverage. |
| `zone_coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed when in zone coverage. |
| `man_coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed when in man coverage. |
| `zone_avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield when in zone coverage. |
| `man_snap_counts_coverage_percent` | numeric | Share of the player's coverage snaps played when in man coverage. |
| `player` | character | Player's display name as PFF lists it. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `zone_forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting when in zone coverage. |
| `man_forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion when in man coverage. |
| `zone_targets` | numeric | Targets into the player's coverage when in zone coverage. |
| `zone_longest` | numeric | Longest completion allowed, in yards when in zone coverage. |
| `zone_dropped_ints` | numeric | Interception chances PFF charted as dropped when in zone coverage. |
| `zone_interceptions` | numeric | Interceptions made in coverage when in zone coverage. |
| `man_forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting when in man coverage. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `zone_missed_tackles` | numeric | Missed tackles when in zone coverage. |
| `base_snap_counts_coverage` | numeric | Coverage snaps from the facet's unsplit base row, covering all coverage schemes. |
| `man_missed_tackles` | numeric | Missed tackles when in man coverage. |
| `zone_grades_coverage_defense` | numeric | PFF coverage grade when in zone coverage, 0-100. |
| `man_qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage when in man coverage. |
| `zone_pass_break_ups` | numeric | Passes broken up when in zone coverage. |
| `zone_qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage when in zone coverage. |
| `man_yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap when in man coverage. |
| `zone_catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage when in zone coverage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_facet_coverage_scheme-example}

```python
pff_facet_coverage_scheme()
```

_Last validated n/a._
