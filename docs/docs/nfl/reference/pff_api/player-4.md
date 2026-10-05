---
title: "NFL — PFF Developer API (api.pff.com, API key) — Player: receiving"
sidebar_label: "Player: receiving"
sidebar_position: 11
description: "NFL — PFF Developer API (api.pff.com, API key) — Player: receiving — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Player: receiving

## pff_api_player_receiving_depth

Receiving by target depth for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/receiving/depth`

**Valid URL:** [https://api.pff.com/v1/player/receiving/depth?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/receiving/depth?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns {#pff_api_player_receiving_depth-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `left_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the middle of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the right third of the field. |
| `medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield). |
| `right_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the right third of the field, 0-100. |
| `left_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the right third of the field. |
| `short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield). |
| `deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield). |
| `center_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the middle of the field. |
| `right_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the middle of the field. |
| `behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage. |
| `right_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the right third of the field, 0-100. |
| `left_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield). |
| `deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `center_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the left third of the field. |
| `deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield). |
| `left_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the left third of the field. |
| `center_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the right third of the field. |
| `right_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the right third of the field, 0-100. |
| `short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield). |
| `behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage. |
| `right_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the left third of the field, 0-100. |
| `right_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the right third of the field. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `center_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the middle of the field, 0-100. |
| `deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield). |
| `center_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the left third of the field. |
| `center_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the middle of the field, 0-100. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the middle of the field. |
| `medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield). |
| `medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield). |
| `deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield). |
| `right_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the middle of the field. |
| `medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield). |
| `left_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the left third of the field. |
| `right_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the right third of the field, 0-100. |
| `center_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield). |
| `center_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield). |
| `center_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the middle of the field. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield). |
| `short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield). |
| `center_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the right third of the field, 0-100. |
| `center_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the middle of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield). |
| `deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield), 0-100. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield), 0-100. |
| `center_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the middle of the field. |
| `medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield). |
| `medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield). |
| `center_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `center_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield). |
| `behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage. |
| `left_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the left third of the field. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield). |
| `left_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage. |
| `right_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the right third of the field. |
| `medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield), 0-100. |
| `right_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage. |
| `right_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield). |
| `left_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield). |
| `center_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the left third of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield). |
| `short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield). |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield). |
| `behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage. |
| `center_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the middle of the field. |
| `center_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the middle of the field, 0-100. |
| `right_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield). |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the right third of the field. |
| `left_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the left third of the field, 0-100. |
| `center_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield). |
| `left_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the right third of the field. |
| `behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `left_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the right third of the field. |
| `behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage. |
| `right_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the left third of the field. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `center_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the left third of the field, 0-100. |
| `left_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage. |
| `right_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the middle of the field. |
| `deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage. |
| `left_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `right_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the right third of the field. |
| `right_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the left third of the field. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield). |
| `center_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield). |
| `left_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the left third of the field. |
| `short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield). |
| `left_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the left third of the field. |
| `center_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield). |
| `medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield). |
| `left_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the left third of the field, 0-100. |
| `left_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield). |
| `center_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield). |
| `left_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield). |
| `left_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the left third of the field. |
| `short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield). |
| `right_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the right third of the field, 0-100. |
| `behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `right_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the right third of the field. |
| `medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield). |
| `right_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage. |
| `center_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage. |
| `right_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield). |
| `left_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield). |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the left third of the field. |
| `behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage. |
| `short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield). |
| `medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield). |
| `behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage. |
| `left_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the middle of the field, 0-100. |
| `left_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the middle of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield). |
| `deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield). |
| `left_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the left third of the field. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all depths and directions. |
| `deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield). |
| `center_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield). |
| `short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield). |
| `left_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield). |
| `medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield). |
| `center_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the left third of the field. |
| `behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage. |
| `behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage. |
| `right_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the middle of the field. |
| `right_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the middle of the field, 0-100. |
| `left_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage. |
| `medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield). |
| `left_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the middle of the field. |
| `medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield). |
| `center_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the middle of the field. |
| `penalties` | numeric | Penalties charged to the player over the covered span. |
| `short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield). |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage. |
| `left_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the middle of the field. |
| `left_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield). |
| `short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield). |
| `deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield). |
| `center_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield). |
| `center_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield), 0-100. |
| `right_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the middle of the field. |
| `center_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the middle of the field, 0-100. |
| `deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield). |
| `left_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the right third of the field. |
| `center_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield). |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `center_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage. |
| `short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield). |
| `deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield). |
| `left_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the right third of the field. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield). |
| `left_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield). |
| `left_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the left third of the field, 0-100. |
| `right_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield). |
| `behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage. |
| `center_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the middle of the field. |
| `right_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the middle of the field. |
| `deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield). |
| `right_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage, 0-100. |
| `left_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the right third of the field. |
| `position` | character | PFF position code the player is listed at (e.g. QB, HB, FB, WR, TE, T, G, C, ED, DI, LB, CB, S, K, P). |
| `right_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the right third of the field. |
| `center_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the middle of the field, 0-100. |
| `right_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the left third of the field. |
| `short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield). |
| `center_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield). |
| `center_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the middle of the field. |
| `center_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield). |
| `left_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield), 0-100. |
| `left_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage, 0-100. |
| `right_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage. |
| `left_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the left third of the field. |
| `player` | character | Player's display name as PFF lists it. |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield). |
| `center_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the left third of the field, 0-100. |
| `right_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the right third of the field. |
| `medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield), 0-100. |
| `right_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the right third of the field. |
| `medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield). |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `center_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield). |
| `center_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield). |
| `center_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the left third of the field. |
| `left_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield). |
| `center_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the right third of the field. |
| `right_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the middle of the field. |
| `medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield). |
| `left_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the left third of the field, 0-100. |
| `behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage. |
| `left_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the right third of the field, 0-100. |
| `right_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the right third of the field, 0-100. |
| `left_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield). |
| `center_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage. |
| `behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage. |
| `left_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the middle of the field, 0-100. |
| `deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield). |
| `center_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the middle of the field. |
| `short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield). |
| `right_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage. |
| `short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield). |
| `left_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the left third of the field, 0-100. |
| `medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield). |
| `medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield). |
| `behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage. |
| `left_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield). |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `left_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the right third of the field. |
| `short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield). |
| `right_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield). |
| `deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield). |
| `center_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the middle of the field. |
| `behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage. |
| `medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield). |
| `left_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the left third of the field. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_receiving_depth-example}

```python
pff_api_player_receiving_depth(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._
