# Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY)

> Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.golf` — 21 endpoints.

> **Note:** Runtime only: each call sends one GraphQL query to `orchestrator.pgatour.com/graphql`. sdv-py publishes no release assets from this API and ships no bulk or backfill tooling for it.

> **Note:** The request body is `{operationName, query, variables}`; the query document ships in `sportsdataverse/golf/pgatour_queries/`. Every request also carries an `x-api-key` header (left out of the example request): the public key pgatour.com's own frontend sends, read from the site on first use. Set the `PGA_API_KEY` environment variable to supply a key yourself.

> **Note:** Tour codes: `R` PGA TOUR, `S` PGA TOUR Champions, `H` Korn Ferry Tour. Tournament ids are the tour code, the season and the event number (`R2026014` is the 2026 Masters): read them from `pgatour_schedule`.

## Player

| Function | Summary |
|---|---|
| [pgatour_player](pgatour/player.md#pgatour_player) | One player's profile: name, country, world ranking, height, weight, birth date and place, year turned pro, school, residence. |
| [pgatour_player_directory](pgatour/player.md#pgatour_player_directory) | A tour's player directory: one row per player (id, name, country, age, college, year turned pro). |
| [pgatour_player_results](pgatour/player.md#pgatour_player_results) | A player's career results on a tour: one row per tournament edition played (finish, score to par, total, points, money, round scores). |

## Other

| Function | Summary |
|---|---|
| [pgatour_schedule](pgatour/other.md#pgatour_schedule) | A tour's season schedule: one row per tournament (id, dates, course, purse, champion, points on offer), completed events first. |
| [pgatour_leaderboard](pgatour/other.md#pgatour_leaderboard) | One tournament's leaderboard: position, total to par, strokes, holes completed, today's score, round scores and projected FedExCup rank per player. |
| [pgatour_scorecard](pgatour/other.md#pgatour_scorecard) | One player's scorecard for a tournament: one row per hole per round (par, yardage, strokes, birdie / bogey status, running score to par). |
| [pgatour_tee_times](pgatour/other.md#pgatour_tee_times) | A tournament's tee times: one row per player per group per round (tee time in UTC, starting tee, course). |
| [pgatour_stat_catalog](pgatour/other.md#pgatour_stat_catalog) | Every stat a tour publishes: category, sub-category, stat id and title. The stat ids are what pgatour_stat_details takes. |
| [pgatour_stat_details](pgatour/other.md#pgatour_stat_details) | One stat's season table, long form: one row per player per stat column (rank, column name, value as displayed). |
| [pgatour_tour_cup](pgatour/other.md#pgatour_tour_cup) | A season-long points race (the FedExCup by default): one row per player with official and projected rank and points. |
| [pgatour_shot_details](pgatour/other.md#pgatour_shot_details) | ShotLink shot tracking for one player's round: one row per stroke with description, distance, lie and start / end coordinates. |
| [pgatour_schedule_years](pgatour/other.md#pgatour_schedule_years) | The seasons a tour has a published schedule for: one row per season, the current one flagged. The seasons are what pgatour_schedule(year=) takes. |
| [pgatour_tournaments](pgatour/other.md#pgatour_tournaments) | Tournament headers: one row per tournament id (name, dates, status, current round, format, city, time zone). |
| [pgatour_past_champions](pgatour/other.md#pgatour_past_champions) | Every past champion of a tournament: one row per edition (season, winner, total and score to par). |
| [pgatour_hole_by_hole](pgatour/other.md#pgatour_hole_by_hole) | Hole-by-hole scores of one round for the whole field: one row per player per hole (par, yardage, strokes, birdie / bogey status). |
| [pgatour_leaderboard_stats](pgatour/other.md#pgatour_leaderboard_stats) | A tournament's leaderboard stat table, long form: one row per player per stat (win / top 10 / make-cut chances by default). |
| [pgatour_field](pgatour/other.md#pgatour_field) | A tournament's field: one row per entered player, then the alternates (status, world ranking, how they qualified). |
| [pgatour_past_results](pgatour/other.md#pgatour_past_results) | One edition's final leaderboard: one row per player (position, total, score to par, round scores). |
| [pgatour_course_stats](pgatour/other.md#pgatour_course_stats) | Hole difficulty for every course of a tournament: one row per hole per round (scoring average, rank, eagles to double bogeys). |
| [pgatour_hole_details](pgatour/other.md#pgatour_hole_details) | One hole of one course over a tournament: one row (par, yardage, difficulty rank, counts and shares of eagles to double bogeys). |
| [pgatour_scorecard_stats](pgatour/other.md#pgatour_scorecard_stats) | One player's stat lines for a tournament: one row per stat per round (performance, scoring and strokes gained, with the season-to-date value). |
