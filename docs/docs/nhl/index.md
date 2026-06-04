# sportsdataverse.nhl package

## Submodules

## sportsdataverse.nhl.nhl_api module

sportsdataverse.nhl.nhl_api — **DEPRECATED**.

These functions target `statsapi.web.nhl.com/api/v1/`, which the NHL
**retired in September 2023**. Calls return HTTP 404 in production.

Migration: use [`sportsdataverse.nhl.nhl_api_web`](#module-sportsdataverse.nhl.nhl_api_web) instead.

Deprecated here            | Replacement in `nhl_api_web` |
<br/>

```
|----------------------------|
```

————————————|
| [`nhl_api_pbp()`](#sportsdataverse.nhl.nhl_api.nhl_api_pbp)        | `nhl_web_pbp()`                |
| [`nhl_api_schedule()`](#sportsdataverse.nhl.nhl_api.nhl_api_schedule)   | `nhl_web_schedule()`           |

The endpoint paths, return shapes, and game-id semantics all differ between
the old Stats API and the new `api-web.nhle.com/v1/` surface. See the
`nhl_api_web` module docstring for the conventions.

### sportsdataverse.nhl.nhl_api.nhl_api_pbp(game_id: int, \*\*kwargs) → Dict

nhl_api_pbp() - **DEPRECATED** — pull a game from `statsapi.web.nhl.com`.

#### Deprecated
Deprecated since version This: function targets the NHL Stats API endpoint that was retired in
September 2023. Use `sportsdataverse.nhl.nhl_web_pbp()` instead,
which hits the current `api-web.nhle.com/v1/gamecenter/{gid}/play-by-play`
endpoint.

Original docstring follows for archival reference.

* **Parameters:**
  **game_id** (*int*) – Unique game_id, can be obtained from nhl_schedule().
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
    “odds”, “onIce”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Pull a single game’s metadata via the legacy NHL Stats API endpoint:

```default
from sportsdataverse.nhl import nhl_api_pbp
game = nhl_api_pbp(game_id=2021020079)
sorted(game.keys())  # ['datetime', 'game', 'gameId', 'gameLink', 'players', 'status', 'teams', 'venues']
print(game["gameId"], game["status"]["abstractGameState"])
```

Inspect the home / away team summary blocks:

```default
game["teams"]["home"]["name"], game["teams"]["away"]["name"]
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_api.nhl_api_schedule(start_date: str, end_date: str, return_as_pandas=False, \*\*kwargs) → DataFrame

nhl_api_schedule() - **DEPRECATED** — pull the schedule from `statsapi.web.nhl.com`.

#### Deprecated
Deprecated since version This: function targets the retired NHL Stats API. Use
`sportsdataverse.nhl.nhl_web_schedule()` instead — which hits
`api-web.nhle.com/v1/schedule/{date}` and returns a week-of-games
payload (the modern API uses 7-day rolls rather than open ranges).

Original docstring follows.

* **Parameters:**
  * **start_date** (*str*) – Start date to pull the NHL API schedule.
  * **end_date** (*str*) – End date to pull the NHL API schedule.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the schedule for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Pull a one-week schedule slice:

```default
from sportsdataverse.nhl import nhl_api_schedule
sched = nhl_api_schedule(start_date="2021-10-23", end_date="2021-10-28")
print(sched.shape)
sched.select(["gamePk", "gameDate", "teams.home.team.name", "teams.away.team.name"]).head()
```

Pandas round-trip:

```default
sched_pd = nhl_api_schedule(
    start_date="2021-10-23", end_date="2021-10-28", return_as_pandas=True
)
sched_pd[["gamePk", "gameDate", "status.detailedState"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

## sportsdataverse.nhl.nhl_api_web module

sportsdataverse.nhl.nhl_api_web — wrappers for `api-web.nhle.com/v1/`.

**Documentation**:

* NHL api-web endpoint reference: [https://py.sportsdataverse.org/docs/nhl/api-web](https://py.sportsdataverse.org/docs/nhl/api-web)
* NHL api-web parser layer: [https://py.sportsdataverse.org/docs/nhl/api-web#parser-layer](https://py.sportsdataverse.org/docs/nhl/api-web#parser-layer)

This is the **modern replacement** for the NHL’s deprecated public Stats API
(`statsapi.web.nhl.com/api/v1/`, retired Sep 2023). The functions in
[`sportsdataverse.nhl.nhl_api`](#module-sportsdataverse.nhl.nhl_api) that target `statsapi.web.nhl.com` are
broken in production and should not be used; this module is their successor.

Endpoint catalog sourced from the OpenAPI spec at
`fastRhockey/data-raw/nhl_api_web_openapi.yaml` (which is itself sourced
from [https://github.com/dfleis/nhl-api-docs](https://github.com/dfleis/nhl-api-docs) and cross-referenced with
[https://github.com/RentoSaijo/nhlscraper/wiki](https://github.com/RentoSaijo/nhlscraper/wiki) and
[https://github.com/coreyjs/nhl-api-py](https://github.com/coreyjs/nhl-api-py)).

### Conventions

* **Season strings** are 8-digit, e.g. `"20242025"` for the 2024-25 season.
  Helpers accept either the 8-digit string OR the end-year as an integer
  (e.g. `2025` → `"20242025"`).
* **Game type**: `1` = preseason, `2` = regular season, `3` = playoffs.
* **Team**: three-letter abbreviation (e.g. `"TOR"`, `"BOS"`).
* **Date**: `YYYY-MM-DD`.
* All functions return `Dict` (the raw JSON payload). Parsing into tidy
  polars frames is a per-endpoint follow-up — for the migration sketch the
  goal is to land a complete, documented surface that callers can mine.

### sportsdataverse.nhl.nhl_api_web.nhl_web_boxscore(game_id: int, \*\*kwargs) → Dict

Pull the boxscore for one NHL game.

Wraps `GET /v1/gamecenter/{gameId}/boxscore`.

* **Returns:**
  `playerByGameStats.{homeTeam,awayTeam}.{forwards,defense,goalies}[]`
  plus team-level shot/goal counts, period scoring, and game status.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_api_web.nhl_web_club_schedule_month(team: str, month: str | None = None, \*\*kwargs) → Dict

Pull a team’s schedule for one month.

Wraps `GET /v1/club-schedule/{team}/month/{month}` or `/now`.

* **Parameters:**
  * **team** – 3-letter abbreviation.
  * **month** – `YYYY-MM` (e.g. `"2024-11"`) or `None` for current month.

### sportsdataverse.nhl.nhl_api_web.nhl_web_club_schedule_season(team: str, season: int | str | None = None, \*\*kwargs) → Dict

Pull a team’s full-season schedule.

Wraps `GET /v1/club-schedule-season/{team}/{season}` or `/now`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_club_schedule_week(team: str, date: str | None = None, \*\*kwargs) → Dict

Pull a team’s schedule for one week.

Wraps `GET /v1/club-schedule/{team}/week/{date}` or `/now`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_club_stats(team: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull a team’s season stat block.

Wraps `GET /v1/club-stats/{team}/{season}/{gameType}` or `/now`.

* **Parameters:**
  * **team** – 3-letter abbreviation.
  * **season** – end-year int / 8-digit string. `None` → `/now`.
  * **game_type** – 1=pre, 2=reg, 3=playoffs.

### sportsdataverse.nhl.nhl_api_web.nhl_web_club_stats_season(team: str, \*\*kwargs) → Dict

Pull the seasons a team has stats for.

Wraps `GET /v1/club-stats-season/{team}`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_draft_picks(year: int | str, round_: int | str = 'all', \*\*kwargs) → Dict

Pull NHL draft picks for a year (and optionally one round).

Wraps `GET /v1/draft/picks/{year}/{round}` (`round` may be `"all"`).

### sportsdataverse.nhl.nhl_api_web.nhl_web_draft_picks_now(\*\*kwargs) → Dict

Pull the current / most recent draft pick set.

Wraps `GET /v1/draft/picks/now`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_draft_rankings(year: int | str, category: int = 1, \*\*kwargs) → Dict

Pull NHL Central Scouting rankings for a draft year.

Wraps `GET /v1/draft/rankings/{year}/{rankingCategory}`.

* **Parameters:**
  **category** – 1 = N.A. skater, 2 = N.A. goalie, 3 = Int. skater, 4 = Int. goalie.

### sportsdataverse.nhl.nhl_api_web.nhl_web_draft_rankings_now(\*\*kwargs) → Dict

Pull the current Central Scouting rankings.

Wraps `GET /v1/draft/rankings/now`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_draft_tracker_picks_now(\*\*kwargs) → Dict

Pull the live draft-tracker pick list (during the draft itself).

Wraps `GET /v1/draft-tracker/picks/now`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_goalie_leaders(season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull goalie stat leaders.

Wraps `GET /v1/goalie-stats-leaders/{season}/{gameType}` or `/current`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_landing(game_id: int, \*\*kwargs) → Dict

Pull the gamecenter landing payload for one NHL game.

Wraps `GET /v1/gamecenter/{gameId}/landing`. The richest single-call
shape: `matchup, summary, three-stars, season-series, gameVideo`, etc.

### sportsdataverse.nhl.nhl_api_web.nhl_web_pbp(game_id: int, \*\*kwargs) → Dict

Pull the play-by-play feed for one NHL game.

Wraps `GET /v1/gamecenter/{gameId}/play-by-play`. Replaces the
deprecated `sportsdataverse.nhl.nhl_api_pbp()` (which targets the
retired `statsapi.web.nhl.com`).

* **Parameters:**
  **game_id** (*int*) – NHL game id. Same identifier ESPN exposes as
  `event_id` is *not* compatible — use the NHL-side game id
  from [`nhl_web_schedule()`](#sportsdataverse.nhl.nhl_api_web.nhl_web_schedule).
* **Returns:**
  `plays[]` array (typeCode, typeDescKey, period, timeInPeriod,
  details with x/y coordinates), plus `gameState`, `rosterSpots[]`,
  `homeTeam` / `awayTeam` blocks with score and shots.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_web_pbp
feed = nhl_web_pbp(2024020001)
print(len(feed["plays"]))
```

### sportsdataverse.nhl.nhl_api_web.nhl_web_player_game_log(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull a player’s game-by-game log.

Wraps `GET /v1/player/{playerId}/game-log/{season}/{gameType}` or `/now`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_player_landing(player_id: int, \*\*kwargs) → Dict

Pull the player profile / overview.

Wraps `GET /v1/player/{playerId}/landing`. Returns the rich shape used
by NHL.com player pages: bio, current team, career totals, season totals,
last 5, awards, drafted info.

### sportsdataverse.nhl.nhl_api_web.nhl_web_player_spotlight(\*\*kwargs) → Dict

Pull the league’s currently featured players.

Wraps `GET /v1/player-spotlight`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_playoff_series(season: int | str, series_letter: str, \*\*kwargs) → Dict

Pull a single playoff series payload.

Wraps `GET /v1/schedule/playoff-series/{season}/{seriesLetter}`.

* **Parameters:**
  * **season** – end-year int or 8-digit string.
  * **series_letter** – `"a"`..\`\`”o”

    ```
    ``
    ```

    , identifying the playoff matchup.

### sportsdataverse.nhl.nhl_api_web.nhl_web_right_rail(game_id: int, \*\*kwargs) → Dict

Pull the gamecenter right-rail payload (in-game widgets).

Wraps `GET /v1/gamecenter/{gameId}/right-rail`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_roster(team: str, season: int | str | None = None, \*\*kwargs) → Dict

Pull a team’s roster.

Wraps `GET /v1/roster/{team}/{season}` or `/v1/roster/{team}/current`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_roster_season(team: str, \*\*kwargs) → Dict

Pull every season a team has had on file.

Wraps `GET /v1/roster-season/{team}`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_schedule(date: str | None = None, \*\*kwargs) → Dict

Pull the week-of NHL schedule rooted at `date`.

Wraps `GET /v1/schedule/{date}` or `/v1/schedule/now`. The response
carries a week’s worth of games — the NHL schedules in 7-day rolls.

* **Parameters:**
  **date** – `YYYY-MM-DD` or `None` to use the current week.
* **Returns:**
  `gameWeek[].{date, dayAbbrev, games[]}`.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_api_web.nhl_web_schedule_calendar(date: str | None = None, \*\*kwargs) → Dict

Pull the calendar of game-days for the season.

Wraps `GET /v1/schedule-calendar/{date}` or `/v1/schedule-calendar/now`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_score(date: str | None = None, \*\*kwargs) → Dict

Pull the single-day scoreboard for `date`.

Wraps `GET /v1/score/{date}` or `/v1/score/now`.

* **Returns:**
  `games[]` with one entry per game on that date plus the in-game
  clock / period / score.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_api_web.nhl_web_scoreboard(date: str | None = None, team: str | None = None, \*\*kwargs) → Dict

Pull the in-game scoreboard payload.

Wraps one of:
: * `GET /v1/scoreboard/{date}` — league-wide on a date
  * `GET /v1/scoreboard/now` — league-wide now
  * `GET /v1/scoreboard/{team}/now` — team-scoped now

* **Parameters:**
  * **date** – `YYYY-MM-DD`. If both `date` and `team` are None, defaults
    to `/v1/scoreboard/now`.
  * **team** – 3-letter abbreviation (mutually exclusive with `date`).

### sportsdataverse.nhl.nhl_api_web.nhl_web_skater_leaders(season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull skater stat leaders.

Wraps `GET /v1/skater-stats-leaders/{season}/{gameType}` or `/current`.

### sportsdataverse.nhl.nhl_api_web.nhl_web_standings(date: str | None = None, \*\*kwargs) → Dict

Pull the NHL standings.

Wraps `GET /v1/standings/{date}` or `/v1/standings/now`.

* **Returns:**
  `standings[]` one row per team with `teamAbbrev, conferenceName,
  divisionName, gamesPlayed, wins, losses, otLosses, points, pointPctg,
  goalFor, goalAgainst, goalDifferential, leagueSequence, divisionSequence,
  wildcardSequence`.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_api_web.nhl_web_standings_season(\*\*kwargs) → Dict

Pull the per-season standings cutover dates.

Wraps `GET /v1/standings-season`. Useful for resolving “the standings
snapshot at the end of regular season N” without hard-coding dates.

## sportsdataverse.nhl.nhl_api_web_parsers module

sportsdataverse.nhl.nhl_api_web_parsers — polars parsers for the modern
NHL game-feed API at `api-web.nhle.com/v1/`.

**Documentation**:

* NHL api-web parser deep-dive: [https://py.sportsdataverse.org/docs/nhl/api-web](https://py.sportsdataverse.org/docs/nhl/api-web)
* Parsers overview: [https://py.sportsdataverse.org/docs/parsers/](https://py.sportsdataverse.org/docs/parsers/)
* Reusable patterns: [https://py.sportsdataverse.org/docs/architecture/building-blocks](https://py.sportsdataverse.org/docs/architecture/building-blocks)

Each `nhl_web_*` wrapper in [`sportsdataverse.nhl.nhl_api_web`](#module-sportsdataverse.nhl.nhl_api_web) ships
a different payload shape — game-center deep dives carry per-team player
arrays, schedules nest day → games, leaderboards key by stat-category, and
the right-rail endpoint exposes 8+ independent sub-frames. This module
mirrors the design of `sportsdataverse._common_espn_parsers`:

* Every parser returns `polars.DataFrame` by default; pass
  `return_as_pandas=True` for pandas.
* Empty / malformed payloads return a zero-row frame instead of raising.
* Output columns are snake-cased via
  `sportsdataverse.dl_utils.underscore()`.
* List-valued cells are stringified so polars accepts the frame.

Parsers fall into three groups:

1. **Game-center**:  [`parse_nhl_web_pbp()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_pbp),
   [`parse_nhl_web_boxscore()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_boxscore), [`parse_nhl_web_landing()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_landing),
   [`parse_nhl_web_right_rail()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_right_rail) (dispatcher returning 6 sub-frames).
2. **Schedule / score**: [`parse_nhl_web_schedule()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_schedule),
   [`parse_nhl_web_score()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_score), [`parse_nhl_web_scoreboard()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_scoreboard),
   [`parse_nhl_web_club_schedule()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_club_schedule).
3. **Team / player / standings / leaders / draft**:
   [`parse_nhl_web_standings()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_standings), [`parse_nhl_web_standings_season()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_standings_season),
   [`parse_nhl_web_club_stats()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_club_stats) (dispatcher returning skaters +
   goalies), [`parse_nhl_web_roster()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_roster) (merges 3 position groups),
   [`parse_nhl_web_player_landing()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_player_landing),
   [`parse_nhl_web_player_game_log()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_player_game_log), [`parse_nhl_web_leaders()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_leaders),
   [`parse_nhl_web_draft_picks()`](#sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_draft_picks).

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_boxscore(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_boxscore()` into one row per (team × player).

Boxscore ships `playerByGameStats: {awayTeam: {forwards, defense,
goalies\}, homeTeam: \{forwards, defense, goalies\}\}`. This parser
walks all six (team × position-group) buckets and tags each row
with `home_away` (“home” / “away”) and `position_group`
(“forwards” / “defense” / “goalies”) so the output is one tidy
long-form frame.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_club_schedule(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_club_schedule_season()` / `_month` / `_week`
into one row per game.

All three club-schedule endpoints share the `{games: [...]}`
payload shape plus a few context fields (`currentSeason`,
`previousSeason`, `clubTimezone`). The context fields are
prefixed onto each row as `club_*` columns.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_club_stats(payload: Dict, section: str = None, return_as_pandas: bool = False)

Parse `nhl_web_club_stats()` — dispatcher with skaters + goalies.

Returns a dict `{skaters: <frame>, goalies: <frame>}` by default,
or a single frame when `section="skaters"` / `"goalies"`.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_draft_picks(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_draft_picks()` into one row per pick.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_landing(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_landing()` into a single-row game profile.

The landing endpoint ships the game header (id, date, venue, teams,
periodDescriptor, gameState, clock, plus a `summary` sub-dict with
scoring / threeStars / penalties — those are stringified to keep the
output one row per call).

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_leaders(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_skater_leaders()` / `nhl_web_goalie_leaders()`
into one row per (category × player).

The leaders payloads are keyed by stat category at the top level —
e.g. `{points: [<10 player rows>], goals: [<10 player rows>], ...}`
for skaters; `{wins: [...], savePctg: [...]}` for goalies. This
parser walks every top-level list-valued key, tags each row with
the `category` it came from, and concatenates.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_pbp(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_pbp()` into one row per play.

Walks `payload["plays"]` (~330 plays per game) and flattens each
play’s nested `periodDescriptor` / `details` sub-dicts. The PBP
feed identifies plays by `eventId` + `sortOrder` and keys event
types via `typeCode` / `typeDescKey`.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_player_game_log(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_player_game_log()` into one row per game.

Walks `payload["gameLog"]` (~76 games per season for a regular
skater) and flattens.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_player_landing(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_player_landing()` into a single-row player
profile. Nested `featuredStats` / `careerTotals` / `last5Games`
sub-frames are stringified — call them out separately if needed.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_right_rail(payload: Dict, section: str = None, return_as_pandas: bool = False)

Parse `nhl_web_right_rail()` — dispatcher with 6 sub-frames.

The right-rail endpoint ships game-context sub-frames typically
rendered alongside the box-score on NHL.com:

* `season_series`    — list of head-to-head games (~7 rows)
* `shots_by_period`  — per-period shot totals (3 rows)
* `team_game_stats`  — per-category team-vs-team stat comparison
  : (~10 rows; one row per category)
* `game_info`        — single-row game-info dict (referees,
  : linesmen, awayTeam, homeTeam fields)
* `linescore_by_period` — per-period score breakdown
* `season_series_wins` — single-row aggregate of series wins

With `section=None` (default), returns a dict of all 6 sub-frames
keyed by section name. With `section="<name>"`, returns just that
one frame.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_roster(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_roster()` into one row per player.

Shape: `{forwards: [...], defensemen: [...], goalies: [...]}`.
Merges all three position groups with a `position_group` column
so the output is one long-form frame instead of three.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_schedule(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_schedule()` into one row per scheduled game.

Input: `{gameWeek: [{date, dayAbbrev, numberOfGames, games: [...]},
...], ...\}`. Walks every `gameWeek[].games[]` and prefixes the
day’s `date` onto each game row.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_score(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_score()` into one row per game for the date.

Shape: `{currentDate, games: [...], gameWeek: [...]}`.
Returns the `games` array flattened.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_scoreboard(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_scoreboard()` into one row per game across days.

Shape: `{focusedDate, gamesByDate: [{date, games: [...]}, ...]}`.
Walks every `gamesByDate[].games[]` and prefixes the day’s
`date` as `scoreboard_date`.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_standings(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_standings()` into one row per team.

### sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_standings_season(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse `nhl_web_standings_season()` into one row per season.

### sportsdataverse.nhl.nhl_api_web_parsers.parser_for_nhl_api_web(fn_name: str)

Return the registered parser for an `nhl_web_*` wrapper.

Returns `None` for endpoints without a registered parser (e.g.
`playoff_series`, `player_spotlight`, `draft_rankings`) since
their payloads are too idiosyncratic for a useful generic fallback.
Callers should null-check the result.

* **Parameters:**
  **fn_name** – The `__name__` of any `nhl_web_*` wrapper.
* **Returns:**
  Parser callable, or `None` if unregistered.

## sportsdataverse.nhl.nhl_edge module

sportsdataverse.nhl.nhl_edge — wrappers for NHL EDGE Statcast endpoints.

**Documentation**:

* NHL EDGE endpoint reference: [https://py.sportsdataverse.org/docs/nhl/edge](https://py.sportsdataverse.org/docs/nhl/edge)
* NHL EDGE parser deep-dive: [https://py.sportsdataverse.org/docs/nhl/edge-parsers](https://py.sportsdataverse.org/docs/nhl/edge-parsers)

NHL EDGE is the league’s Statcast-equivalent tracking system, exposing puck
and player positional data, shot speed, skating distance/speed, shot-location
heat maps, and zone-time metrics.  Endpoints live under two path families:

* `/v1/edge/*`  — primary player / team EDGE stats
* `/v1/cat/edge/*` — categorized (composite) player views

Endpoint catalog sourced from the OpenAPI spec at
`fastRhockey/data-raw/nhl_api_web_openapi.yaml`.

### Conventions

* **Season strings** — 8-digit, e.g. `"20242025"` for the 2024-25 season.
  Pass an 8-digit string or the 4-digit end-year as an int (`2025` →
  `"20242025"`).  Pass `None` to hit the `/now` variant (current season).
* **Game type** — `1` = preseason, `2` = regular season, `3` = playoffs.
* **Positions / strength / sortBy** — string slugs used as-is in the URL path
  (e.g. `"all"`, `"5v5"`, `"maxSpeed"`).
* All functions return `Dict` (the raw JSON payload).

### sportsdataverse.nhl.nhl_edge.nhl_edge_cat_goalie_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull categorized (cat) EDGE detail stats for a single goalie.

Wraps `GET /v1/cat/edge/goalie-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  Cat EDGE goalie detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_cat_skater_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull categorized (cat) EDGE detail stats for a single skater.

Wraps `GET /v1/cat/edge/skater-detail/{playerId}/now` or
`/{season}/{gameType}`.  The `/cat/edge/` family returns a composite
view that groups metrics into named categories, useful for radar/spider
chart visualizations on NHL.com.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  Cat EDGE skater detail payload.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_edge_cat_skater_detail
nhl_edge_cat_skater_detail(8480801)
```

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_5v5_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE 5-on-5 detail stats for a single goalie.

Wraps `GET /v1/edge/goalie-5v5-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE goalie 5v5 detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_5v5_top_10(sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 goalies by 5-on-5 metrics.

Wraps `GET /v1/edge/goalie-5v5-top-10/{sortBy}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **sort_by** (*str*) – Sort metric slug (e.g. `"savePctg"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 goalie 5v5 payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_comparison(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE comparison data for a single goalie.

Wraps `GET /v1/edge/goalie-comparison/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE goalie comparison payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE detail stats for a single goalie.

Wraps `GET /v1/edge/goalie-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE goalie detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_edge_save_pctg_top_10(sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 goalies by save-percentage.

Wraps `GET /v1/edge/goalie-edge-save-pctg-top-10/{sortBy}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **sort_by** (*str*) – Sort metric slug (e.g. `"savePctgAboveExpected"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 goalie save-percentage payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_landing(season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE goalie landing page (summary across all goalies).

Wraps `GET /v1/edge/goalie-landing/now` or
`/v1/edge/goalie-landing/{season}/{gameType}`.

* **Parameters:**
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE goalie landing payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_save_percentage_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE save-percentage detail for a single goalie.

Wraps `GET /v1/edge/goalie-save-percentage-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE goalie save-percentage detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_shot_location_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE shot-location detail for a single goalie.

Wraps `GET /v1/edge/goalie-shot-location-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE goalie shot-location detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_goalie_shot_location_top_10(category: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 goalies for a shot-location category.

Wraps `GET /v1/edge/goalie-shot-location-top-10/{category}/{sortBy}/now`
or `/{season}/{gameType}`.

* **Parameters:**
  * **category** (*str*) – Shot-location category slug (e.g. `"shotAttempts"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"savePctg"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 goalie shot-location payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_comparison(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE comparison data for a single skater.

Wraps `GET /v1/edge/skater-comparison/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE skater comparison payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE detail stats for a single skater.

Wraps `GET /v1/edge/skater-detail/{playerId}/now` or
`/v1/edge/skater-detail/{playerId}/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    current season (`/now`).
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE skater detail payload.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_edge_skater_detail
nhl_edge_skater_detail(8480801)
```

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_distance_top_10(positions: str, strength: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 skaters by skating distance.

Wraps `GET /v1/edge/skater-distance-top-10/{positions}/{strength}/{sortBy}/now`
or `/{season}/{gameType}`.

* **Parameters:**
  * **positions** (*str*) – Position filter slug (e.g. `"all"`, `"F"`,
    `"D"`).
  * **strength** (*str*) – Strength state slug (e.g. `"all"`, `"5v5"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"totalDistance"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 skater distance payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_landing(season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE skater landing page (summary across all skaters).

Wraps `GET /v1/edge/skater-landing/now` or
`/v1/edge/skater-landing/{season}/{gameType}`.

* **Parameters:**
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE skater landing payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_shot_location_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE shot-location detail for a single skater.

Wraps `GET /v1/edge/skater-shot-location-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE skater shot-location detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_shot_location_top_10(position: str, category: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 skaters for a shot-location category.

Wraps `GET /v1/edge/skater-shot-location-top-10/{position}/{category}/{sortBy}/now`
or `/{season}/{gameType}`.

* **Parameters:**
  * **position** (*str*) – Position slug, e.g. `"all"`, `"C"`, `"L"`,
    `"R"`, `"D"`.
  * **category** (*str*) – Shot-location category slug (e.g. `"shotAttempts"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"goals"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 skater shot-location payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_shot_speed_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE shot-speed detail for a single skater.

Wraps `GET /v1/edge/skater-shot-speed-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE skater shot-speed detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_shot_speed_top_10(positions: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 skaters by shot speed.

Wraps `GET /v1/edge/skater-shot-speed-top-10/{positions}/{sortBy}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **positions** (*str*) – Position filter slug (e.g. `"all"`, `"F"`,
    `"D"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"maxSpeed"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 skater shot-speed payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_skating_distance_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE skating-distance detail for a single skater.

Wraps `GET /v1/edge/skater-skating-distance-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE skater skating-distance detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_skating_speed_detail(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE skating-speed detail for a single skater.

Wraps `GET /v1/edge/skater-skating-speed-detail/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE skater skating-speed detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_speed_top_10(positions: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 skaters by skating speed.

Wraps `GET /v1/edge/skater-speed-top-10/{positions}/{sortBy}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **positions** (*str*) – Position filter slug (e.g. `"all"`, `"F"`,
    `"D"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"maxSpeed"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 skater speed payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_zone_time(player_id: int, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE zone-time detail for a single skater.

Wraps `GET /v1/edge/skater-zone-time/{playerId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **player_id** (*int*) – NHL player id.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE skater zone-time payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_skater_zone_time_top_10(positions: str, strength: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 skaters by zone time.

Wraps `GET /v1/edge/skater-zone-time-top-10/{positions}/{strength}/{sortBy}/now`
or `/{season}/{gameType}`.

* **Parameters:**
  * **positions** (*str*) – Position filter slug (e.g. `"all"`, `"F"`,
    `"D"`).
  * **strength** (*str*) – Strength state slug (e.g. `"all"`, `"5v5"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"offZoneTime"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 skater zone-time payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_detail(team_id: int | str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE detail stats for a single team.

Wraps `GET /v1/edge/team-detail/{teamId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **team_id** – NHL team id (integer) or 3-letter abbreviation string.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE team detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_landing(season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE team landing page (summary across all teams).

Wraps `GET /v1/edge/team-landing/now` or
`/v1/edge/team-landing/{season}/{gameType}`.

* **Parameters:**
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE team landing payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_shot_location_detail(team_id: int | str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE shot-location detail for a single team.

Wraps `GET /v1/edge/team-shot-location-detail/{teamId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **team_id** – NHL team id (integer) or 3-letter abbreviation string.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE team shot-location detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_shot_location_top_10(position: str, category: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 teams for a shot-location category.

Wraps `GET /v1/edge/team-shot-location-top-10/{position}/{category}/{sortBy}/now`
or `/{season}/{gameType}`.

* **Parameters:**
  * **position** (*str*) – Position context slug (e.g. `"all"`).
  * **category** (*str*) – Shot-location category slug (e.g. `"shotAttempts"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"goals"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 team shot-location payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_shot_speed_detail(team_id: int | str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE shot-speed detail for a single team.

Wraps `GET /v1/edge/team-shot-speed-detail/{teamId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **team_id** – NHL team id (integer) or 3-letter abbreviation string.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE team shot-speed detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_skating_distance_detail(team_id: int | str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE skating-distance detail for a single team.

Wraps `GET /v1/edge/team-skating-distance-detail/{teamId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **team_id** – NHL team id (integer) or 3-letter abbreviation string.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE team skating-distance detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_skating_distance_top_10(positions: str, strength: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 teams by skating distance.

Wraps `GET /v1/edge/team-skating-distance-top-10/{positions}/{strength}/{sortBy}/now`
or `/{season}/{gameType}`.

* **Parameters:**
  * **positions** (*str*) – Position filter slug (e.g. `"all"`, `"F"`,
    `"D"`).
  * **strength** (*str*) – Strength state slug (e.g. `"all"`, `"5v5"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"totalDistance"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 team skating-distance payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_skating_speed_detail(team_id: int | str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE skating-speed detail for a single team.

Wraps `GET /v1/edge/team-skating-speed-detail/{teamId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **team_id** – NHL team id (integer) or 3-letter abbreviation string.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE team skating-speed detail payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_skating_speed_top_10(positions: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 teams by skating speed.

Wraps `GET /v1/edge/team-skating-speed-top-10/{positions}/{sortBy}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **positions** (*str*) – Position filter slug (e.g. `"all"`, `"F"`,
    `"D"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"maxSpeed"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 team skating-speed payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_zone_time_details(team_id: int | str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull EDGE zone-time details for a single team.

Wraps `GET /v1/edge/team-zone-time-details/{teamId}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **team_id** – NHL team id (integer) or 3-letter abbreviation string.
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE team zone-time details payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_edge.nhl_edge_team_zone_time_top_10(strength: str, sort_by: str, season: int | str | None = None, game_type: int = 2, \*\*kwargs) → Dict

Pull the EDGE top-10 teams by zone time.

Wraps `GET /v1/edge/team-zone-time-top-10/{strength}/{sortBy}/now` or
`/{season}/{gameType}`.

* **Parameters:**
  * **strength** (*str*) – Strength state slug (e.g. `"all"`, `"5v5"`).
  * **sort_by** (*str*) – Sort metric slug (e.g. `"offZoneTime"`).
  * **season** – 8-digit season string or 4-digit end-year int.  `None` →
    `/now`.
  * **game_type** (*int*) – 1=pre, 2=reg, 3=playoffs.  Default 2.
* **Returns:**
  EDGE top-10 team zone-time payload.
* **Return type:**
  Dict

## sportsdataverse.nhl.nhl_edge_parsers module

sportsdataverse.nhl.nhl_edge_parsers — polars parsers for NHL EDGE payloads.

**Documentation**:

* NHL EDGE parser deep-dive: [https://py.sportsdataverse.org/docs/nhl/edge-parsers](https://py.sportsdataverse.org/docs/nhl/edge-parsers)
* NHL EDGE endpoint reference: [https://py.sportsdataverse.org/docs/nhl/edge](https://py.sportsdataverse.org/docs/nhl/edge)
* Parsers overview: [https://py.sportsdataverse.org/docs/parsers/](https://py.sportsdataverse.org/docs/parsers/)

NHL EDGE returns position-tracking / shot-speed / zone-time data via
`api-web.nhle.com/v1/edge/*`. The OpenAPI spec
(`fastRhockey/data-raw/nhl_api_web_openapi.yaml`) declares every response
as `type: object` with no inner schema, so this module’s parsers are
**defensive by design** — they walk through a sequence of likely top-level
keys, fall back to `pandas.json_normalize` on whatever shape comes back,
and return a zero-row polars frame rather than raising when the payload is
empty.

### Parser families

The 35 EDGE endpoints in [`sportsdataverse.nhl.nhl_edge`](#module-sportsdataverse.nhl.nhl_edge) cluster into
four primary shape families plus a sub-frame family for unrolling nested
lists inside detail payloads:

* **Leaderboards** (`*_top_10`) — list of player/team rows with shared
  schema. Parser: [`parse_edge_top10()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_top10).  *(Note: all \`\`\*_top_10\`\`
  URL paths return 404 as of 2026-05-23 — the parser is kept for
  forward-compat if NHL restores the surface.)*
* **Detail pages** (`*_detail`, `*_5v5_detail`, `*_comparison`) —
  multi-section single-entity payload. Parser: [`parse_edge_detail()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_detail).
* **Shot-location** (`*_shot_location_*`) — strike-zone–style heat map
  with one cell per zone (17-cell grid + 4-12 row aggregate).
  Parser: [`parse_edge_shot_location()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_shot_location).
* **Zone-time** (`*_zone_time_*`) — possession share by zone (offensive,
  defensive, neutral; with strength-state splits where available).
  Parser: [`parse_edge_zone_time()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_zone_time).
* **Sub-frame parsers** for the nested lists that `parse_edge_detail`
  deliberately stringifies (to keep the output one row per call):
  - [`parse_edge_sog_details()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_sog_details) — 17-cell SOG / save grid from
    skater-detail, team-detail, goalie-detail, `*-shot-location-detail`.
  - [`parse_edge_sog_summary()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_sog_summary) — 4-row location-code aggregate from
    the same endpoints.
  - [`parse_edge_hardest_shots()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_hardest_shots) — 10-row hardest-shots list from
    `skater-shot-speed-detail`.

Endpoints not in those families pass through as raw `Dict`; call
[`parse_edge_payload()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_payload) for a best-effort flatten.

### Usage

> from sportsdataverse.nhl import nhl_edge_skater_shot_speed_top_10
> from sportsdataverse.nhl.nhl_edge_parsers import parse_edge_top10

> raw = nhl_edge_skater_shot_speed_top_10(“all”, “maxSpeed”)
> df = parse_edge_top10(raw)
> print(df.shape)

### sportsdataverse.nhl.nhl_edge_parsers.parse_edge_detail(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse an EDGE detail / comparison payload into a single-row frame.

Flattens the entire payload one level deep via
`pandas.json_normalize()`. List-valued attributes (e.g. season-by-
season splits, shot-location grids) are kept as their string
representation so the result remains one row per detail call.  Use
[`parse_edge_shot_location()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_shot_location) / [`parse_edge_zone_time()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_zone_time) when
you need the nested structures unrolled into long-form rows.

* **Parameters:**
  * **payload** – Raw JSON dict from any `nhl_edge_*_detail` /
    `nhl_edge_*_comparison` wrapper.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas) with one row summarising the detail
  payload, columns auto-flattened. Returns a zero-row frame when
  `payload` is empty.

### sportsdataverse.nhl.nhl_edge_parsers.parse_edge_hardest_shots(payload: Dict, return_as_pandas: bool = False) → DataFrame

Extract the hardest-shots list from `skater-shot-speed-detail`.

The endpoint ships `hardestShots: list[10]` with per-shot metadata
(`gameDate`, `shotSpeed`, `timeInPeriod`, etc.). This parser
returns those 10 rows as a tidy frame.

* **Parameters:**
  * **payload** – Raw JSON dict from `nhl_edge_skater_shot_speed_detail`.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas) with one row per hardest shot; zero
  rows when `hardestShots` is missing or empty.

### sportsdataverse.nhl.nhl_edge_parsers.parse_edge_payload(payload: Dict, return_as_pandas: bool = False) → DataFrame

Generic best-effort flatten for any EDGE payload shape.

Picks the largest list of dicts inside the payload (most likely to be
the “interesting” row source) and flattens it; falls back to flattening
the payload itself as a single row.

* **Parameters:**
  * **payload** – Raw JSON dict from any `nhl_edge_*` wrapper.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas). Zero-row when payload is empty.

### sportsdataverse.nhl.nhl_edge_parsers.parse_edge_shot_location(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse an EDGE shot-location heat map into long-form rows.

Picks the most granular zone list available in the payload, in the
priority order `shotLocationDetails` → `sogDetails` →
`shotLocationTotals` → `shotLocationSummary` → `sogSummary`.
Each zone becomes one row in the output frame.

Skater / team detail payloads carry **both** a granular 17-cell grid
(`sogDetails` / `shotLocationDetails`) and a 4-row aggregate
(`sogSummary` / `shotLocationSummary`). When both are present,
only the granular grid is returned — call [`parse_edge_sog_summary()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_sog_summary)
for the aggregate view.

* **Parameters:**
  * **payload** – Raw JSON dict from any `nhl_edge_*_shot_location_*`
    wrapper, or from any `*_detail` wrapper that ships a
    shot-location grid inline.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas) with one row per zone cell. Returns
  a zero-row frame when no recognized zone list is found.

### sportsdataverse.nhl.nhl_edge_parsers.parse_edge_sog_details(payload: Dict, return_as_pandas: bool = False) → DataFrame

Extract the 17-cell shots-on-goal heat map from a detail payload.

Looks for `sogDetails` (skater-detail, team-detail) or
`shotLocationDetails` (goalie-detail,

```
*
```

-shot-location-detail).
Returns one row per zone cell with the `area` column plus shot /
goal / save metrics depending on the entity type.

* **Parameters:**
  * **payload** – Raw JSON dict from any `*_detail` wrapper.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas), zero-row if the payload lacks both
  `sogDetails` and `shotLocationDetails`.

### sportsdataverse.nhl.nhl_edge_parsers.parse_edge_sog_summary(payload: Dict, return_as_pandas: bool = False) → DataFrame

Extract the 4-row shots-on-goal location aggregate from a detail payload.

Looks for `sogSummary` (skater-detail, team-detail),
`shotLocationSummary` (goalie-detail), or `shotLocationTotals`
(team-shot-location-detail, goalie-shot-location-detail). Returns
one row per location code with shot / goal / save metrics.

* **Parameters:**
  * **payload** – Raw JSON dict from any `*_detail` wrapper.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas), zero-row if no aggregate is found.

### sportsdataverse.nhl.nhl_edge_parsers.parse_edge_top10(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse an EDGE leaderboard (`*_top_10`) response into a tidy frame.

Tries a sequence of likely top-level keys (`"top10"`, `"leaderboard"`,
`"players"`, `"skaters"`, `"goalies"`, `"teams"`, `"data"`,
`"items"`) — the first that resolves to a non-empty list is the row
source.  Flattens with `pandas.json_normalize()`, snake-cases the
columns, and converts to polars.

* **Parameters:**
  * **payload** – Raw JSON dict from any `nhl_edge_*_top_10` wrapper.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas) with one row per ranked entity. Returns
  a zero-row frame when `payload` is empty or no candidate key
  resolves to a non-empty list.

### sportsdataverse.nhl.nhl_edge_parsers.parse_edge_zone_time(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse an EDGE zone-time payload into long-form rows.

Each zone (offensive / defensive / neutral) or strength-state row
becomes one row in the output frame. Falls back to flattening the
entire payload as a single row when no recognized zone list is found.

* **Parameters:**
  * **payload** – Raw JSON dict from any `nhl_edge_*_zone_time_*` wrapper.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas), zero-row if the payload is empty.

### sportsdataverse.nhl.nhl_edge_parsers.parser_for_edge(fn_name: str)

Return the registered EDGE parser for a wrapper name.

Falls back to [`parse_edge_payload()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_payload) (generic best-effort flatten)
when no specific parser is registered, so the caller always gets a
DataFrame-returning function rather than `None`.

* **Parameters:**
  **fn_name** – The `__name__` of any `nhl_edge_*` wrapper.
* **Returns:**
  one of [`parse_edge_top10()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_top10),
  [`parse_edge_detail()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_detail), [`parse_edge_shot_location()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_shot_location),
  [`parse_edge_zone_time()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_zone_time), or [`parse_edge_payload()`](#sportsdataverse.nhl.nhl_edge_parsers.parse_edge_payload).
* **Return type:**
  Parser callable

## sportsdataverse.nhl.nhl_game_rosters module

### sportsdataverse.nhl.nhl_game_rosters.espn_nhl_game_rosters(game_id: int, raw=False, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nhl_game_rosters() - Pull the game by id.

* **Parameters:**
  * **game_id** (*int*) – Unique game_id, can be obtained from espn_nhl_schedule().
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe of game roster data with columns:
  ‘athlete_id’, ‘athlete_uid’, ‘athlete_guid’, ‘athlete_type’,
  ‘first_name’, ‘last_name’, ‘full_name’, ‘athlete_display_name’,
  ‘short_name’, ‘weight’, ‘display_weight’, ‘height’, ‘display_height’,
  ‘age’, ‘date_of_birth’, ‘slug’, ‘jersey’, ‘linked’, ‘active’,
  ‘alternate_ids_sdr’, ‘birth_place_city’, ‘birth_place_state’,
  ‘birth_place_country’, ‘headshot_href’, ‘headshot_alt’,
  ‘experience_years’, ‘experience_display_value’,
  ‘experience_abbreviation’, ‘status_id’, ‘status_name’, ‘status_type’,
  ‘status_abbreviation’, ‘hand_type’, ‘hand_abbreviation’,
  ‘hand_display_value’, ‘draft_display_text’, ‘draft_round’, ‘draft_year’,
  ‘draft_selection’, ‘player_id’, ‘starter’, ‘valid’, ‘did_not_play’,
  ‘display_name’, ‘ejected’, ‘athlete_href’, ‘position_href’,
  ‘statistics_href’, ‘team_id’, ‘team_guid’, ‘team_uid’, ‘team_slug’,
  ‘team_location’, ‘team_name’, ‘team_abbreviation’,
  ‘team_display_name’, ‘team_short_display_name’, ‘team_color’,
  ‘team_alternate_color’, ‘is_active’, ‘is_all_star’,
  ‘logo_href’, ‘logo_dark_href’, ‘game_id’
* **Return type:**
  pl.DataFrame

### Example

Pull both teams’ rosters for a single game (Stanley Cup Final 2023):

```default
from sportsdataverse.nhl import espn_nhl_game_rosters
rosters = espn_nhl_game_rosters(game_id=401559395)
print(rosters.shape)
rosters.select(["athlete_display_name", "jersey", "team_abbreviation", "starter"]).head(10)
```

Just the starters:

```default
import polars as pl
rosters.filter(pl.col("starter") == True).select(["athlete_display_name", "team_abbreviation"])
```

Pandas round-trip:

```default
rosters_pd = espn_nhl_game_rosters(game_id=401559395, return_as_pandas=True)
rosters_pd[["athlete_display_name", "team_abbreviation", "did_not_play"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_athlete_items(teams_rosters, \*\*kwargs)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_game_items(summary)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_roster_items(items, summary_url, \*\*kwargs)

### sportsdataverse.nhl.nhl_game_rosters.helper_nhl_team_items(items, \*\*kwargs)

## sportsdataverse.nhl.nhl_loaders module

### sportsdataverse.nhl.nhl_loaders.load_nhl_pbp(seasons: List[int], return_as_pandas=False) → DataFrame

Load NHL play by play data going back to 2011

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2011 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the play-by-plays available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2011.

### Example

Pull a single season’s play-by-play parquet:

```default
from sportsdataverse.nhl import load_nhl_pbp
pbp = load_nhl_pbp(seasons=2023)
print(pbp.shape)
```

Pull a range of seasons:

```default
pbp = load_nhl_pbp(seasons=range(2018, 2024))
pbp.group_by("season").len().sort("season")
```

Filter to goal events and round-trip to pandas:

```default
import polars as pl
goals = pbp.filter(pl.col("type_text") == "Goal")
goals_pd = goals.to_pandas()
goals_pd[["season", "period", "time", "text"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_loaders.load_nhl_player_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load NHL player boxscore data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2011 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  player boxscores available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2011.

### Example

Pull player box scores for a single season:

```default
from sportsdataverse.nhl import load_nhl_player_boxscore
pb = load_nhl_player_boxscore(seasons=2023)
print(pb.shape)
```

Top 10 single-game point performers:

```default
import polars as pl
pb.with_columns(points=pl.col("goals") + pl.col("assists")).sort(
    "points", descending=True
).select(["game_id", "athlete_display_name", "goals", "assists", "points"]).head(10)
```

Pandas round-trip across multiple seasons:

```default
pb_pd = load_nhl_player_boxscore(seasons=range(2020, 2024), return_as_pandas=True)
pb_pd.groupby("season")[["goals", "assists"]].sum()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_loaders.load_nhl_schedule(seasons: List[int], return_as_pandas=False) → DataFrame

Load NHL schedule data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2002 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the schedule for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Pull a single season’s schedule:

```default
from sportsdataverse.nhl import load_nhl_schedule
sched = load_nhl_schedule(seasons=2023)
print(sched.shape)
```

Pull a range of seasons and count by status:

```default
sched = load_nhl_schedule(seasons=range(2018, 2024))
sched.group_by(["season", "status_type_description"]).len().sort(["season", "len"])
```

Pandas round-trip with a single season:

```default
sched_pd = load_nhl_schedule(seasons=[2023], return_as_pandas=True)
sched_pd[["game_id", "home_name", "away_name", "game_date"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_loaders.load_nhl_team_boxscore(seasons: List[int], return_as_pandas=False) → DataFrame

Load NHL team boxscore data

* **Parameters:**
  * **seasons** (*list*) – Used to define different seasons. 2011 is the earliest available season.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing the
  team boxscores available for the requested seasons.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2011.

### Example

Pull team box scores for a single season:

```default
from sportsdataverse.nhl import load_nhl_team_boxscore
tb = load_nhl_team_boxscore(seasons=2023)
print(tb.shape)
```

Pull a range of seasons:

```default
tb = load_nhl_team_boxscore(seasons=range(2018, 2024))
tb.group_by("season").len().sort("season")
```

Tampa Bay Lightning (team_id 14) game-by-game scoring:

```default
import polars as pl
tb.filter(pl.col("team_id") == 14).select(["game_id", "team_score", "opponent_team_score"]).head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_loaders.nhl_teams(return_as_pandas=False) → DataFrame

Load NHL team ID information and logos

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams available for the requested seasons.
* **Return type:**
  pl.DataFrame

### Example

Pull the static teams + logos table:

```default
from sportsdataverse.nhl import nhl_teams
teams = nhl_teams()
print(teams.shape)
teams.head()
```

Pandas round-trip — convenient for joining against your own roster table:

```default
teams_pd = nhl_teams(return_as_pandas=True)
list(teams_pd.columns)[:10]
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

## sportsdataverse.nhl.nhl_pbp module

### sportsdataverse.nhl.nhl_pbp.espn_nhl_pbp(game_id: int, raw=False, \*\*kwargs) → Dict

espn_nhl_pbp() - Pull the game by id. Data from API endpoints - nhl/playbyplay, nhl/summary

#### NOTE
This is the **ESPN** NHL play-by-play, not the modern NHL
api-web one. The two surfaces have different ID spaces and
different schemas — they are NOT interchangeable:

- `espn_nhl_pbp(game_id)` uses **ESPN event IDs** (e.g.
  `401559395`). Returns a Dict of ~17 sub-frames matching
  the ESPN Site v2 summary shape (boxscore / plays / leaders /
  standings / etc.). Useful for historical alignment with the
  hoopR / wehoop R-package data stack.
- `nhl_web_pbp(game_id)` + `parse_nhl_web_pbp(payload)`
  uses **NHL native game IDs** (e.g. `2023030417`). Returns
  the modern api-web.nhle.com PBP shape (`plays[]` with
  `eventId`, `typeCode`, `typeDescKey`,
  `periodDescriptor`, nested `details`). Use this for live
  games + modern NHL.com source-of-truth data.

Pick the surface that matches your ID space + downstream join
keys. The two cannot be cross-referenced by `game_id`.

* **Parameters:**
  **game_id** (*int*) – Unique ESPN event id (NOT the NHL native game
  id), can be obtained from nhl_schedule().
* **Returns:**
  Dictionary of game data with keys - “gameId”, “plays”, “boxscore”, “header”, “broadcasts”,
  : ”videos”, “playByPlaySource”, “standings”, “leaders”, “seasonseries”, “pickcenter”, “againstTheSpread”,
    “odds”, “onIce”, “gameInfo”, “season”
* **Return type:**
  Dict

### Example

Pull a single game’s parsed feed (Stanley Cup Finals 2023 game):

```default
from sportsdataverse.nhl import espn_nhl_pbp
game = espn_nhl_pbp(game_id=401559395)
list(game.keys())  # 'gameId', 'plays', 'boxscore', ...
```

Inspect parsed plays and a quick filter on goal events:

```default
import polars as pl
plays = pl.DataFrame(game["plays"])
print(plays.shape)
goals = plays.filter(pl.col("type.text") == "Goal")
goals.select(["period", "time", "text"]).head()
```

Pull the unparsed payload for custom downstream parsing:

```default
raw = espn_nhl_pbp(game_id=401559395, raw=True)
sorted(raw.keys())[:5]
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_pbp.helper_nhl_game_data(pbp_txt, init)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp(game_id, pbp_txt)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pbp_features(game_id, pbp_txt, init)

### sportsdataverse.nhl.nhl_pbp.helper_nhl_pickcenter(pbp_txt)

### sportsdataverse.nhl.nhl_pbp.nhl_pbp_disk(game_id, path_to_json)

## sportsdataverse.nhl.nhl_records module

sportsdataverse.nhl.nhl_records — wrappers for `records.nhl.com/site/api/`.

**Documentation**:

* NHL Records endpoint reference: [https://py.sportsdataverse.org/docs/nhl/records](https://py.sportsdataverse.org/docs/nhl/records)
* Parser module: [`sportsdataverse.nhl.nhl_records_parsers`](#module-sportsdataverse.nhl.nhl_records_parsers)

Covers the most useful ~35 endpoints across awards, coaches, skaters,
goaltenders, franchises, draft, all-star, milestones, and other historical
records.  All queries support the standard NHL Records API filter kwargs:
`cayenneExp`, `factCayenneExp`, `include`, `limit`, `start`,
`sort` — pass them as keyword arguments and they are forwarded as query
parameters.

Endpoint catalog sourced from the OpenAPI spec at
`fastRhockey/data-raw/nhl_records_openapi.yaml`
(base URL: `https://records.nhl.com/site/api`).

### Conventions

* All functions return `Dict` (the raw JSON payload decoded from the
  API response).  The top-level shape is always
  `{"data": [...], "total": N}`.
* Path parameters (`franchise_id`, `id`, `season_id`, …) map to
  optional positional/keyword arguments.  Pass `None` (or omit) to get
  the list endpoint; pass a value to get the single-resource variant.
* `**filters` accepts any extra query parameters supported by the Records
  API (e.g. `cayenneExp="franchiseId=1"`, `limit=50`, `sort="points"`).

### sportsdataverse.nhl.nhl_records.nhl_records_all_time_record_vs_franchise(\*\*filters) → Dict

All-time head-to-head records between every franchise pairing.

Wraps `GET /all-time-record-vs-franchise`.

* **Parameters:**
  **\*\*filters** – Optional query parameters (e.g.
  `cayenneExp="franchiseId=1"` to scope to one franchise).
* **Returns:**
  Wins, losses, ties, OTL for every franchise-vs-franchise
  matchup since 1917.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_all_time_record_vs_franchise
h2h = nhl_records_all_time_record_vs_franchise(
    cayenneExp="franchiseId=1"
)
```

### sportsdataverse.nhl.nhl_records.nhl_records_allstar_coach_career(\*\*filters) → Dict

All-Star Game career records for coaches.

Wraps `GET /all-star-coach-career-stats`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  All-Star coaching appearances and W/L records.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_allstar_goalie_career(\*\*filters) → Dict

All-Star Game career statistics for goaltenders.

Wraps `GET /all-star-goaltender-career-stats`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Career All-Star GP, GAA, SV% per goaltender.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_allstar_goalie_game(\*\*filters) → Dict

All-Star Game single-game stats for goaltenders.

Wraps `GET /all-star-goaltender-game-stats`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Individual All-Star game stat lines per goaltender.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_allstar_skater_career(\*\*filters) → Dict

All-Star Game career statistics for skaters.

Wraps `GET /all-star-skater-career-stats`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Career All-Star GP, G, A, PTS, PIM per skater.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_allstar_skater_career
stars = nhl_records_allstar_skater_career(
    sort='[{"property":"goals","direction":"DESC"}]',
    limit=25,
)
```

### sportsdataverse.nhl.nhl_records.nhl_records_allstar_skater_game(\*\*filters) → Dict

All-Star Game single-game scoring records for skaters.

Wraps `GET /all-star-skater-game-stats`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Individual All-Star game stat lines per skater.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_attendance(\*\*filters) → Dict

NHL arena attendance records.

Wraps `GET /attendance`.

* **Parameters:**
  **\*\*filters** – Optional query parameters (e.g.
  `cayenneExp="franchiseId=1"` to scope to one franchise,
  `sort='[{"property":"attendance","direction":"DESC"}]'`).
* **Returns:**
  Per-game or per-season attendance entries.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_attendance
att = nhl_records_attendance(
    sort='[{"property":"attendance","direction":"DESC"}]',
    limit=10,
)
```

### sportsdataverse.nhl.nhl_records.nhl_records_awards(\*\*filters) → Dict

List all NHL award / trophy records.

Wraps `GET /award-details`.

* **Parameters:**
  **\*\*filters** – Optional query parameters such as `cayenneExp`,
  `include`, `limit`, `start`, `sort`.
* **Returns:**
  `{"data": [...], "total": N}` where each entry describes
  an award, its winner, the season, and the franchise.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_awards
awards = nhl_records_awards(limit=25)
print(awards["total"])
```

### sportsdataverse.nhl.nhl_records.nhl_records_awards_by_franchise(franchise_id: int, \*\*filters) → Dict

List award records for a single franchise.

Wraps `GET /award-details/{franchiseId}`.

* **Parameters:**
  * **franchise_id** (*int*) – NHL Records franchise identifier
    (e.g. `1` for NJ Devils).
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Award entries filtered to the requested franchise.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_awards_by_franchise
devils_awards = nhl_records_awards_by_franchise(1)
```

### sportsdataverse.nhl.nhl_records.nhl_records_awards_trophy_season(trophy_id: int, season_id: int, \*\*filters) → Dict

Retrieve the trophy winner for a specific season.

Wraps `GET /award-details/trophy/{trophyId}/season/{seasonId}`.

* **Parameters:**
  * **trophy_id** (*int*) – Numeric trophy identifier
    (e.g. `5` for the Hart Trophy).
  * **season_id** (*int*) – 8-digit season identifier
    (e.g. `20242025` for the 2024-25 season).
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Award entry for that trophy and season.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_awards_trophy_season
hart = nhl_records_awards_trophy_season(5, 20242025)
```

### sportsdataverse.nhl.nhl_records.nhl_records_away_team_record(\*\*filters) → Dict

League-wide away-team win/loss record by season.

Wraps `GET /away-team-record`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Away-team record aggregated by season.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_coach(coach_id: int, \*\*filters) → Dict

Retrieve one coach by their numeric ID.

Wraps `GET /coach/{id}`.

* **Parameters:**
  * **coach_id** (*int*) – NHL Records coach identifier.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Single coach record.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_coach_career(coach_id: int | None = None, \*\*filters) → Dict

Coach career-records (regular season).

Wraps `GET /coach-career-records` or
`GET /coach-career-records/{id}` when *coach_id* is supplied.

* **Parameters:**
  * **coach_id** (*int* *,* *optional*) – Restrict to a single coach.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Career wins, losses, ties, OT losses, points-pct per coach.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_coach_career
all_careers = nhl_records_coach_career(limit=100)
one_coach   = nhl_records_coach_career(coach_id=1)
```

### sportsdataverse.nhl.nhl_records.nhl_records_coach_career_with_playoffs(\*\*filters) → Dict

Coach career records inclusive of regular season + playoffs.

Wraps `GET /coach-career-records-regular-plus-playoffs`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Combined regular-season and playoff win/loss totals per coach.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_coach_franchise(coach_id: int | None = None, \*\*filters) → Dict

Coach records scoped to individual franchise stints.

Wraps `GET /coach-franchise-records` or
`GET /coach-franchise-records/{id}`.

* **Parameters:**
  * **coach_id** (*int* *,* *optional*) – Single coach.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Per-franchise-stint win/loss rows for the coach(es).
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_coach_milestone_wins(wins: int, playoffs: bool = False, \*\*filters) → Dict

Coaches who reached a wins milestone in fewest games.

Wraps one of the `/coach-fewest-games-to-{N}-wins` or
`/coach-fewest-games-to-{N}-playoff-wins` paths.

Supported *wins* values: `50, 100, 150, 200, 300, 400, 500, 600, 700,
800, 900, 1000` (regular season); `50, 100, 150` (playoffs).

* **Parameters:**
  * **wins** (*int*) – Milestone win total (e.g. `100`).
  * **playoffs** (*bool*) – If `True`, use the playoff-wins path.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Coaches who hit the milestone, sorted by games needed.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_coach_milestone_wins
fastest_100 = nhl_records_coach_milestone_wins(100)
fastest_playoff_100 = nhl_records_coach_milestone_wins(100, playoffs=True)
```

### sportsdataverse.nhl.nhl_records.nhl_records_coach_stanley_cup(\*\*filters) → Dict

Coach Stanley Cup Final win streak and consecutive-cup records.

Wraps `GET /coach-stanley-cup-streak`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Coaches with the longest Stanley Cup winning streaks.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_coaches(\*\*filters) → Dict

List NHL head coaches.

Wraps `GET /coach`.

* **Parameters:**
  **\*\*filters** – Optional query parameters (`cayenneExp`, `limit`,
  `start`, `sort`).
* **Returns:**
  `{"data": [...], "total": N}` with coach biographical
  and career-summary fields.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_coaches
coaches = nhl_records_coaches(limit=50)
```

### sportsdataverse.nhl.nhl_records.nhl_records_comeback_wins(scope: str = 'league', \*\*filters) → Dict

Comeback wins from a multi-goal deficit.

Wraps:
: * `GET /comeback-league-wins` when *scope* is `"league"`.
  * `GET /comeback-franchise-wins` when *scope* is `"franchise"`.

* **Parameters:**
  * **scope** (*str*) – `"league"` (default) or `"franchise"`.
  * **\*\*filters** – Optional query parameters (e.g.
    `cayenneExp="franchiseId=1"`).
* **Returns:**
  Games where the team overcame a deficit to win.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_consecutive_100pt_seasons(\*\*filters) → Dict

Skaters with the most consecutive 100-point seasons.

Wraps `GET /consecutive-100-point-seasons`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Skaters sorted by streak length, with season range.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_consecutive_goal_seasons(goals: int = 50, \*\*filters) → Dict

Skaters with the most consecutive N-goal seasons.

Wraps one of:
: * `GET /consecutive-20-goal-seasons`
  * `GET /consecutive-30-goal-seasons`
  * `GET /consecutive-40-goal-seasons`
  * `GET /consecutive-50-goal-seasons`
  * `GET /consecutive-60-goal-seasons`

* **Parameters:**
  * **goals** (*int*) – Goal threshold — one of `20, 30, 40, 50, 60`.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Skaters sorted by consecutive-season streak.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_consecutive_goal_seasons
streaks = nhl_records_consecutive_goal_seasons(50)
```

### sportsdataverse.nhl.nhl_records.nhl_records_draft(draft_id: int | None = None, \*\*filters) → Dict

Retrieve NHL Entry Draft picks.

Wraps `GET /draft` (all years) or `GET /draft/{id}` when
*draft_id* is supplied.

* **Parameters:**
  * **draft_id** (*int* *,* *optional*) – Draft year (e.g. `2024`).
  * **\*\*filters** – Optional query parameters (`cayenneExp`,
    `limit`, `start`, `sort`).
* **Returns:**
  Draft pick records with player, team, round, and
  overall-pick number.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_draft
picks_2024 = nhl_records_draft(2024)
first_rounders = nhl_records_draft(2024, cayenneExp="roundNumber=1")
```

### sportsdataverse.nhl.nhl_records.nhl_records_draft_by_team(team_id: int, \*\*filters) → Dict

All draft picks made by a single team.

Wraps `GET /draft/byTeam/{teamId}`.

* **Parameters:**
  * **team_id** (*int*) – NHL team identifier.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Draft picks by that franchise across all years.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_draft_lottery_odds(\*\*filters) → Dict

Draft lottery odds (current year or filtered by season).

Wraps `GET /draft-lottery-odds`.

* **Parameters:**
  **\*\*filters** – Optional query parameters (e.g.
  `cayenneExp="seasonId=20242025"`).
* **Returns:**
  Per-team draft lottery odds.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_draft_prospect(prospect_id: int | None = None, \*\*filters) → Dict

Draft prospect records.

Wraps `GET /draft-prospect` or `GET /draft-prospect/{id}`.

* **Parameters:**
  * **prospect_id** (*int* *,* *optional*) – Individual prospect.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Prospect biographical and scouting-ranking data.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_expansion_draft_picks(\*\*filters) → Dict

Expansion draft picks (e.g. Vegas 2017, Seattle 2021).

Wraps `GET /expansion-draft-picks`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Players selected in each expansion draft.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_fastest_goals(n_goals: int = 2, \*\*filters) → Dict

Fastest N goals by one team in a single game.

Wraps one of:
: * `GET /fastest-2-goals-one-team`
  * `GET /fastest-3-goals-one-team`
  * `GET /fastest-4-goals-one-team`
  * `GET /fastest-5-goals-one-team`

* **Parameters:**
  * **n_goals** (*int*) – Goal count — one of `2, 3, 4, 5`.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Games where the milestone was set, sorted by elapsed
  time (fastest first).
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_fastest_goals
fastest_3 = nhl_records_fastest_goals(3)
```

### sportsdataverse.nhl.nhl_records.nhl_records_fastest_goals_both_teams(n_goals: int = 2, \*\*filters) → Dict

Fastest N goals combined (both teams) in a single game.

Wraps one of:
: * `GET /fastest-2-goals-both-teams`
  * `GET /fastest-3-goals-both-teams`
  * `GET /fastest-4-goals-both-teams`
  * `GET /fastest-5-goals-both-teams`
  * `GET /fastest-6-goals-both-teams`

* **Parameters:**
  * **n_goals** (*int*) – Combined goal count — one of `2, 3, 4, 5, 6`.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Sorted by elapsed time (fastest first).
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_franchise_detail(\*\*filters) → Dict

Franchise detail records (extended metadata per franchise).

Wraps `GET /franchise-detail`.

* **Parameters:**
  **\*\*filters** – Optional query parameters such as
  `cayenneExp="mostRecentTeamId=1"` to scope to one franchise.
* **Returns:**
  Extended per-franchise metadata including captains, GMs,
  head coaches, and retired numbers.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_franchise_playoff_appearances(\*\*filters) → Dict

Franchise playoff appearance counts and streak information.

Wraps `GET /franchise-playoff-appearances`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Franchise playoff-appearance totals and consecutive
  appearance streaks.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_franchise_season_results(\*\*filters) → Dict

Season-by-season results for each franchise.

Wraps `GET /franchise-season-results`.

* **Parameters:**
  **\*\*filters** – Optional query parameters (e.g.
  `cayenneExp="franchiseId=1"`).
* **Returns:**
  One row per franchise-season with GP, W, L, T, OTL, PTS,
  goals for/against, and playoff seed.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_franchise_team_totals(\*\*filters) → Dict

All-time team totals per franchise (regular season).

Wraps `GET /franchise-team-totals`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Cumulative win/loss/goal/points totals for every franchise
  in regular-season play.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_franchise_team_totals
totals = nhl_records_franchise_team_totals(
    cayenneExp="franchiseId=1"
)
```

### sportsdataverse.nhl.nhl_records.nhl_records_franchise_totals(\*\*filters) → Dict

League-wide franchise totals (all-time aggregate per franchise).

Wraps `GET /franchise-totals`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  All-time wins, losses, ties, OTL, and points totals for
  every franchise (regular season and playoffs combined).
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_franchises(\*\*filters) → Dict

List all NHL franchises (historical and active).

Wraps `GET /franchise`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  `{"data": [...], "total": N}` with `franchiseId`,
  `fullName`, `mostRecentTeamId`, `firstSeasonId`,
  `lastSeasonId`, `teamCommonName`, etc.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_franchises
frx = nhl_records_franchises()
print(frx["total"])
```

### sportsdataverse.nhl.nhl_records.nhl_records_games_played_streak_skaters(active_only: bool = False, \*\*filters) → Dict

Consecutive games-played streaks for skaters.

Wraps `GET /games-played-streak-skaters` (career) or
`GET /games-played-active-streak-skaters` (currently active streaks).

* **Parameters:**
  * **active_only** (*bool*) – If `True`, return only active streaks.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Skaters sorted by streak length.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_gm_career(gm_id: int | None = None, \*\*filters) → Dict

General Manager career records.

Wraps `GET /general-manager-career-records` or
`GET /general-manager/{id}` (biography) when *gm_id* is given.

* **Parameters:**
  * **gm_id** (*int* *,* *optional*) – Restrict to a single GM.
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  Career W/L/T/OTL and points-pct for each GM’s regular-season
  tenures.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_gm_franchise(\*\*filters) → Dict

General Manager records scoped to franchise stints.

Wraps `GET /general-manager-franchise-records`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Per-franchise-stint records for every GM.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_goalie_career_stats(\*\*filters) → Dict

Goaltender career statistics (regular season).

Wraps `GET /goalie-career-stats`.

* **Parameters:**
  **\*\*filters** – Optional query parameters (`limit`, `start`,
  `sort`, `cayenneExp`).
* **Returns:**
  Career GP, W, L, T/OTL, GAA, SV%, SO per goaltender.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_goalie_career_stats
goalies = nhl_records_goalie_career_stats(
    sort='[{"property":"wins","direction":"DESC"}]',
    limit=25,
)
```

### sportsdataverse.nhl.nhl_records.nhl_records_goalie_career_stats_with_playoffs(\*\*filters) → Dict

Goaltender career stats inclusive of regular season and playoffs.

Wraps `GET /goalie_career_stats_incl_playoffs`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Combined regular-season + playoff career totals.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_goalie_playoff_streak(\*\*filters) → Dict

Goaltender consecutive playoff-win streaks.

Wraps `GET /goalie-playoff-streak`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Playoff win streaks sorted by length.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_goalie_season_stats(\*\*filters) → Dict

Goaltender single-season statistics.

Wraps `GET /goalie-season-stats`.

* **Parameters:**
  **\*\*filters** – Optional query parameters (e.g.
  `cayenneExp="seasonId=20242025"`).
* **Returns:**
  Per-goaltender per-season rows.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_goalie_shutout_streak(\*\*filters) → Dict

Goaltenders with the longest consecutive-shutout streaks.

Wraps `GET /goalie-shutout-streak`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Goaltenders sorted by streak length.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_goalie_undefeated_streak(\*\*filters) → Dict

Goaltender longest undefeated streaks (wins + ties).

Wraps `GET /goalie-undefeated-streak`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Streaks sorted descending by length.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_goalie_win_plateaus(\*\*filters) → Dict

Goaltenders who reached each win plateau (100, 200, 300 …).

Wraps `GET /goalie-win-plateaus`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Goalies listed at each plateau milestone with the date
  and game in which they reached it.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_goalie_win_streak(\*\*filters) → Dict

Goaltenders with the longest consecutive-win streaks.

Wraps `GET /goalie-win-streak`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Goaltenders sorted by streak length.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_hof_players(\*\*filters) → Dict

Hockey Hall of Fame player inductees.

Wraps `GET /hof/players`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  HOF player entries with induction year, position, and
  career summary.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_hof_players
hof = nhl_records_hof_players()
print(hof["total"])
```

### sportsdataverse.nhl.nhl_records.nhl_records_hof_players_by_office(office_id: int, \*\*filters) → Dict

Hall of Fame players for a specific induction office/category.

Wraps `GET /hof/players/{officeId}`.

* **Parameters:**
  * **office_id** (*int*) – HOF office identifier (e.g. `1` for
    Player, `2` for Builder, `3` for Referee/Linesman).
  * **\*\*filters** – Optional query parameters.
* **Returns:**
  HOF inductees in that category.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_home_team_record(\*\*filters) → Dict

League-wide home-team win/loss record by season.

Wraps `GET /home-team-record`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.
* **Returns:**
  Home-team record aggregated by season.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_skater_career_leaders(\*\*filters) → Dict

All-time skater career leaderboards.

Wraps `GET /skater-career-leaders`.

* **Parameters:**
  **\*\*filters** – Optional query parameters.  Use
  `cayenneExp="categoryType=goals"` (or `"assists"`,
  `"points"`, `"penaltyMinutes"`) to pick the leaderboard.
* **Returns:**
  Career stat leaders with rank, player name, and value.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_records.nhl_records_skater_career_stats(\*\*filters) → Dict

Skater career statistics (all-time, regular season).

Wraps `GET /goalie-career-stats` … wait — this is the **skater**
variant.  Wraps `GET /skater-career-statistics` if it exists in the
spec; falls back to the aggregate skater endpoint.

Wraps `GET /skater-career-statistics`.

* **Parameters:**
  **\*\*filters** – Optional query parameters such as
  `cayenneExp="seasonId=20242025"`,
  `sort=[{"property":"points","direction":"DESC"}]`,
  `limit=25`.
* **Returns:**
  Career GP, G, A, PTS, PIM, +/- per skater.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_records_skater_career_stats
top_scorers = nhl_records_skater_career_stats(
    sort='[{"property":"points","direction":"DESC"}]',
    limit=25,
)
```

## sportsdataverse.nhl.nhl_records_parsers module

sportsdataverse.nhl.nhl_records_parsers — polars parsers for the
NHL Records site API at `records.nhl.com/site/api/`.

**Documentation**:

* NHL Records endpoint reference: [https://py.sportsdataverse.org/docs/nhl/records](https://py.sportsdataverse.org/docs/nhl/records)
* Parsers overview: [https://py.sportsdataverse.org/docs/parsers/](https://py.sportsdataverse.org/docs/parsers/)

Every Records endpoint ships its rows under the same top-level
`{data: [...], total: N}` shape (identical to NHL Stats REST), so
a single generic parser [`parse_nhl_records()`](#sportsdataverse.nhl.nhl_records_parsers.parse_nhl_records) handles all 50
wrappers in [`sportsdataverse.nhl.nhl_records`](#module-sportsdataverse.nhl.nhl_records).

### sportsdataverse.nhl.nhl_records_parsers.parse_nhl_records(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse any NHL Records response into a tidy frame.

Every Records endpoint ships `{data: [{...}, ...], total: N}`.
This parser unwraps `data` and flattens it via
`pandas.json_normalize()`.

* **Parameters:**
  * **payload** – Raw JSON dict from any `nhl_records_*` wrapper.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas) with one row per record. Zero rows
  when the payload is missing `data` or has an empty list.

### sportsdataverse.nhl.nhl_records_parsers.parser_for_nhl_records(fn_name: str)

Return the parser for any `nhl_records_*` wrapper.

Because every Records endpoint shares the `{data: [...]}` shape,
this function always returns [`parse_nhl_records()`](#sportsdataverse.nhl.nhl_records_parsers.parse_nhl_records). The
function exists for API symmetry with
[`sportsdataverse.nhl.nhl_stats_rest_parsers.parser_for_nhl_stats_rest()`](#sportsdataverse.nhl.nhl_stats_rest_parsers.parser_for_nhl_stats_rest)
and [`sportsdataverse.mlb.mlb_api_parsers.parser_for_mlb_api()`](sportsdataverse.mlb.md#sportsdataverse.mlb.mlb_api_parsers.parser_for_mlb_api).

* **Parameters:**
  **fn_name** – The `__name__` of any `nhl_records_*` wrapper.
  Unused — all names route to the same parser.
* **Returns:**
  [`parse_nhl_records()`](#sportsdataverse.nhl.nhl_records_parsers.parse_nhl_records).

## sportsdataverse.nhl.nhl_schedule module

### sportsdataverse.nhl.nhl_schedule.espn_nhl_calendar(season=None, ondays=None, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nhl_calendar - look up the NHL calendar for a given season

* **Parameters:**
  * **season** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **ondays** (*boolean*) – Used to return dates for calendar ondays
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing calendar dates for the requested season.
* **Return type:**
  pl.DataFrame
* **Raises:**
  **ValueError** – If season is less than 2002.

### Example

Calendar dates for a season:

```default
from sportsdataverse.nhl import espn_nhl_calendar
cal = espn_nhl_calendar(season=2023)
print(cal.shape)
cal.head()
```

Just the on-days (game-played dates), useful for batch loops:

```default
ondays = espn_nhl_calendar(season=2023, ondays=True)
for url in ondays["url"].head(3).to_list():
    print(url)
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_schedule.espn_nhl_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nhl_schedule - look up the NHL schedule for a given date

* **Parameters:**
  * **dates** (*int*) – Used to define different seasons. 2002 is the earliest available season.
  * **season_type** (*int*) – season type, 1 for pre-season, 2 for regular season, 3 for post-season, 4 for all-star, 5 for off-season
  * **limit** (*int*) – number of records to return, default: 500.
  * **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing schedule dates for the requested season. Returns None if no games
* **Return type:**
  pl.DataFrame

### Example

Pull a single date’s slate (YYYYMMDD):

```default
from sportsdataverse.nhl import espn_nhl_schedule
sched = espn_nhl_schedule(dates=20230613)  # 2023 Stanley Cup Final game date
print(sched.shape)
sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()
```

Pull a regular-season slate from a season-year:

```default
reg = espn_nhl_schedule(dates=2023, season_type=2, limit=500)
reg.group_by("status_type_description").len().sort("len", descending=True)
```

Pandas round-trip for one date:

```default
espn_nhl_schedule(dates=20230613, return_as_pandas=True).head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_schedule.most_recent_nhl_season()

most_recent_nhl_season - return the season year for “today”.

NHL seasons are labeled by the year they end in. October flips the
label to next calendar year (the new season just started), otherwise
the current calendar year is returned.

* **Returns:**
  A season year suitable for season-aware loaders / schedule helpers.
* **Return type:**
  int

### Example

Use as a default season for downstream calls:

```default
from sportsdataverse.nhl import most_recent_nhl_season, espn_nhl_calendar
season = most_recent_nhl_season()
cal = espn_nhl_calendar(season=season)
print(season, cal.height)
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

### sportsdataverse.nhl.nhl_schedule.scoreboard_event_parsing(event)

### sportsdataverse.nhl.nhl_schedule.year_to_season(year)

year_to_season - format a starting year as the canonical `YYYY-YY` season string.

NHL season strings (used by `statsapi` / `api-web.nhle.com`) are of the form
`"2023-24"`. This helper converts a starting year (`2023`) into that string.

* **Parameters:**
  **year** – Starting calendar year of the season (e.g. `2023`).
* **Returns:**
  Season string formatted as `"YYYY-YY"`.
* **Return type:**
  str

### Example

Convert a starting year:

```default
from sportsdataverse.nhl import year_to_season
year_to_season(2023)  # '2023-24'
year_to_season(2009)  # '2009-10'
year_to_season(1999)  # '1999-00'
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

## sportsdataverse.nhl.nhl_stats_rest module

sportsdataverse.nhl.nhl_stats_rest — wrappers for `api.nhle.com/stats/rest/`.

**Documentation**:

* NHL Stats REST endpoint reference: [https://py.sportsdataverse.org/docs/nhl/stats-rest](https://py.sportsdataverse.org/docs/nhl/stats-rest)
* Parser module: [`sportsdataverse.nhl.nhl_stats_rest_parsers`](#module-sportsdataverse.nhl.nhl_stats_rest_parsers)

This module covers the NHL Stats REST API, which serves historical and
aggregate player/team/game statistics with Cayenne filter expressions.  It is
a **different surface** from the modern game-feed API in
[`sportsdataverse.nhl.nhl_api_web`](#module-sportsdataverse.nhl.nhl_api_web) (`api-web.nhle.com/v1/`).

Endpoint catalog sourced from the OpenAPI 3.0 spec at
`fastRhockey/data-raw/nhl_stats_rest_openapi.yaml`.

### Conventions

* **lang** defaults to `"en"` for every function.  Other locale codes
  (`"fr"`, `"es"`, etc.) may work where the API supports them.
* **Cayenne filter expressions** — the Stats REST API uses a SQL-like filter
  syntax in the `cayenneExp` query parameter, e.g.
  `cayenneExp="seasonId=20242025 and gameTypeId=2"`.  `factCayenneExp`
  applies a secondary filter on fact/aggregate columns.
* **report** endpoints (`skater`, `goalie`, `team`) accept names such as
  `"summary"`, `"advanced"`, `"powerplay"`, `"penaltykill"`, etc.
* **attribute** endpoints (`leaders/goalies`, `leaders/skaters`) accept
  stat-column names such as `"wins"`, `"savePct"`, `"goals"`, `"points"`.
* All functions return `Dict` (the raw JSON payload).  Parsing into tidy
  polars frames is a per-endpoint follow-up.
* `**filters` kwargs are passed directly as URL query parameters; `None`
  values are stripped before the request is made.

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_component_season(lang: str = 'en', \*\*kwargs) → Dict

Retrieve the component-season configuration.

Wraps `GET /{lang}/componentSeason`.

* **Parameters:**
  **lang** – Locale code.  Defaults to `"en"`.
* **Returns:**
  Component-season data object.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_config(lang: str = 'en', \*\*kwargs) → Dict

Retrieve the Stats REST API configuration payload.

Wraps `GET /{lang}/config`.  The config object describes available
report names, attribute codes, and filter expression syntax.

* **Parameters:**
  **lang** – Locale code.  Defaults to `"en"`.
* **Returns:**
  Configuration data object.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_content_module(template_key: str, lang: str = 'en', \*\*kwargs) → Dict

Retrieve a content module by template key.

Wraps `GET /{lang}/content/module/{templateKey}`.

* **Parameters:**
  * **template_key** – The template key identifying the content module.
  * **lang** – Locale code.  Defaults to `"en"`.
* **Returns:**
  Content module payload.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_content_module
mod = nhl_stats_rest_content_module(template_key="homepage")
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_country(lang: str = 'en', \*\*kwargs) → Dict

Retrieve the list of countries used in NHL data.

Wraps `GET /{lang}/country`.

* **Parameters:**
  **lang** – Locale code.  Defaults to `"en"`.
* **Returns:**
  Country list payload.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_draft(lang: str = 'en', \*\*filters) → Dict

Retrieve draft data, optionally filtered with Cayenne expressions.

Wraps `GET /{lang}/draft`.

* **Parameters:**
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  Common
    keys include `cayenneExp` (e.g.
    `cayenneExp="draftYear=2024"`), `sort`, `start`,
    `limit`, `include`, `exclude`, `factCayenneExp`,
    `isAggregate`, `isGame`, `dir`.
* **Returns:**
  Draft records object with `data` array and `total` count.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_draft
picks = nhl_stats_rest_draft(cayenneExp="draftYear=2024")
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_franchise(lang: str = 'en', \*\*filters) → Dict

Retrieve franchise data.

Wraps `GET /{lang}/franchise`.

* **Parameters:**
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters.  Common keys: `cayenneExp`,
    `sort`, `start`, `limit`, `include`, `exclude`,
    `factCayenneExp`, `isAggregate`, `isGame`, `dir`.
* **Returns:**
  Franchise records object.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_game(lang: str = 'en', \*\*filters) → Dict

Retrieve game-level data.

Wraps `GET /{lang}/game`.

* **Parameters:**
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters.  Common keys: `cayenneExp`
    (e.g. `cayenneExp="seasonId=20242025 and gameTypeId=2"`),
    `sort`, `start`, `limit`, `include`, `exclude`,
    `factCayenneExp`, `isAggregate`, `isGame`, `dir`.
* **Returns:**
  Game records object with `data` array and `total` count.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_game
games = nhl_stats_rest_game(cayenneExp="seasonId=20242025 and gameTypeId=2")
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_glossary(lang: str = 'en', \*\*kwargs) → Dict

Retrieve the NHL Stats glossary of stat definitions.

Wraps `GET /{lang}/glossary`.

* **Parameters:**
  **lang** – Locale code.  Defaults to `"en"`.
* **Returns:**
  Glossary payload mapping stat codes to human-readable
  descriptions.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_goalie_report(report: str, lang: str = 'en', \*\*filters) → Dict

Retrieve a goalie statistical report.

Wraps `GET /{lang}/goalie/{report}`.

* **Parameters:**
  * **report** – Report name.  Common values include `"summary"`,
    `"advanced"`, `"daysRest"`, `"savesByStrength"`,
    `"shootout"`, `"startRelieved"`.  Check
    [`nhl_stats_rest_config()`](#sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_config) for the full enumeration.
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  Common
    keys: `cayenneExp`, `factCayenneExp`, `include`,
    `exclude`, `sort`, `dir`, `start`, `limit`,
    `isAggregate`, `isGame`.
* **Returns:**
  Goalie report records object with `data` array and `total`.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_goalie_report
summ = nhl_stats_rest_goalie_report(
    "summary",
    cayenneExp="seasonId=20242025 and gameTypeId=2",
    sort="wins",
    limit=50,
)
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_leaders_goalies(attribute: str, lang: str = 'en', \*\*filters) → Dict

Retrieve league leaders for a goalie statistical attribute.

Wraps `GET /{lang}/leaders/goalies/{attribute}`.

* **Parameters:**
  * **attribute** – Stat attribute name (e.g. `"wins"`, `"savePct"`,
    `"goalsAgainstAverage"`, `"shutouts"`).
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.
* **Returns:**
  Goalie leaders payload.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_leaders_goalies
leaders = nhl_stats_rest_leaders_goalies("wins")
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_leaders_skaters(attribute: str, lang: str = 'en', \*\*filters) → Dict

Retrieve league leaders for a skater statistical attribute.

Wraps `GET /{lang}/leaders/skaters/{attribute}`.

* **Parameters:**
  * **attribute** – Stat attribute name (e.g. `"goals"`, `"assists"`,
    `"points"`, `"plusMinus"`).
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.
* **Returns:**
  Skater leaders payload.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_leaders_skaters
leaders = nhl_stats_rest_leaders_skaters("points")
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_milestones_goalies(lang: str = 'en', \*\*filters) → Dict

Retrieve milestone data for goalies.

Wraps `GET /{lang}/milestones/goalies`.

* **Parameters:**
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  Common
    keys: `cayenneExp`, `sort`, `start`, `limit`,
    `include`, `exclude`, `factCayenneExp`, `dir`.
* **Returns:**
  Goalie milestone records.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_milestones_skaters(lang: str = 'en', \*\*filters) → Dict

Retrieve milestone data for skaters.

Wraps `GET /{lang}/milestones/skaters`.

* **Parameters:**
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  Common
    keys: `cayenneExp`, `sort`, `start`, `limit`,
    `include`, `exclude`, `factCayenneExp`, `dir`.
* **Returns:**
  Skater milestone records.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_ping(\*\*kwargs) → Dict

Ping the NHL Stats REST API database.

Wraps `GET /ping`.  Useful as a liveness check before issuing heavier
queries.

* **Returns:**
  `{"ping": "moo"}` (or similar API liveness payload) on success,
  `{}` on failure.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_ping
print(nhl_stats_rest_ping())
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_players(lang: str = 'en', \*\*filters) → Dict

Retrieve the NHL player registry.

Wraps `GET /{lang}/players`.

* **Parameters:**
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  Common
    keys: `cayenneExp`, `sort`, `start`, `limit`,
    `include`, `exclude`, `factCayenneExp`, `dir`.
* **Returns:**
  Player records with `data` array and `total` count.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_players
players = nhl_stats_rest_players(
    cayenneExp="active=1",
    sort="lastName",
    limit=100,
)
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_season(lang: str = 'en', \*\*kwargs) → Dict

Retrieve the list of all NHL seasons.

Wraps `GET /{lang}/season`.

* **Parameters:**
  **lang** – Locale code.  Defaults to `"en"`.
* **Returns:**
  Season records with start/end dates and season ID codes.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_shiftcharts(lang: str = 'en', \*\*filters) → Dict

Retrieve shift-chart data.

Wraps `GET /{lang}/shiftcharts`.

* **Parameters:**
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  The
    primary filter is `cayenneExp` — at minimum supply a
    `gameId` constraint, e.g.
    `cayenneExp="gameId=2024020001"`.  Other common keys:
    `sort`, `start`, `limit`, `include`, `exclude`,
    `factCayenneExp`, `dir`.
* **Returns:**
  Shift records (player, team, period, start/end times).
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_shiftcharts
shifts = nhl_stats_rest_shiftcharts(cayenneExp="gameId=2024020001")
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_skater_report(report: str, lang: str = 'en', \*\*filters) → Dict

Retrieve a skater statistical report.

Wraps `GET /{lang}/skater/{report}`.

* **Parameters:**
  * **report** – Report name.  Common values include `"summary"`,
    `"advanced"`, `"powerplay"`, `"penaltykill"`,
    `"realtime"`, `"timeonice"`, `"faceoffpercentages"`,
    `"faceoffwins"`, `"goals"`, `"penalties"`,
    `"penaltyShots"`, `"points"`, `"bios"`,
    `"shootout"`, `"hits"`, `"blockedShots"`.
    Check [`nhl_stats_rest_config()`](#sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_config) for the full enumeration.
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  Common
    keys: `cayenneExp`, `factCayenneExp`, `include`,
    `exclude`, `sort`, `dir`, `start`, `limit`,
    `isAggregate`, `isGame`.
* **Returns:**
  Skater report records object with `data` array and `total`.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_skater_report
summ = nhl_stats_rest_skater_report(
    "summary",
    cayenneExp="seasonId=20242025 and gameTypeId=2",
    sort="points",
    limit=50,
)
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_team(lang: str = 'en', \*\*filters) → Dict

Retrieve the list of all NHL teams.

Wraps `GET /{lang}/team`.

* **Parameters:**
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  Common
    keys: `cayenneExp`, `sort`, `start`, `limit`,
    `include`, `exclude`, `factCayenneExp`, `dir`.
* **Returns:**
  Team records with IDs, abbreviations, and full names.
* **Return type:**
  Dict

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_team_by_id(team_id: int, lang: str = 'en', \*\*kwargs) → Dict

Retrieve a single team by its numeric ID.

Wraps `GET /{lang}/team/id/{id}`.

* **Parameters:**
  * **team_id** – NHL team integer ID (e.g. `10` for Toronto Maple Leafs).
  * **lang** – Locale code.  Defaults to `"en"`.
* **Returns:**
  Team record for the requested ID.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_team_by_id
team = nhl_stats_rest_team_by_id(10)
```

### sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_team_report(report: str, lang: str = 'en', \*\*filters) → Dict

Retrieve a team statistical report.

Wraps `GET /{lang}/team/{report}`.

* **Parameters:**
  * **report** – Report name.  Common values include `"summary"`,
    `"advanced"`, `"powerplay"`, `"penaltykill"`,
    `"realtime"`, `"timeonice"`, `"penaltiesAgainst"`,
    `"scoringFirst"`, `"leadingTrailing"`.
    Check [`nhl_stats_rest_config()`](#sportsdataverse.nhl.nhl_stats_rest.nhl_stats_rest_config) for the full enumeration.
  * **lang** – Locale code.  Defaults to `"en"`.
  * **\*\*filters** – Optional query parameters forwarded to the API.  Common
    keys: `cayenneExp`, `factCayenneExp`, `include`,
    `exclude`, `sort`, `dir`, `start`, `limit`,
    `isAggregate`, `isGame`.
* **Returns:**
  Team report records object with `data` array and `total`.
* **Return type:**
  Dict

Example:

```default
from sportsdataverse.nhl import nhl_stats_rest_team_report
pp = nhl_stats_rest_team_report(
    "powerplay",
    cayenneExp="seasonId=20242025 and gameTypeId=2",
    sort="powerPlayPct",
    limit=32,
)
```

## sportsdataverse.nhl.nhl_stats_rest_parsers module

sportsdataverse.nhl.nhl_stats_rest_parsers — polars parsers for the
NHL Stats REST API at `api.nhle.com/stats/rest/`.

**Documentation**:

* NHL Stats REST endpoint reference: [https://py.sportsdataverse.org/docs/nhl/stats-rest](https://py.sportsdataverse.org/docs/nhl/stats-rest)
* Parsers overview: [https://py.sportsdataverse.org/docs/parsers/](https://py.sportsdataverse.org/docs/parsers/)

Every Stats REST endpoint ships its rows under the same top-level
`{data: [...], total: N}` shape, so a single generic parser
[`parse_nhl_stats_rest()`](#sportsdataverse.nhl.nhl_stats_rest_parsers.parse_nhl_stats_rest) handles all 21 wrappers in
[`sportsdataverse.nhl.nhl_stats_rest`](#module-sportsdataverse.nhl.nhl_stats_rest).

The meta endpoints (`stats_rest_config`, `stats_rest_componentSeason`,
`stats_rest_ping`) return non-`data`-keyed payloads and are not in
the registry — they pass through as raw `Dict`.

### sportsdataverse.nhl.nhl_stats_rest_parsers.parse_nhl_stats_rest(payload: Dict, return_as_pandas: bool = False) → DataFrame

Parse any NHL Stats REST response into a tidy frame.

Every Stats REST endpoint ships `{data: [{...}, ...], total: N}`.
This parser unwraps `data` and flattens it via
`pandas.json_normalize()`.

* **Parameters:**
  * **payload** – Raw JSON dict from any `nhl_stats_rest_*` wrapper.
  * **return_as_pandas** – Return `pandas.DataFrame` instead of polars.
* **Returns:**
  `pl.DataFrame` (or pandas) with one row per record. Zero rows
  for meta payloads (`config`, `componentSeason`, `ping`)
  that don’t carry a `data` array.

### sportsdataverse.nhl.nhl_stats_rest_parsers.parser_for_nhl_stats_rest(fn_name: str)

Return the registered parser for an `nhl_stats_rest_*` wrapper.

Falls back to [`parse_nhl_stats_rest()`](#sportsdataverse.nhl.nhl_stats_rest_parsers.parse_nhl_stats_rest) (the generic `data`-
array flattener) for any unregistered name, so the caller always
gets a DataFrame-returning callable.

* **Parameters:**
  **fn_name** – The `__name__` of any `nhl_stats_rest_*` wrapper.
* **Returns:**
  Parser callable. Never `None`.

## sportsdataverse.nhl.nhl_teams module

### sportsdataverse.nhl.nhl_teams.espn_nhl_teams(return_as_pandas=False, \*\*kwargs) → DataFrame

espn_nhl_teams - look up NHL teams

* **Parameters:**
  **return_as_pandas** (*bool*) – If True, returns a pandas dataframe. If False, returns a polars dataframe.
* **Returns:**
  Polars dataframe containing teams for the requested league.
  This function caches by default, so if you want to refresh the data, use the command
  sportsdataverse.nhl.espn_nhl_teams.clear_cache().
* **Return type:**
  pl.DataFrame

### Example

Pull the full NHL team directory:

```default
from sportsdataverse.nhl import espn_nhl_teams
teams = espn_nhl_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()
```

Find Tampa Bay Lightning (team_id 14):

```default
import polars as pl
teams.filter(pl.col("team_id") == "14").to_dicts()
```

Refresh the cache (the call is `lru_cache`’d) and round-trip to pandas:

```default
espn_nhl_teams.cache_clear()
teams_pd = espn_nhl_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
```

See Also:
: * [fastRhockey](https://fastRhockey.sportsdataverse.org) — R companion package; mirrors this surface
  * [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — alternative Python source for the NHL stats API

## Module contents
