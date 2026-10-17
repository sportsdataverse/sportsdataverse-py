---
title: "ABA — ABA League sites (aba-liga.com, Adriatic: ABA League and ABA League 2)"
sidebar_label: "ABA League sites (aba-liga.com, Adriatic: ABA League and ABA League 2)"
description: "ABA — ABA League sites (aba-liga.com, Adriatic: ABA League and ABA League 2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# ABA — ABA League sites (aba-liga.com, Adriatic: ABA League and ABA League 2)

`sportsdataverse.basketball.aba` — 12 endpoints.

> **Note:** Unofficial use of a public source: these wrappers read the leagues' own PHP sites (www.aba-liga.com, druga.aba-liga.com), keyless, with no bot wall; payloads are not redistributed as release assets. www.aba-liga.com/robots.txt is the single line `User-agent: *` (nothing disallowed, no Crawl-delay) and druga has no robots.txt; no terms page was found. The runtime waits at least 1 s per host before every request.

> **Note:** Coverage: box scores from 2001-02; play-by-play, the starter and on-court markers, the shot chart and the minute-by-minute score from 2017-18 (earlier games answer an empty pbp and NoDataError for the helpers). A game is keyed by (game_no, season, league_id): game_no restarts at 1 for every season and league. Team and player ids are global; the calendar carries club names only. Club names are the club's CURRENT name on every season: join on team_id.

> **Note:** Only finished games are served: an unplayed game, or one whose period scores, team totals or play-by-play do not add up to the final score, raises AssetFetchError, as does a page for another game, season or league than asked. The three live-match helpers echo nothing about the game, so the runtime fetches the same game's match page first (one extra request) and checks the helper against it.

> **Note:** Times: `CET` means Central European wall-clock time including summer time (the label never changes); ABA League 2 prints `Local time`. Officials appear by name only; the player bio (birth date and place, nationality) is the public player profile.

## aba_game

Header of one finished game: round, date, venue, attendance, both clubs with their score, commissioner and referees.

**Endpoint URL:** `GET https://www.aba-liga.com/match/{game_no}/{season}/Overview`

**Valid URL:** [https://www.aba-liga.com/match/6/2026/Overview?league=aba](https://www.aba-liga.com/match/6/2026/Overview?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_no` | `game_no` |  | `Y` |  | Required. Game number within the season AND league (it restarts at 1 for every season and league), e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `season_label` | character | Season label as the page prints it ('2025/26' on the match page, 'Season 2025/26' on the calendar and standings pages). |
| `round` | character | Round / phase label as the page prints it: 'ROUND 1', 'Top8 - R2', 'Quarter-finals, Round 1', 'Finals, Round 4', 'Semifinal'. Same vocabulary as calendar games[].round. |
| `competition` | character | Competition name as printed, sponsor included and applied to every season: 'AdmiralBet ABA League' (2017-18 and 2025-26), 'Regional League' (2008-09), '2. ABA liga' (ABA League 2). |
| `group` | character | Group letter of a two-group regular season ('A', 'B'). ABSENT when the season has no groups (2008-09, 2017-18 and ABA League 2 samples). |
| `when` | character | Date-and-time line verbatim (English weekday, dd.mm.yyyy, hh:mm, time-zone label). |
| `date` | character | Game date, from the date-and-time line (the first date of a window); null for 'TBA'. |
| `date_to` | character | Last date of a 'dd.mm.yyyy - dd.mm.yyyy' window printed before a game is scheduled; null on a game with a single date. |
| `time` | character | Tip-off time 'hh:mm' in time_zone. |
| `time_zone` | character | 'CET' = Central European wall-clock time INCLUDING summer time (the label never changes; the Dubai local times prove CEST in October/June, CET in winter); 'local' = the arena's local time (ABA League 2 prints 'Local time'). |
| `local_time` | character | Arena-local tip-off 'hh:mm' when the arena is outside Central Europe ('(19:00 Dubai local time)'); null otherwise. |
| `local_place` | character | City of local_time ('Dubai', 'Cluj-Napoca'); null when the arena is in Central Europe. |
| `attendance` | integer | Spectators (the users icon). The parser sets null when the page prints none (never seen: every banked page prints it; null is part of the record contract). |
| `venue` | character | Arena name ('Venue: ...'). ABSENT on the 2008-09 sample (the page has no venue line). |
| `home_team_id` | integer | ABA team id (key space: aba-liga.com clubs, the same id in every season and in both leagues, e.g. Partizan = 22 in 2008-09 and 2025-26, Krka = 10). The first number of /team/{id}/{sez}/{lea}/0/{slug}/ and of the logo images/club/{size}/{id}.png. Joins match game.*_team_id / players / teams / pbp, standings rows and the player page. Not a player id. |
| `home_team` | character | Home club name as the site prints it NOW: a club's current name is applied to every season (2008-09 shows 'Partizan Mozzart Bet'); a club that no longer plays keeps its last name (2008-09 'Petrol Olimpija'). Join on the team id. |
| `home_city` | character | Home club city (local spelling: 'Beograd', 'Novo Mesto'). |
| `home_score` | integer | Home final score. |
| `away_team_id` | integer | ABA team id (key space: aba-liga.com clubs, the same id in every season and in both leagues, e.g. Partizan = 22 in 2008-09 and 2025-26, Krka = 10). The first number of /team/{id}/{sez}/{lea}/0/{slug}/ and of the logo images/club/{size}/{id}.png. Joins match game.*_team_id / players / teams / pbp, standings rows and the player page. Not a player id. |
| `away_team` | character | Away club name as the site prints it now (see home_team). |
| `away_city` | character | Away club city (local spelling). |
| `away_score` | integer | Away final score. |
| `commissioner` | character | Game commissioner's name ('Commissioner:' line); 2008-09 adds an IOC-style country code in parentheses ('(SRB)'). Null when not printed. |
| `referees` | character | Referees' names in page order (crew chief first), joined with '; ' (2008-09 adds an IOC-style country code in parentheses). Names only. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_game-example}

```python
aba_game(game_no=6, season=2026)
```

_Last validated n/a._

## aba_periods

Points each team scored in each quarter and overtime of one finished game.

**Endpoint URL:** `GET https://www.aba-liga.com/match/{game_no}/{season}/Overview`

**Valid URL:** [https://www.aba-liga.com/match/6/2026/Overview?league=aba](https://www.aba-liga.com/match/6/2026/Overview?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_no` | `game_no` |  | `Y` |  | Required. Game number within the season AND league (it restarts at 1 for every season and league), e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_periods-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `period` | character | 'Q1'..'Q4', then 'OT1', 'OT2' ... for overtimes. |
| `period_no` | integer | 1-based position of the period: 1-4 for Q1-Q4, then 5 for OT1, 6 for OT2. |
| `home_points` | integer | Home points scored IN the period (not cumulative). |
| `away_points` | integer | Away points scored IN the period (not cumulative). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_periods-example}

```python
aba_periods(game_no=6, season=2026)
```

_Last validated n/a._

## aba_box_players

Player box score of one finished game (2001-02 on): minutes, points, shooting splits, rebounds, assists, fouls, plus/minus and valuation.

**Endpoint URL:** `GET https://www.aba-liga.com/match/{game_no}/{season}/Overview`

**Valid URL:** [https://www.aba-liga.com/match/6/2026/Overview?league=aba](https://www.aba-liga.com/match/6/2026/Overview?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_no` | `game_no` |  | `Y` |  | Required. Game number within the season AND league (it restarts at 1 for every season and league), e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_box_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `side` | character | 'home' or 'away' (the home team is listed first everywhere on the site). |
| `team_id` | integer | ABA club id (global: the same id in every season and in both leagues, e.g. Partizan = 22 in 2008-09 and 2025-26). Joins every frame that carries it; the calendar has club names only. |
| `jersey` | integer | Jersey number worn in the game (the box lists players by jersey number). |
| `player_id` | integer | ABA player id (global: one person keeps one id across seasons and both leagues, e.g. Carlik Jones = 4807), cast to an integer. Join on it, never on the printed name. |
| `player_name` | character | Name as the box prints it: 'Surname I.' (diacritics kept). |
| `starter` | logical | In the starting five: the box marks it with '*' (matches the play-by-play 'in starting lineup' rows on every sample). Null for the whole game when no row carries a '*' (box-only seasons, 2008-09 sample). |
| `on_court_at_end` | logical | On court at the final buzzer: the box prints the name in bold (matches the substitutions in the play-by-play on every sample). Null for the whole game when no name is bold (box-only seasons). |
| `played` | logical | False on a did-not-play row (minutes '00:00' and one empty cell across the stat columns); its stat columns are null. |
| `minutes` | character | Minutes played, 'mm:ss' text. '00:00' on did-not-play rows. |
| `points` | integer | Points scored (overall Pts). Null on did-not-play rows (played = false). |
| `fg_pct` | numeric | Field-goal percentage over 2- and 3-point attempts, 0-100 with one decimal (FG2 + FG3 made / attempted); 0 when no attempt. Header 'overall %'. Null on did-not-play rows (played = false). |
| `fg2_made` | integer | 2-point field goals made (FG2 M). Null on did-not-play rows (played = false). |
| `fg2_att` | integer | 2-point field goals attempted (FG2 A). Null on did-not-play rows (played = false). |
| `fg2_pct` | numeric | 2-point percentage, 0-100 with one decimal; 0 when no attempt. Null on did-not-play rows (played = false). |
| `fg3_made` | integer | 3-point field goals made (FG3 M). Null on did-not-play rows (played = false). |
| `fg3_att` | integer | 3-point field goals attempted (FG3 A). Null on did-not-play rows (played = false). |
| `fg3_pct` | numeric | 3-point percentage, 0-100 with one decimal; 0 when no attempt. Null on did-not-play rows (played = false). |
| `ft_made` | integer | Free throws made (FT M). Null on did-not-play rows (played = false). |
| `ft_att` | integer | Free throws attempted (FT A). Null on did-not-play rows (played = false). |
| `ft_pct` | numeric | Free-throw percentage, 0-100 with one decimal; 0 when no attempt. Null on did-not-play rows (played = false). |
| `reb_def` | integer | Defensive rebounds (Rebs D). Null on did-not-play rows (played = false). |
| `reb_off` | integer | Offensive rebounds (Rebs O). Null on did-not-play rows (played = false). |
| `reb_tot` | integer | Total rebounds (Rebs T). Null on did-not-play rows (played = false). |
| `assists` | integer | Assists made (the Ass column). |
| `steals` | integer | Steals made (the St column). |
| `turnovers` | integer | Turnovers (To). Null on did-not-play rows (played = false). |
| `blocks` | integer | Shots blocked by this player or team (Blck Fv, 'in favour'). Null on did-not-play rows (played = false). |
| `blocks_against` | integer | Own shots blocked by an opponent (Blck Ag, 'against'). Null on did-not-play rows (played = false). |
| `fouls` | integer | Fouls committed (Foul Cm). Null on did-not-play rows (played = false). |
| `fouls_drawn` | integer | Fouls drawn / received (Foul Rv). Null on did-not-play rows (played = false). |
| `pts_paint` | integer | Points in the paint (Points from Paint). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `pts_second_chance` | integer | Second-chance points (Points from 2ndCh). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `pts_fast_break` | integer | Fast-break points (Points from FstBr). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `plus_minus` | integer | Plus/minus while on court (+/-). 0 on every row of the 2008-09 sample (not tracked then). Null on did-not-play rows (played = false). |
| `valuation` | integer | Valuation (Val, 'rank Value'): the efficiency rating (PIR-style index) the site ranks players by. Null on did-not-play rows (played = false). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_box_players-example}

```python
aba_box_players(game_no=6, season=2026)
```

_Last validated n/a._

## aba_box_teams

Team totals of one finished game: the Team Compare table, home then away.

**Endpoint URL:** `GET https://www.aba-liga.com/match/{game_no}/{season}/Overview`

**Valid URL:** [https://www.aba-liga.com/match/6/2026/Overview?league=aba](https://www.aba-liga.com/match/6/2026/Overview?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_no` | `game_no` |  | `Y` |  | Required. Game number within the season AND league (it restarts at 1 for every season and league), e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_box_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `side` | character | 'home' or 'away' (the home team is listed first everywhere on the site). |
| `team_id` | integer | ABA club id (global: the same id in every season and in both leagues, e.g. Partizan = 22 in 2008-09 and 2025-26). Joins every frame that carries it; the calendar has club names only. |
| `team_name` | character | Club name as the site prints it now (see game.home_team). |
| `minutes` | character | Minutes played, 'mm:ss' text. Team rows sum the five on court: '200:00', '225:00' with one overtime. |
| `points` | integer | Points scored (overall Pts). |
| `fg_pct` | numeric | Field-goal percentage over 2- and 3-point attempts, 0-100 with one decimal (FG2 + FG3 made / attempted); 0 when no attempt. Header 'overall %'. |
| `fg2_made` | integer | 2-point field goals made (FG2 M). |
| `fg2_att` | integer | 2-point field goals attempted (FG2 A). |
| `fg2_pct` | numeric | 2-point percentage, 0-100 with one decimal; 0 when no attempt. |
| `fg3_made` | integer | 3-point field goals made (FG3 M). |
| `fg3_att` | integer | 3-point field goals attempted (FG3 A). |
| `fg3_pct` | numeric | 3-point percentage, 0-100 with one decimal; 0 when no attempt. |
| `ft_made` | integer | Free throws made (FT M). |
| `ft_att` | integer | Free throws attempted (FT A). |
| `ft_pct` | numeric | Free-throw percentage, 0-100 with one decimal; 0 when no attempt. |
| `reb_def` | integer | Defensive rebounds (Rebs D). |
| `reb_off` | integer | Offensive rebounds (Rebs O). |
| `reb_tot` | integer | Total rebounds (Rebs T). Team rows equal the players' sum on every sample (team rebounds are not added). |
| `assists` | integer | Assists made (the Ass column). |
| `steals` | integer | Steals made (the St column). |
| `turnovers` | integer | Turnovers (To). |
| `blocks` | integer | Shots blocked by this player or team (Blck Fv, 'in favour'). |
| `blocks_against` | integer | Own shots blocked by an opponent (Blck Ag, 'against'). |
| `fouls` | integer | Fouls committed (Foul Cm). |
| `fouls_drawn` | integer | Fouls drawn / received (Foul Rv). |
| `pts_paint` | integer | Points in the paint (Points from Paint). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `pts_second_chance` | integer | Second-chance points (Points from 2ndCh). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `pts_fast_break` | integer | Fast-break points (Points from FstBr). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `plus_minus` | integer | Plus/minus while on court (+/-). 0 on every row of the 2008-09 sample (not tracked then). Team rows: the final margin (0 on the 2008-09 sample). |
| `valuation` | integer | Valuation (Val, 'rank Value'): the efficiency rating (PIR-style index) the site ranks players by. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_box_teams-example}

```python
aba_box_teams(game_no=6, season=2026)
```

_Last validated n/a._

## aba_pbp

Play-by-play of one finished game (2017-18 on), in game order: shots, rebounds, fouls, substitutions, timeouts and period markers.

**Endpoint URL:** `GET https://www.aba-liga.com/match/{game_no}/{season}/Overview`

**Valid URL:** [https://www.aba-liga.com/match/6/2026/Overview?league=aba](https://www.aba-liga.com/match/6/2026/Overview?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_no` | `game_no` |  | `Y` |  | Required. Game number within the season AND league (it restarts at 1 for every season and league), e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `seq` | integer | 1-based position in game order across the whole game (after the reversal). |
| `period` | integer | 1-4 = quarters, 5 = overtime (the #q5 tab; every overtime of the game). |
| `clock` | character | Clock in the period, 'mm:ss' REMAINING (counts down from 10:00; overtime from 05:00). Null on the starting-lineup rows and on game / period start and end markers. |
| `seconds_left` | integer | The clock as whole seconds REMAINING in the period (08:34 = 514), derived by the parser; null where the clock is. |
| `home_score` | integer | Home score after this event: only set on scoring events (the page prints the score only there); null otherwise. Carry it forward for a running score. |
| `away_score` | integer | Away score after this event, as home_score. |
| `side` | character | Which column the event sits in: 'home', 'away', or null for game/period markers and bare-clock rows. A timeout row gets the side of its team badge. |
| `team_id` | integer | ABA club id (global: the same id in every season and in both leagues, e.g. Partizan = 22 in 2008-09 and 2025-26). Joins every frame that carries it; the calendar has club names only. |
| `jersey` | integer | Jersey number as printed; the play-by-play omits it on some rows (all starting-lineup rows and many home rows), so it is null there. |
| `player_id` | integer | ABA player id (global: one person keeps one id across seasons and both leagues, e.g. Carlik Jones = 4807), cast to an integer. Join on it, never on the printed name. |
| `player_name` | character | Name as the play-by-play prints it: 'SURNAME I' upper case (the trailing '.' belongs to the event text on the page and is dropped). |
| `text` | character | Event text in English after the player's name: '<action> (<qualifier>)', 'in starting lineup', or the whole text of a team / game row ('(defensive)', 'Timeout', 'start of 1. quarter'). Null on bare-clock rows ('10:00' with nothing else, opening some periods). |
| `action` | character | text without its parenthesised qualifier; see the action table below the route. Null on team rows (they print only '(qualifier)') and bare-clock rows. |
| `qualifier` | character | The parenthesised part of text (shot type, rebound side, free-throw 'n of m', foul or turnover type); see the qualifier table. Null when the text has none. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_pbp-example}

```python
aba_pbp(game_no=6, season=2026)
```

_Last validated n/a._

## aba_shots

Shot chart of one finished game (2017-18 on): every field-goal attempt with its court position.

**Endpoint URL:** `GET https://www.aba-liga.com/live-match/rezultati-1718/create_shooting_chart.php`

**Valid URL:** [https://www.aba-liga.com/live-match/rezultati-1718/create_shooting_chart.php?game_no=6&season=2026&league=aba](https://www.aba-liga.com/live-match/rezultati-1718/create_shooting_chart.php?game_no=6&season=2026&league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_no` | `game_no` |  | `Y` |  | Required. Game number within the season AND league (it restarts at 1 for every season and league), e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `side` | character | Which team took the shot: home or away (the site's ekipa, a string '1' / '2'). |
| `x` | numeric | Shot x as a PERCENTAGE (0-100) of the court LENGTH from the LEFT baseline of the site's 1000x535 px full-court image. Home shoots at x < 50 and away at x > 50. The site sends it as a string in one half of a game and a number in the other; cast to float. |
| `y` | numeric | Shot y as a PERCENTAGE (0-100) of the court WIDTH, drawn upward from the bottom edge (the line through both baskets is 50). Whether 'up' is the scorer's table side is unverified. |
| `made` | logical | True when the attempt was made (the site's koordinata_uspeh '1'), false when missed. |
| `player_id` | integer | Shooter's ABA player id (global), cast from the site's string; null when the shooter was not recorded (id 0). Join to the box score on it, never on the name. |
| `player_name` | character | Shooter's full name, 'First Last' (the box prints 'Last F.'); null when not recorded. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_shots-example}

```python
aba_shots(game_no=6, season=2026)
```

_Last validated n/a._

## aba_score_flow

Score after every whole minute of one finished game (the Result Graph; 2017-18 on).

**Endpoint URL:** `GET https://www.aba-liga.com/live-match/rezultati-1718/create_json.php`

**Valid URL:** [https://www.aba-liga.com/live-match/rezultati-1718/create_json.php?game_no=6&season=2026&league=aba](https://www.aba-liga.com/live-match/rezultati-1718/create_json.php?game_no=6&season=2026&league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_no` | `game_no` |  | `Y` |  | Required. Game number within the season AND league (it restarts at 1 for every season and league), e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_score_flow-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `minute` | integer | Elapsed game minute: 0 (tip-off) to 40, then 41-45 per overtime; every minute once, in order. |
| `home_score` | integer | Home cumulative score at the end of that minute (the site sends a numeric string). The last row equals the final score. |
| `away_score` | integer | Away cumulative score at the end of that minute (the site sends a numeric string). The last row equals the final score. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_score_flow-example}

```python
aba_score_flow(game_no=6, season=2026)
```

_Last validated n/a._

## aba_score_difference

Score after every whole minute of one finished game, from the Score difference chart (2017-18 on); the same rows as aba_score_flow.

**Endpoint URL:** `GET https://www.aba-liga.com/live-match/rezultati-1718/create_scoredifference.php`

**Valid URL:** [https://www.aba-liga.com/live-match/rezultati-1718/create_scoredifference.php?game_no=6&season=2026&league=aba](https://www.aba-liga.com/live-match/rezultati-1718/create_scoredifference.php?game_no=6&season=2026&league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_no` | `game_no` |  | `Y` |  | Required. Game number within the season AND league (it restarts at 1 for every season and league), e.g. 6; ``game_no`` in aba_calendar. Playoff games that were not needed leave gaps. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_score_difference-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `minute` | integer | Elapsed game minute: 0 (tip-off) to 40, then 41-45 per overtime; every minute once, in order. |
| `home_score` | integer | Home cumulative score at the end of that minute (the site sends a numeric string). The last row equals the final score. |
| `away_score` | integer | Away cumulative score at the end of that minute (the site sends a numeric string). The last row equals the final score. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_score_difference-example}

```python
aba_score_difference(game_no=6, season=2026)
```

_Last validated n/a._

## aba_calendar

Full schedule and results of one season of one league: every round and game.

**Endpoint URL:** `GET https://www.aba-liga.com/calendar/{season}/`

**Valid URL:** [https://www.aba-liga.com/calendar/2026/?league=aba](https://www.aba-liga.com/calendar/2026/?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_calendar-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `season_label` | character | Season label as the page prints it ('2025/26' on the match page, 'Season 2025/26' on the calendar and standings pages). |
| `round` | character | Round or phase label, the accordion panel title ('ROUND 1', 'Top8 - R1', 'Quarter-finals, Round 1', 'Finals, Round 4'). |
| `group` | character | Group letter ('A', 'B') from the Group column, which exists only in seasons with groups; null after the groups (Top8, Play-out, playoffs) and where the page has no Group column. |
| `home_team` | character | Home club name as the site prints it now (see the match page's game.home_team). Null while a playoff pairing is not set (' : '): seen on the live 2026-27 page (122 of 302 games), which is NOT banked, so no committed sample shows it. |
| `away_team` | character | Away club name; null like home_team while a pairing is not set (live 2026-27 page, not banked). |
| `home_abbr` | character | Home club's three-letter code ('PAR', 'CZV'), from the mobile copy of the cell. |
| `away_abbr` | character | Away club's three-letter code. |
| `home_score` | integer | Home final score. Null for a game not played yet (empty Result cell): seen on the live 2026-27 page (282 of 302 games), which is NOT banked. |
| `away_score` | integer | Away final score; null like home_score for a game not played yet (live 2026-27 page, not banked). |
| `when` | character | Date and Time cell verbatim: 'Monday, 06.10.2025 18:00 CET', with '(19:00 Dubai local time)' for arenas outside Central Europe, 'Local time' on ABA League 2, a date window or 'TBA' before scheduling. |
| `date` | character | Game date, from the date-and-time line (the first date of a window); null for 'TBA'. |
| `date_to` | character | Last date of a 'dd.mm.yyyy - dd.mm.yyyy' window printed before a game is scheduled; null on a game with a single date. |
| `time` | character | Tip-off 'hh:mm' in time_zone. Null when only a date window or 'TBA' is printed (live 2026-27 page, NOT banked). |
| `time_zone` | character | 'CET' = Central European wall-clock time INCLUDING summer time (CET in winter, CEST in summer; the label never changes); 'local' = arena local time ('Local time', ABA League 2). Null with a date window or 'TBA' (live 2026-27 page, NOT banked). |
| `local_time` | character | Arena-local tip-off 'hh:mm' when the arena is outside Central Europe ('(19:00 Dubai local time)'); null otherwise. |
| `local_place` | character | City of local_time ('Dubai', 'Cluj-Napoca'); null when the arena is in Central Europe. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_calendar-example}

```python
aba_calendar(season=2026)
```

_Last validated n/a._

## aba_standings

Standings tables of one season of one league: regular season groups, Top 8 and Play-out.

**Endpoint URL:** `GET https://www.aba-liga.com/standings/{season}/`

**Valid URL:** [https://www.aba-liga.com/standings/2026/?league=aba](https://www.aba-liga.com/standings/2026/?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `season_label` | character | Season label as the page prints it ('2025/26' on the match page, 'Season 2025/26' on the calendar and standings pages). |
| `table` | character | Title of the table's phase heading (h2): 'Top 8 Season 2025/26', 'Play-out Season 2025/26', 'Regular Season 2025/26'. |
| `group` | character | Group heading under the phase ('Group A', 'Group B'); null when the table has no group heading. |
| `rank` | integer | Position in this table (#). |
| `team_id` | integer | ABA club id (global: the same id in every season and in both leagues, e.g. Partizan = 22 in 2008-09 and 2025-26). Joins every frame that carries it; the calendar has club names only. |
| `team_name` | character | Club name as the site prints it now (see the match page's game.home_team). |
| `games` | integer | Games played (GP). |
| `wins` | integer | Games won in this table (the W column). |
| `losses` | integer | Games lost in this table (the L column). |
| `points_for` | integer | Points scored (left side of 'Overall' 'for:against'). |
| `points_against` | integer | Points allowed (right side of 'Overall'). |
| `point_diff` | integer | Point difference (Diff), points_for - points_against. |
| `standing_points` | integer | Standings points (Points): 2 per win and 1 per loss (FIBA), e.g. 21-3 = 45. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_standings-example}

```python
aba_standings(season=2026)
```

_Last validated n/a._

## aba_player

One player's bio for one season and league: club, position, height, birth date and place, nationality and the career line.

**Endpoint URL:** `GET https://www.aba-liga.com/player/{player_id}/{season}/`

**Valid URL:** [https://www.aba-liga.com/player/4807/2026/?league=aba](https://www.aba-liga.com/player/4807/2026/?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Required. ABA player id (global: one person keeps one id across seasons and both leagues), e.g. 4807 (Carlik Jones); ``player_id`` in aba_box_players. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | ABA player id (global: one person keeps one id across seasons and both leagues, e.g. Carlik Jones = 4807), cast to an integer. Join on it, never on the printed name. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `name` | character | Full name from the h1, 'First Last' (checked against the expected name). |
| `team_id` | integer | ABA club id (global: the same id in every season and in both leagues, e.g. Partizan = 22 in 2008-09 and 2025-26). Joins every frame that carries it; the calendar has club names only. |
| `club` | character | Club name of the 'Club:' row. |
| `position` | character | Position in English as printed ('Guard' on the sample). |
| `height_cm` | integer | Height in centimetres ('185 cm'). |
| `birth_date` | character | Date of birth, ISO 'YYYY-MM-DD' (public player bio). |
| `birth_place` | character | Place of birth as printed, 'City, COUNTRY' ('Cincinnati, USA'). |
| `nationality` | character | Sporting nationality, a three-letter code as printed ('SSD' = South Sudan; IOC/FIBA-style codes). |
| `career` | character | The Clubs box, one free-text line per stint, oldest first, joined with '; ' ('2021/22 - Texas Legends; 2024-27 - Partizan Mozzart Bet'). |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_player-example}

```python
aba_player(player_id=4807, season=2026)
```

_Last validated n/a._

## aba_player_games

One player's game log for one season and league: one row per game with the box-score stats.

**Endpoint URL:** `GET https://www.aba-liga.com/player/{player_id}/{season}/`

**Valid URL:** [https://www.aba-liga.com/player/4807/2026/?league=aba](https://www.aba-liga.com/player/4807/2026/?league=aba)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Required. ABA player id (global: one person keeps one id across seasons and both leagues), e.g. 4807 (Carlik Jones); ``player_id`` in aba_box_players. |
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26). ``aba_runtime`` sends the site's START-year key ``sez = season - 2001`` (25). The ABA League's first season is 2001-02 (``season=2002``). |
| `league` | `league` |  |  | `Y` | League: ``aba`` (ABA League, www.aba-liga.com, site code lea=1) or ``aba2`` (ABA League 2, druga.aba-liga.com, lea=2). Both sites serve one database; the runtime asks the league's own host. |

### Returns {#aba_player_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | ABA player id (global: one person keeps one id across seasons and both leagues, e.g. Carlik Jones = 4807), cast to an integer. Join on it, never on the printed name. |
| `season` | integer | Season END year (2026 = 2025-26): the site's START-year key sez (25) plus 2001. |
| `league_id` | integer | League code, the site's lea: 1 = ABA League, 2 = ABA League 2. |
| `league` | character | League label: aba (ABA League, lea 1) or aba2 (ABA League 2, lea 2), the wrapper's league argument. |
| `round_no` | integer | Season-wide round index of the game: 1-18 = regular season, later phases continue the count (ABA 2025-26: 'Top8 - R2' = 21, 'Finals, Round 4' = 48). |
| `game_no` | integer | Game number, per season AND league: it restarts at 1 for every season and league, so a game's key is (game_no, season, league_id). Unplayed playoff games leave gaps. The {gameNo} of the match route. |
| `game` | character | Short pairing label 'Home-Away' with short club names ('Partizan-Krka', 'C. Zvezda-Partizan'). |
| `minutes` | character | Minutes played, 'mm:ss' text. |
| `points` | integer | Points scored (overall Pts). |
| `fg_pct` | numeric | Field-goal percentage over 2- and 3-point attempts, 0-100 with one decimal (FG2 + FG3 made / attempted); 0 when no attempt. Header 'overall %'. |
| `fg2_made` | integer | 2-point field goals made (FG2 M). |
| `fg2_att` | integer | 2-point field goals attempted (FG2 A). |
| `fg2_pct` | numeric | 2-point percentage, 0-100 with one decimal; 0 when no attempt. |
| `fg3_made` | integer | 3-point field goals made (FG3 M). |
| `fg3_att` | integer | 3-point field goals attempted (FG3 A). |
| `fg3_pct` | numeric | 3-point percentage, 0-100 with one decimal; 0 when no attempt. |
| `ft_made` | integer | Free throws made (FT M). |
| `ft_att` | integer | Free throws attempted (FT A). |
| `ft_pct` | numeric | Free-throw percentage, 0-100 with one decimal; 0 when no attempt. |
| `reb_def` | integer | Defensive rebounds (Rebs D). |
| `reb_off` | integer | Offensive rebounds (Rebs O). |
| `reb_tot` | integer | Total rebounds (Rebs T). |
| `assists` | integer | Assists made (the Ass column). |
| `steals` | integer | Steals made (the St column). |
| `turnovers` | integer | Turnovers (To). |
| `blocks` | integer | Shots blocked by this player or team (Blck Fv, 'in favour'). |
| `blocks_against` | integer | Own shots blocked by an opponent (Blck Ag, 'against'). |
| `fouls` | integer | Fouls committed (Foul Cm). |
| `fouls_drawn` | integer | Fouls drawn / received (Foul Rv). |
| `pts_paint` | integer | Points in the paint (Points from Paint). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `pts_second_chance` | integer | Second-chance points (Points from 2ndCh). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `pts_fast_break` | integer | Fast-break points (Points from FstBr). Null where the season has no such column (2008-09) and on did-not-play rows. |
| `plus_minus` | integer | Plus/minus while on court (+/-). 0 on every row of the 2008-09 sample (not tracked then). |
| `valuation` | integer | Valuation (Val, 'rank Value'): the efficiency rating (PIR-style index) the site ranks players by. |

**`return_parsed=False`** — the raw record (`dict`): the page record (the refs capture shape) or, for the three live-match helpers, `{'rows': [...]}`, plus `_request` (kind, season, sez, league and the game or player asked for).

### Example {#aba_player_games-example}

```python
aba_player_games(player_id=4807, season=2026)
```

_Last validated n/a._
