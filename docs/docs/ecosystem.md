---
title: The SportsDataverse ecosystem
sidebar_label: Ecosystem & philosophy
sidebar_position: 2
---

# The SportsDataverse ecosystem & philosophy

`sportsdataverse-py` is the Python member of the **[SportsDataverse](https://www.sportsdataverse.org)**
— a family of free, open-source packages that put clean, tidy sports data in the
hands of analysts across R, Python, and Node.js. This page explains the design
philosophy the package shares with its sister projects, the function-naming
paradigm that makes the surface predictable, and how to move between the Python
and R packages (and the wider open-source sports ecosystem) without relearning
anything.

## Philosophy

Four ideas run through every SportsDataverse package:

1. **Free and open.** The data is public; the tooling that tidies it should be
   too. Everything here is MIT-licensed and community-maintained.
2. **Tidy by default.** Raw sports APIs return deeply-nested JSON. The job of a
   SportsDataverse package is to flatten that into rectangular, analysis-ready
   tables — polars/pandas DataFrames here, tibbles in R — with stable column
   names you can build a model on.
3. **One mental model across sports _and_ languages.** Learn the pattern once and
   it transfers: the same verbs (`scoreboard`, `pbp`, `team_roster`,
   `player_gamelog`, `load_*`) mean the same thing in `nba` and `wnba` and `cfb`,
   and the name you call in Python is the name you'd call in the R sister package.
4. **Benchmarkable models.** Beyond aggregation, the project exists to make
   open-source expected-points (EP) and win-probability (WP) work — especially
   for American football — reproducible and comparable.

## Function-naming paradigm

Once you know the prefixes, you can usually guess the function name.

| Pattern | Meaning | Examples |
|---|---|---|
| `espn_<league>_<entity>()` | ESPN cross-league wrapper (same shape in all 8 leagues) | `espn_nba_scoreboard`, `espn_wnba_team_roster`, `espn_cfb_player_gamelog` |
| `<league>_<entity>()` / `<api>_<entity>()` | A league's **native** (non-ESPN) API | `nhl_pbp`, `nhl_edge_skater_detail`, `mlb_api_schedule`, `mlb_statcast` |
| `load_<league>_<dataset>(seasons=...)` | 404-safe loader of a pre-built parquet release | `load_nba_pbp`, `load_wnba_shots`, `load_cfb_betting_lines` |
| `parse_<...>()` / `parser_for_<api>()` | Raw `Dict` → tidy polars/pandas frame (+ registry lookup) | `parse_mlb_api_person_stats`, `parser_for_nhl_api_web` |

Two conventions keep the ESPN surface aligned with the R packages:

- **R-aligned vocabulary.** ESPN's raw taxonomy is normalized to the
  cfbfastR/hoopR/wehoop wording: an *athlete* is a **player**, an *event* is a
  **game**, a *competitor* is a **game team**. So you call
  `espn_nba_player_overview()` (not `athlete_overview`) and
  `espn_cfb_game_plays()` (not `event_plays`) — across every league.
- **Collision resolution (one bare name).** When two endpoints would resolve to
  the same name, one keeps the clean bare name and the other is version-qualified.
  Every league therefore has a bare `espn_<league>_player_stats()` (season stats)
  alongside the comprehensive `espn_<league>_player_stats_v3()`.

Return types are predictable: **parser-backed wrappers return a polars DataFrame
by default (0.0.54+)** — pass `return_parsed=False` for the raw `Dict`; wrappers
without a parser return the `Dict`. Use `return_as_pandas=True` to get a pandas
DataFrame, or import from the `sportsdataverse.parsed.<league>` mirror for an
explicit parsed-by-default namespace. See [Architecture](architecture/espn-cross-league.md)
and [Parsers](parsers/index.md) for the full story.

## Data releases & loaders

The `load_<league>_*()` functions skip live scraping entirely — they read
pre-built, season-partitioned parquet that the SportsDataverse data pipelines
publish on a schedule, and they are **404-safe** (a season with no published
asset is skipped with a warning rather than raising). The data comes from a small
set of companion data repositories:

- **[sportsdataverse-data](https://github.com/sportsdataverse/sportsdataverse-data/releases)**
  — the GitHub Releases host that most ESPN-derived datasets load from (NBA, WNBA,
  MBB, WBB, NHL, PWHL, …).
- **[cfbfastR-data](https://github.com/sportsdataverse/cfbfastR-data)** — college
  football play-by-play, rosters, schedules, and team info.
- **[fastRhockey-data](https://github.com/sportsdataverse/fastRhockey-data)** —
  NHL/PWHL play-by-play and box scores.
- **[nflverse-data](https://github.com/nflverse/nflverse-data)** — NFL data, read
  through the nflreadpy-style [`nfl`](nfl/index.md) module.

These mirror the R packages' own release repos
([hoopR-data](https://github.com/sportsdataverse/hoopR-data),
[wehoop-data](https://github.com/sportsdataverse/wehoop-data), …): the same
release-backed loader idea, and often the very same data.

### Automation status

Each generated-loader league's **Loaders** reference page carries an *Automation
status* table mapping every dataset to its release tag and the pipeline that
produces it, so you can see at a glance what's current and where it comes from:

- [NBA loaders](nba/reference/loaders.md) · [WNBA loaders](wnba/reference/loaders.md)
  · [MBB loaders](mbb/reference/loaders.md) · [WBB loaders](wbb/reference/loaders.md)
- [CFB loaders](cfb/reference/loaders.md) · [NHL loaders](nhl/reference/loaders.md)
  · [PWHL loaders](pwhl/reference/loaders.md)

(The NFL module loads from nflverse releases via nflreadpy, and MLB pairs the
official Stats API with Baseball Savant, so those two don't use the generated
release-loader pages above.)

## Python ↔ R: the sister packages

sdv-py deliberately mirrors the R packages' names, so a call you know in R is the
call you make in Python. Each sport's R sister:

| Sport(s) | `sportsdataverse-py` module | R sister package |
|---|---|---|
| NBA, NCAA men's basketball | [`nba`](nba/index.md), [`mbb`](mbb/index.md) | [hoopR](https://hoopR.sportsdataverse.org) |
| WNBA, NCAA women's basketball | [`wnba`](wnba/index.md), [`wbb`](wbb/index.md) | [wehoop](https://wehoop.sportsdataverse.org) |
| College football | [`cfb`](cfb/index.md) | [cfbfastR](https://cfbfastR.sportsdataverse.org) |
| NFL | [`nfl`](nfl/index.md) | [nflverse](https://nflverse.nflverse.com) (see below) |
| MLB | [`mlb`](mlb/index.md) | [baseballr](https://billpetti.github.io/baseballr/) |
| NHL, PWHL | [`nhl`](nhl/index.md), [`pwhl`](pwhl/index.md) | [fastRhockey](https://fastRhockey.sportsdataverse.org) |

For example, today's WNBA scoreboard is the same verb in both languages:

```r
# R (wehoop)
wehoop::espn_wnba_scoreboard()
```

```python
# Python (sportsdataverse-py)
from sportsdataverse.wnba import espn_wnba_scoreboard
espn_wnba_scoreboard(return_parsed=True)
```

### A 1:1 function map

A representative slice of the surface — each `sportsdataverse-py` function links to
its reference page, and each R function links to its sister-package docs. The
pattern holds well beyond these rows: ESPN wrappers, native league APIs, and
`load_*` release loaders all line up.

| `sportsdataverse-py` | R sister | What it returns |
|---|---|---|
| [`espn_nba_scoreboard`](nba/reference/site.md#espn_nba_scoreboard) | [`hoopR::espn_nba_scoreboard`](https://hoopR.sportsdataverse.org/reference/espn_nba_scoreboard.html) | NBA games + scores for a date |
| [`espn_wnba_scoreboard`](wnba/reference/site.md#espn_wnba_scoreboard) | [`wehoop::espn_wnba_scoreboard`](https://wehoop.sportsdataverse.org/reference/espn_wnba_scoreboard.html) | WNBA games + scores for a date |
| [`espn_cfb_scoreboard`](cfb/reference/site.md#espn_cfb_scoreboard) | [`cfbfastR::espn_cfb_scoreboard`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_scoreboard.html) | CFB games + scores for a week |
| [`espn_mlb_scoreboard`](mlb/reference/site.md#espn_mlb_scoreboard) | [`baseballr::espn_mlb_scoreboard`](https://billpetti.github.io/baseballr/reference/espn_mlb_scoreboard.html) | MLB games + scores for a date |
| [`espn_nba_standings`](nba/reference/site.md#espn_nba_standings) | [`hoopR::espn_nba_standings`](https://hoopR.sportsdataverse.org/reference/espn_nba_standings.html) | League standings table |
| [`espn_wnba_team_roster`](wnba/reference/site.md#espn_wnba_team_roster) | [`wehoop::espn_wnba_team_roster`](https://wehoop.sportsdataverse.org/reference/espn_wnba_team_roster.html) | A team's roster |
| [`nhl_web_pbp`](nhl/reference/nhl_api_web.md#nhl_web_pbp) | [`fastRhockey::nhl_game_pbp`](https://fastRhockey.sportsdataverse.org/reference/nhl_game_pbp.html) | NHL play-by-play for a game (api-web) |
| [`nhl_edge_skater_detail`](nhl/reference/nhl_edge.md#nhl_edge_skater_detail) | [`fastRhockey::nhl_edge_skater_detail`](https://fastRhockey.sportsdataverse.org/reference/nhl_edge_skater_detail.html) | Per-skater EDGE tracking (speed / distance / shots) |
| [`espn_nhl_teams`](nhl/reference/additional.md#espn_nhl_teams) | [`fastRhockey::espn_nhl_teams`](https://fastRhockey.sportsdataverse.org/reference/espn_nhl_teams.html) | All NHL teams (ESPN) |
| [`mlb_api_pbp`](mlb/reference/mlb_api.md#mlb_api_pbp) | [`baseballr::mlb_pbp`](https://billpetti.github.io/baseballr/reference/mlb_pbp.html) | MLB play-by-play for a game (Stats API) |
| [`mlb_api_draft`](mlb/reference/mlb_api.md#mlb_api_draft) | [`baseballr::mlb_draft`](https://billpetti.github.io/baseballr/reference/mlb_draft.html) | MLB amateur draft picks for a year |
| [`load_nba_pbp`](nba/reference/loaders.md#load_nba_pbp) | [`hoopR::load_nba_pbp`](https://hoopR.sportsdataverse.org/reference/load_nba_pbp.html) | Whole-season NBA pbp from releases |
| [`load_cfb_pbp`](cfb/reference/loaders.md#load_cfb_pbp) | [`cfbfastR::load_cfb_pbp`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_pbp.html) | Whole-season CFB pbp from releases |
| [`load_nhl_pbp`](nhl/reference/loaders.md#load_nhl_pbp) | [`fastRhockey::load_nhl_pbp`](https://fastRhockey.sportsdataverse.org/reference/load_nhl_pbp.html) | Whole-season NHL pbp from releases |

**Where they diverge:** sdv-py exposes one function per ESPN *surface*
(`espn_nba_teams_site` vs `espn_nba_season_teams`) where the R packages often
collapse them into a single function with branching internals; and sdv-py returns
polars by default rather than a data.frame/tibble.

Beyond the sport packages, the SportsDataverse spans languages and utilities:

- **R umbrella & utilities** — [sportsdataverse-R](https://r.sportsdataverse.org)
  (the meta-package that loads them all), [oddsapiR](https://oddsapiR.sportsdataverse.org)
  (betting odds), [recruitR](https://recruitR.sportsdataverse.org) (recruiting),
  and [sportyR](https://sportyR.sportsdataverse.org) (field/court/rink plots).
- **Python siblings** — [sportypy](https://sportypy.sportsdataverse.org) (the
  Python port of sportyR), [collegebaseball](https://collegebaseball.readthedocs.io),
  and recruitR-py.
- **Node.js** — [sportsdataverse.js](https://js.sportsdataverse.org).

## nflverse and the wider Python ecosystem

SportsDataverse builds on and complements two neighboring communities:

- **[nflverse](https://nflverse.nflverse.com)** — the NFL-focused open ecosystem
  ([nflfastR](https://www.nflfastr.com) and [nflreadr](https://nflreadr.nflverse.com)
  in R, [nflreadpy](https://github.com/nflverse/nflreadpy) in Python). The
  `sportsdataverse.nfl` module mirrors nflreadpy's `load_*` surface and reads the
  same nflverse parquet releases, so nflverse users can swap engines with minimal
  changes.
- **[PySport](https://opensource.pysport.org)** — the open-source sports-analytics
  community and its curated directory of Python libraries. sdv-py sits alongside
  league-specific tools you may already use — [nba_api](https://github.com/swar/nba_api),
  [pybaseball](https://github.com/jldbc/pybaseball), and
  [nhl-api-py](https://github.com/coreyjs/nhl-api-py) — and is happy to be one
  tidy layer in a larger toolbox rather than the only one.

## Where to go next

- New here? Start with the [quickstart notebook](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/01_quickstart.ipynb),
  then the per-sport intro notebook for your league:
  [CFB](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/02_cfb_intro.ipynb) ·
  [NFL](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/03_nfl_intro.ipynb) ·
  [NBA](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/04_nba_intro.ipynb) ·
  [WBB / WNBA](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/05_wbb_wnba_intro.ipynb) ·
  [MBB](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/06_mbb_intro.ipynb) ·
  [NHL](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/07_nhl_intro.ipynb).
  All seven are CI-executed weekly (`nbmake`) so they stay in sync with the API.
- Want the design details? [ESPN cross-league architecture](architecture/espn-cross-league.md)
  and the [parser layer](parsers/index.md).
- Looking for a specific function? Each league's **Reference** section lists every
  wrapper with its endpoint, parameters, and return schema.
