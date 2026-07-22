---
title: Getting Started
sidebar_label: Getting Started
sidebar_position: 1
---

# sportsdataverse-py <a href='https://py.sportsdataverse.org'><img src='https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/master/sdv-py-logo.png' align="right"  width="20%" min-width="100px" /></a>
<!-- badges: start -->

![Lifecycle:experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg?style=for-the-badge&logo=github)
[![PyPI](https://img.shields.io/pypi/v/sportsdataverse?label=sportsdataverse&logo=python&style=for-the-badge)](https://pypi.org/project/sportsdataverse/)
![Contributors](https://img.shields.io/github/contributors/sportsdataverse/sportsdataverse-py?style=for-the-badge)
[![Twitter
Follow](https://img.shields.io/twitter/follow/sportsdataverse?color=blue&label=%40sportsdataverse&logo=twitter&style=for-the-badge)](https://twitter.com/sportsdataverse)

<!-- badges: end -->


See [CHANGELOG.md](https://py.sportsdataverse.org/CHANGELOG) for details.

[sportsdataverse-py](https://py.sportsdataverse.org) gives the community
free, tidy, analysis-ready sports data in Python. It is the Python member
of the **[SportsDataverse](https://www.sportsdataverse.org)** family and
deliberately mirrors its R sisters — [hoopR](https://hoopR.sportsdataverse.org/)
(NBA/MBB), [wehoop](https://wehoop.sportsdataverse.org/) (WNBA/WBB),
[cfbfastR](https://cfbfastR.sportsdataverse.org/) (CFB),
[baseballr](https://billpetti.github.io/baseballr/) (MLB), and
[fastRhockey](https://fastRhockey.sportsdataverse.org/) (NHL/PWHL) — so the
function you know in R is the function you call in Python. The NFL module
mirrors the [nflverse](https://nflverse.nflverse.com)'s
[nflreadpy](https://github.com/nflverse/nflreadpy), and the package plays
well with the wider [PySport](https://opensource.pysport.org) ecosystem.
Beyond aggregation and tidying, the project also exists to make open-source
expected-points and win-probability models reproducible and benchmarkable,
especially for American football.

> **New here?** Read [Ecosystem & philosophy](https://py.sportsdataverse.org/docs/ecosystem)
> for the design philosophy, the full function-naming paradigm, and how the
> Python and R packages line up.

## Quickstart

```bash
pip install sportsdataverse
```

```python
# Today's NBA scoreboard as a polars DataFrame — no kwargs needed via parsed.*
from sportsdataverse.parsed.nba import espn_nba_scoreboard
df = espn_nba_scoreboard()                              # → polars

# Or via the original module with the return_parsed=True opt-in:
from sportsdataverse.nba import espn_nba_scoreboard
df = espn_nba_scoreboard(return_parsed=True)
print(df.select(["event_id", "home_name", "away_name",
                 "home_score", "away_score"]).head())

# Aaron Judge's 2024 season stats from the official MLB API
from sportsdataverse.mlb import mlb_api_person_stats, parse_mlb_api_person_stats
judge = parse_mlb_api_person_stats(
    mlb_api_person_stats(person_id=592450, stats="season", season=2024)
)
print(judge.select(["stats_group", "stat_home_runs", "stat_avg"]))

# Connor McDavid's 2024-25 EDGE skating speed profile
from sportsdataverse.nhl import nhl_edge_skater_detail, parse_edge_detail
mcdavid = parse_edge_detail(nhl_edge_skater_detail(8478402))
print(mcdavid.select(["player_first_name_default", "top_shot_speed_metric"]))
```

Parser-backed wrappers return a polars DataFrame by default (0.0.54+);
pass `return_parsed=False` for the raw `Dict`. Compose with the
matching `parse_*` function for NHL / MLB sibling APIs. See
[Polars / pandas parser layer](#polars--pandas-parser-layer) below.

## Supported leagues and data sources

| League | Module | Surfaces covered |
|---|---|---|
| NBA | `sportsdataverse.nba` | ESPN (Site v2 + Web v3 + Core v2) + stats.nba.com (`nba_stats_*`; G-League / Summer League via `league_id`) + Fox Sports |
| WNBA | `sportsdataverse.wnba` | ESPN + stats.wnba.com (`wnba_stats_*`) |
| MBB (NCAA M) | `sportsdataverse.mbb` | ESPN + NCAA-only (rankings, recruits) + stats.ncaa.org (`ncaa_mbb_*` + pbp/lineup/stint engine) + Fox Sports |
| WBB (NCAA W) | `sportsdataverse.wbb` | ESPN + NCAA-only + stats.ncaa.org (`ncaa_wbb_*`) |
| CFB | `sportsdataverse.cfb` | ESPN + NCAA + stats.ncaa.org (`cfb_ncaa_pbp` + box parsers) + football-only (QBR) + Fox Sports + Yahoo Sports |
| NFL | `sportsdataverse.nfl` | ESPN + NFL.com API + nflverse loaders (nflreadpy parity) + football-only (QBR) |
| MLB | `sportsdataverse.mlb` | ESPN + MLB Stats API (`statsapi.mlb.com`) + Baseball Savant / Statcast (`mlb_statcast_*`) + Fox Sports |
| NHL | `sportsdataverse.nhl` | `api-web.nhle.com/v1/` (game-feed) + NHL EDGE (player tracking) + Stats REST + Records site + Fox Sports |
| PWHL | `sportsdataverse.pwhl` | HockeyTech/LeagueStat (schedule / pbp / shifts / xG) |
| Minor & junior hockey | `sportsdataverse.hockey.<lg>` | HockeyTech — 20 league families (`ahl`, `echl`, `ohl`, `whl`, `qmjhl`, `ushl`, …) |
| College hockey (M/W) | `sportsdataverse.hockey.mch` / `.wch` | ESPN |
| College baseball & softball | `sportsdataverse.baseball` | ESPN + stats.ncaa.org pbp parsers |
| Soccer | `sportsdataverse.soccer` | ESPN (league-parameterized — MLS, NWSL, EPL, …) |
| Cricket | `sportsdataverse.cricket` | ESPN + bundled win-probability models |
| UFL / XFL / CFL | `sportsdataverse.football` | ESPN |
| Odds | `sportsdataverse.odds` | Odds & betting lines |

## Polars / pandas parser layer

Parser-backed wrappers return a polars DataFrame by default (0.0.54+);
pass `return_parsed=False` for the raw `Dict`. The parser layer in
[`sportsdataverse._common_espn_parsers`](https://py.sportsdataverse.org/docs/parsers/index)
(plus matching modules for the MLB and NHL sibling APIs) turns those
payloads into tidy polars (or pandas) DataFrames.

For ESPN wrappers, `return_parsed=True` is now the default for
parser-backed endpoints. Pass `return_parsed=False` to recover the
raw `Dict`, or `return_as_pandas=True` to get a pandas DataFrame:

```python
from sportsdataverse.nba import espn_nba_team_roster

df  = espn_nba_team_roster(team_id=13)                          # → polars (default)
raw = espn_nba_team_roster(team_id=13, return_parsed=False)     # → Dict
pdf = espn_nba_team_roster(team_id=13,
                            return_as_pandas=True)              # → pandas
```

For NHL / MLB sibling-API wrappers, compose the wrapper with its
parser:

```python
from sportsdataverse.nhl import nhl_web_pbp, parse_nhl_web_pbp
df = parse_nhl_web_pbp(nhl_web_pbp(2023030417))                 # 331-row polars frame
```

See the [Architecture](https://py.sportsdataverse.org/docs/architecture/espn-cross-league)
and [Parsers](https://py.sportsdataverse.org/docs/parsers/index)
pages for full details.

## Installation

sportsdataverse-py can be installed via pip:

```bash
pip install sportsdataverse
```

or from the repo (which may at times be more up to date):

```bash
git clone https://github.com/sportsdataverse/sportsdataverse-py
cd sportsdataverse-py
pip install -e .
```

# **Our Authors**

-   [Saiem Gilani](https://twitter.com/saiemgilani)
<a href="https://twitter.com/saiemgilani" target="blank"><img src="https://img.shields.io/twitter/follow/saiemgilani?color=blue&label=%40saiemgilani&logo=twitter&style=for-the-badge" alt="@saiemgilani" /></a>
<a href="https://github.com/saiemgilani" target="blank"><img src="https://img.shields.io/github/followers/saiemgilani?color=eee&logo=Github&style=for-the-badge" alt="@saiemgilani" /></a>


## **Citations**

To cite the [**`sportsdataverse-py`**](https://py.sportsdataverse.org) Python package in publications, use:

BibTex Citation
```bibtex
@misc{gilani_sdvpy_2021,
  author = {Gilani, Saiem},
  title = {sportsdataverse-py: The SportsDataverse's Python Package for Sports Data.},
  url = {https://py.sportsdataverse.org},
  season = {2021}
}
```
