<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse-py](#sportsdataverse-py)
  - [Supported leagues and data sources](#supported-leagues-and-data-sources)
  - [Polars / pandas parser layer](#polars--pandas-parser-layer)
  - [Data status](#data-status)
  - [Installation](#installation)
    - [Standard install (pip)](#standard-install-pip)
    - [Modern install (uv — recommended)](#modern-install-uv--recommended)
    - [Development install](#development-install)
    - [Notes](#notes)
    - [Errors](#errors)
  - [Examples and tutorials](#examples-and-tutorials)
  - [Using with AI agents](#using-with-ai-agents)
  - [Companion packages](#companion-packages)
  - [Follow the SportsDataverse (@SportsDataverse) on X and star this repo](#follow-the-sportsdataverse-sportsdataverse-on-x-and-star-this-repo)
  - [Our authors](#our-authors)
  - [Cheat sheet](#cheat-sheet)
  - [Citations](#citations)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse-py

<a href='https://py.sportsdataverse.org'><img src='https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/main/docs/static/img/sdv-py-logo.png' align="right" width="20%" min-width="100px" alt="sportsdataverse-py logo" /></a>
<!-- badges: start -->

![Lifecycle: beta](https://img.shields.io/badge/lifecycle-beta-blue.svg?style=for-the-badge&logo=github)
[![PyPI](https://img.shields.io/pypi/v/sportsdataverse?label=sportsdataverse&logo=python&style=for-the-badge)](https://pypi.org/project/sportsdataverse/)<a href='https://pypi.org/project/sportsdataverse/'><img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/sportsdataverse?style=for-the-badge"></a>
![Contributors](https://img.shields.io/github/contributors/sportsdataverse/sportsdataverse-py?style=for-the-badge)
[![tests](https://img.shields.io/github/actions/workflow/status/sportsdataverse/sportsdataverse-py/tests.yml?branch=main&event=push&label=tests&logo=github&style=for-the-badge)](https://github.com/sportsdataverse/sportsdataverse-py/actions/workflows/tests.yml?query=branch%3Amain)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/badges/coverage.json&style=for-the-badge)](https://github.com/sportsdataverse/sportsdataverse-py/actions/workflows/quality.yml?query=branch%3Amain)
[![coverage (all, incl. generated)](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/badges/coverage-all.json&style=for-the-badge)](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/CONTRIBUTING.md#coverage-numbers)

<!-- badges: end -->

[sportsdataverse-py](https://py.sportsdataverse.org) gives the community
free, tidy, analysis-ready sports data in Python. It is the Python member
of the **[SportsDataverse](https://www.sportsdataverse.org)** family and
deliberately mirrors its R sisters — [hoopR](https://hoopR.sportsdataverse.org/)
(NBA/MBB), [wehoop](https://wehoop.sportsdataverse.org/) (WNBA/WBB),
[cfbfastR](https://cfbfastR.sportsdataverse.org/) (CFB),
[baseballr](https://baseballr.sportsdataverse.org/) (MLB), and
[fastRhockey](https://fastRhockey.sportsdataverse.org/) (NHL/PWHL) — so the
function you know in R is the function you call in Python. The NFL module
mirrors the [nflverse](https://nflverse.nflverse.com)'s
[nflreadpy](https://github.com/nflverse/nflreadpy), and the package plays
well with the wider [PySport](https://opensource.pysport.org) ecosystem.
Beyond aggregation and tidying, the project also exists to make open-source
expected-points and win-probability models reproducible and benchmarkable,
especially for American football.

> **0.2.0 is in beta.** This README and the [docs](https://py.sportsdataverse.org) describe 0.2.0, published
> on PyPI as the pre-release `0.2.0b1`. pip skips pre-releases unless you ask, so install it with
> `pip install --pre sportsdataverse`. Release notes: [CHANGELOG](https://py.sportsdataverse.org/CHANGELOG).

## Supported leagues and data sources

One row per sport group; each links to that sport's page on the docs site, which lists every league and
provider. The [Getting Started table](https://py.sportsdataverse.org/docs/intro) maps every module to its sources.

| Sport | Leagues | Modules | Sources |
|---|---|---|---|
| [Football](https://py.sportsdataverse.org/docs/leagues/football) | CFB, NFL | `sportsdataverse.cfb`, `.nfl` | ESPN, **stats.ncaa.org** (`cfb_ncaa_*`), **NFL.com API** ("Shield"), **nflverse loaders** (nflreadpy parity), NFL Pro, PFF, Sleeper, On3 + 247Sports recruiting, Fox / Yahoo / CBS |
| | UFL, XFL, CFL | `sportsdataverse.football` | ESPN |
| [Basketball](https://py.sportsdataverse.org/docs/leagues/basketball) | NBA, WNBA, NBA G League | `sportsdataverse.nba`, `.wnba`, `.nbagl` | ESPN, **stats.nba.com** (`nba_stats_*`; `nbagl_stats_*`, 94 G League wrappers), **stats.wnba.com** (`wnba_stats_*`), Basketball-Reference, RealGM, public model datasets, Fox Sports |
| | MBB, WBB | `sportsdataverse.mbb`, `.wbb` | ESPN, **stats.ncaa.org** (`ncaa_mbb_*` / `ncaa_wbb_*`, bigballR parity), KenPom, Bart Torvik, Her Hoop Stats, Fox Sports |
| | NBA Summer League; NBL, FIBA, Olympics (ESPN) | `sportsdataverse.basketball.<home>` (`nbasl_<city>`, `nbl`, `fiba`, `olympics_mbk` / `_wbk`) | ESPN |
| | EuroLeague and 24 international families (Genius Sports, Sportradar Atrium, NBL Australia, ACB, Lega Basket, B.LEAGUE, KBL, CBA, …) | `sportsdataverse.basketball.<home>` | EuroLeague API and each league's own feed |
| [Hockey](https://py.sportsdataverse.org/docs/leagues/hockey) | NHL, PWHL | `sportsdataverse.nhl`, `.pwhl` | ESPN, NHL api-web, NHL EDGE, Stats REST, Records, Fox Sports; HockeyTech (PWHL) |
| | 19 minor & junior leagues (AHL, ECHL, OHL, WHL, QMJHL, USHL, BCHL, …) | `sportsdataverse.hockey.<lg>` | HockeyTech |
| | College hockey (M/W) | `sportsdataverse.hockey.mch`, `.wch` | ESPN |
| | SIHF and 12 European leagues (DEL, ICEHL, EIHL, Metal Ligaen, ELH, Liiga, SHL, HockeyAllsvenskan, National League, Ligue Magnus, KHL, VHL) | `sportsdataverse.hockey.<lg>` (DEL is `hockey.del_`) | Each league's own feed |
| [Baseball](https://py.sportsdataverse.org/docs/leagues/baseball) | MLB; college baseball & softball | `sportsdataverse.mlb`, `.baseball` | ESPN, **MLB Stats API**, **Baseball Savant / Statcast** (`mlb_statcast_*`), stats.ncaa.org, Fox Sports |
| [Soccer](https://py.sportsdataverse.org/docs/leagues/soccer) | 12 leagues (EPL, LaLiga, Bundesliga, Serie A, Ligue 1, MLS, Liga MX, UCL, UEL, NWSL, WWC, WC) | `sportsdataverse.soccer` | ESPN, ASA, FotMob, UEFA, FIFA, MLS and NWSL APIs, Football-Data.co.uk, OpenLigaDB, kloppy open event data |
| [More sports & sources](https://py.sportsdataverse.org/docs/leagues/other) | Cricket | `sportsdataverse.cricket` | ESPN + bundled win-probability models |
| | Formula 1 | `sportsdataverse.f1` | Jolpica |
| | Odds & prediction markets | `sportsdataverse.odds` | The Odds API, Kalshi, Polymarket |
| | Cross-sport providers | `sportsdataverse.providers.<name>` (`cbs`, `fox`, `yahoo`, `espn_content`, `thesportsdb`) | CBS Sports, Fox Sports, Yahoo Sports, ESPN content, TheSportsDB |

The big league modules export roughly 270–740 public names each (ESPN wrappers, that league's
native-API wrappers, dataset loaders and parsers), and the flat `sportsdataverse` namespace holds
about 9,300 public functions. **Fox Sports** adds `fox_<league>_*` Bifrost wrappers (pbp /
boxscore / odds / roster / stats / standings / leaders) for 8 leagues plus the generated `fox_api`
family (33 endpoints). **Yahoo Sports** adds 107 multi-sport `yahoo_*` functions from the generated
`yahoo_shangrila` family and keeps the legacy `yahoo_cfb_*` season-stats and scoreboard wrappers
for college football. **CBS Sports** adds 82 `cbs_napi_*` wrappers. `sportsdataverse.release` ports
the `sportsdataversedata` R release utilities (GitHub-release asset publish / download helpers,
including a pure-Python RDS writer).

## Polars / pandas parser layer

Parser-backed wrappers return a tidy polars DataFrame **by default**.
Pass `return_parsed=False` for the raw `Dict`, or
`return_as_pandas=True` for pandas. Wrappers without a registered
parser return the raw `Dict`.

```python
from sportsdataverse.nba import espn_nba_team_roster

df  = espn_nba_team_roster(team_id=13)                          # → polars (default)
raw = espn_nba_team_roster(team_id=13, return_parsed=False)     # → Dict
pdf = espn_nba_team_roster(team_id=13,
                            return_as_pandas=True)              # → pandas
```

For the NHL and MLB sibling-API wrappers, compose the wrapper with
its parser:

```python
from sportsdataverse.nhl import nhl_web_pbp, parse_nhl_web_pbp
df = parse_nhl_web_pbp(nhl_web_pbp(2023030417))                 # 331-row polars frame
```

See [py.sportsdataverse.org/docs/architecture/espn-cross-league](https://py.sportsdataverse.org/docs/architecture/espn-cross-league)
and [py.sportsdataverse.org/docs/parsers](https://py.sportsdataverse.org/docs/parsers/)
for the full architecture + parser registry.

## Data status

The `load_*()` functions read release assets on [sportsdataverse-data](https://github.com/sportsdataverse/sportsdataverse-data/releases); most `load_nfl_*()` loaders read the nflverse-data releases instead. The badges below are rebuilt nightly from each producer's latest workflow run and newest release files; idle means the sport is out of season. Full detail: [sportsdataverse.org/status](https://sportsdataverse.org/status).

| Dataset | Data updated | Through | Pipeline | Update workflows |
|:--|:--|:--|:--|:--|
| [College football (ESPN)](https://github.com/sportsdataverse/cfbfastR-cfb-data) | [![cfbfastR-cfb-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![cfbfastR-cfb-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![cfbfastR-cfb-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fstatus.json)](https://sportsdataverse.org/status#cfbfastR-cfb-data) | [![cfbfastR-cfb-data daily_cfb](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fwf-daily_cfb.json)](https://github.com/sportsdataverse/cfbfastR-cfb-data/actions/workflows/daily_cfb.yml) [![cfbfastR-cfb-data cfb_ratings_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fwf-cfb_ratings_cron.json)](https://github.com/sportsdataverse/cfbfastR-cfb-data/actions/workflows/cfb_ratings_cron.yml) [![cfbfastR-cfb-data cfb_fpi_weekly](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fwf-cfb_fpi_weekly.json)](https://github.com/sportsdataverse/cfbfastR-cfb-data/actions/workflows/cfb_fpi_weekly.yml) [![cfbfastR-cfb-data cfb_recruiting_proj_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fwf-cfb_recruiting_proj_cron.json)](https://github.com/sportsdataverse/cfbfastR-cfb-data/actions/workflows/cfb_recruiting_proj_cron.yml) [![cfbfastR-cfb-data cfb_model_pipeline](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fwf-cfb_model_pipeline.json)](https://github.com/sportsdataverse/cfbfastR-cfb-data/actions/workflows/cfb_model_pipeline.yml) [![cfbfastR-cfb-data espn_daily_snapshots](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-cfb-data%2Fwf-espn_daily_snapshots.json)](https://github.com/sportsdataverse/cfbfastR-cfb-data/actions/workflows/espn_daily_snapshots.yml) |
| [College football play-by-play (CollegeFootballData)](https://github.com/sportsdataverse/cfbfastR-data) | [![cfbfastR-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![cfbfastR-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![cfbfastR-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-data%2Fstatus.json)](https://sportsdataverse.org/status#cfbfastR-data) | [![cfbfastR-data daily_cfb](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FcfbfastR-data%2Fwf-daily_cfb.json)](https://github.com/sportsdataverse/cfbfastR-data/actions/workflows/daily_cfb.yml) |
| [College football (stats.ncaa.org)](https://github.com/sportsdataverse/ncaa-mfb-football-data) | [![ncaa-mfb-football-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-mfb-football-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![ncaa-mfb-football-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-mfb-football-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![ncaa-mfb-football-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-mfb-football-data%2Fstatus.json)](https://sportsdataverse.org/status#ncaa-mfb-football-data) | [![ncaa-mfb-football-data daily_ncaa_mfb_data](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-mfb-football-data%2Fwf-daily_ncaa_mfb_data.json)](https://github.com/sportsdataverse/ncaa-mfb-football-data/actions/workflows/daily_ncaa_mfb_data.yml) |
| [NFL (ESPN)](https://github.com/sportsdataverse/nfl-data) | [![nfl-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![nfl-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![nfl-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-data%2Fstatus.json)](https://sportsdataverse.org/status#nfl-data) | [![nfl-data espn_nfl_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-data%2Fwf-espn_nfl_cron.json)](https://github.com/sportsdataverse/nfl-data/actions/workflows/espn_nfl_cron.yml) [![nfl-data nfl_pbp_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-data%2Fwf-nfl_pbp_cron.json)](https://github.com/sportsdataverse/nfl-data/actions/workflows/nfl_pbp_cron.yml) [![nfl-data nfl_ratings_weekly](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-data%2Fwf-nfl_ratings_weekly.json)](https://github.com/sportsdataverse/nfl-data/actions/workflows/nfl_ratings_weekly.yml) [![nfl-data nfl_rosters_players_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-data%2Fwf-nfl_rosters_players_cron.json)](https://github.com/sportsdataverse/nfl-data/actions/workflows/nfl_rosters_players_cron.yml) [![nfl-data nfl_model_pipeline](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-data%2Fwf-nfl_model_pipeline.json)](https://github.com/sportsdataverse/nfl-data/actions/workflows/nfl_model_pipeline.yml) |
| [NFL Next Gen Stats](https://github.com/sportsdataverse/nfl-ngs-data) | [![nfl-ngs-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-ngs-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![nfl-ngs-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-ngs-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![nfl-ngs-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-ngs-data%2Fstatus.json)](https://sportsdataverse.org/status#nfl-ngs-data) | [![nfl-ngs-data daily_ngs](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fnfl-ngs-data%2Fwf-daily_ngs.json)](https://github.com/sportsdataverse/nfl-ngs-data/actions/workflows/daily_ngs.yml) |
| [Men's college basketball (ESPN)](https://github.com/sportsdataverse/hoopR-mbb-data) | [![hoopR-mbb-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-mbb-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![hoopR-mbb-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-mbb-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![hoopR-mbb-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-mbb-data%2Fstatus.json)](https://sportsdataverse.org/status#hoopR-mbb-data) | [![hoopR-mbb-data daily_mbb](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-mbb-data%2Fwf-daily_mbb.json)](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/daily_mbb.yml) [![hoopR-mbb-data mbb_models_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-mbb-data%2Fwf-mbb_models_cron.json)](https://github.com/sportsdataverse/hoopR-mbb-data/actions/workflows/mbb_models_cron.yml) |
| [Men's college basketball (stats.ncaa.org)](https://github.com/sportsdataverse/ncaa-mbb-hoops-data) | [![ncaa-mbb-hoops-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-mbb-hoops-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![ncaa-mbb-hoops-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-mbb-hoops-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![ncaa-mbb-hoops-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-mbb-hoops-data%2Fstatus.json)](https://sportsdataverse.org/status#ncaa-mbb-hoops-data) | [![ncaa-mbb-hoops-data ncaa_mbb_models](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-mbb-hoops-data%2Fwf-ncaa_mbb_models.json)](https://github.com/sportsdataverse/ncaa-mbb-hoops-data/actions/workflows/ncaa_mbb_models.yml) |
| [NBA (ESPN)](https://github.com/sportsdataverse/hoopR-nba-data) | [![hoopR-nba-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![hoopR-nba-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![hoopR-nba-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-data%2Fstatus.json)](https://sportsdataverse.org/status#hoopR-nba-data) | [![hoopR-nba-data daily_nba](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-data%2Fwf-daily_nba.json)](https://github.com/sportsdataverse/hoopR-nba-data/actions/workflows/daily_nba.yml) |
| [NBA Stats API](https://github.com/sportsdataverse/hoopR-nba-stats-data) | [![hoopR-nba-stats-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-stats-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![hoopR-nba-stats-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-stats-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![hoopR-nba-stats-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-stats-data%2Fstatus.json)](https://sportsdataverse.org/status#hoopR-nba-stats-data) | [![hoopR-nba-stats-data daily_nba_stats](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-stats-data%2Fwf-daily_nba_stats.json)](https://github.com/sportsdataverse/hoopR-nba-stats-data/actions/workflows/daily_nba_stats.yml) [![hoopR-nba-stats-data nba_models](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-stats-data%2Fwf-nba_models.json)](https://github.com/sportsdataverse/hoopR-nba-stats-data/actions/workflows/nba_models.yml) [![hoopR-nba-stats-data annual_nba_stats_draft](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FhoopR-nba-stats-data%2Fwf-annual_nba_stats_draft.json)](https://github.com/sportsdataverse/hoopR-nba-stats-data/actions/workflows/annual_nba_stats_draft.yml) |
| [Women's college basketball (ESPN)](https://github.com/sportsdataverse/wehoop-wbb-data) | [![wehoop-wbb-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wbb-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![wehoop-wbb-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wbb-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![wehoop-wbb-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wbb-data%2Fstatus.json)](https://sportsdataverse.org/status#wehoop-wbb-data) | [![wehoop-wbb-data daily_wbb](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wbb-data%2Fwf-daily_wbb.json)](https://github.com/sportsdataverse/wehoop-wbb-data/actions/workflows/daily_wbb.yml) [![wehoop-wbb-data weekly_wbb](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wbb-data%2Fwf-weekly_wbb.json)](https://github.com/sportsdataverse/wehoop-wbb-data/actions/workflows/weekly_wbb.yml) [![wehoop-wbb-data wbb_models_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wbb-data%2Fwf-wbb_models_cron.json)](https://github.com/sportsdataverse/wehoop-wbb-data/actions/workflows/wbb_models_cron.yml) |
| [Women's college basketball (stats.ncaa.org)](https://github.com/sportsdataverse/ncaa-wbb-hoops-data) | [![ncaa-wbb-hoops-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-wbb-hoops-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![ncaa-wbb-hoops-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-wbb-hoops-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![ncaa-wbb-hoops-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-wbb-hoops-data%2Fstatus.json)](https://sportsdataverse.org/status#ncaa-wbb-hoops-data) | [![ncaa-wbb-hoops-data ncaa_wbb_models](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fncaa-wbb-hoops-data%2Fwf-ncaa_wbb_models.json)](https://github.com/sportsdataverse/ncaa-wbb-hoops-data/actions/workflows/ncaa_wbb_models.yml) |
| [WNBA (ESPN)](https://github.com/sportsdataverse/wehoop-wnba-data) | [![wehoop-wnba-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![wehoop-wnba-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![wehoop-wnba-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-data%2Fstatus.json)](https://sportsdataverse.org/status#wehoop-wnba-data) | [![wehoop-wnba-data daily_wnba](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-data%2Fwf-daily_wnba.json)](https://github.com/sportsdataverse/wehoop-wnba-data/actions/workflows/daily_wnba.yml) [![wehoop-wnba-data weekly_wnba](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-data%2Fwf-weekly_wnba.json)](https://github.com/sportsdataverse/wehoop-wnba-data/actions/workflows/weekly_wnba.yml) [![wehoop-wnba-data annual_wnba_draft](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-data%2Fwf-annual_wnba_draft.json)](https://github.com/sportsdataverse/wehoop-wnba-data/actions/workflows/annual_wnba_draft.yml) |
| [WNBA Stats API](https://github.com/sportsdataverse/wehoop-wnba-stats-data) | [![wehoop-wnba-stats-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-stats-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![wehoop-wnba-stats-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-stats-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![wehoop-wnba-stats-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-stats-data%2Fstatus.json)](https://sportsdataverse.org/status#wehoop-wnba-stats-data) | [![wehoop-wnba-stats-data daily_wnba_stats](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-stats-data%2Fwf-daily_wnba_stats.json)](https://github.com/sportsdataverse/wehoop-wnba-stats-data/actions/workflows/daily_wnba_stats.yml) [![wehoop-wnba-stats-data wnba_models](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-stats-data%2Fwf-wnba_models.json)](https://github.com/sportsdataverse/wehoop-wnba-stats-data/actions/workflows/wnba_models.yml) [![wehoop-wnba-stats-data annual_wnba_stats_draft](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fwehoop-wnba-stats-data%2Fwf-annual_wnba_stats_draft.json)](https://github.com/sportsdataverse/wehoop-wnba-stats-data/actions/workflows/annual_wnba_stats_draft.yml) |
| [NHL](https://github.com/sportsdataverse/fastRhockey-nhl-data) | [![fastRhockey-nhl-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-nhl-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![fastRhockey-nhl-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-nhl-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![fastRhockey-nhl-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-nhl-data%2Fstatus.json)](https://sportsdataverse.org/status#fastRhockey-nhl-data) | [![fastRhockey-nhl-data daily_nhl_python](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-nhl-data%2Fwf-daily_nhl_python.json)](https://github.com/sportsdataverse/fastRhockey-nhl-data/actions/workflows/daily_nhl_python.yml) [![fastRhockey-nhl-data nhl_model_pipeline](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-nhl-data%2Fwf-nhl_model_pipeline.json)](https://github.com/sportsdataverse/fastRhockey-nhl-data/actions/workflows/nhl_model_pipeline.yml) |
| [PWHL](https://github.com/sportsdataverse/fastRhockey-pwhl-data) | [![fastRhockey-pwhl-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-pwhl-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![fastRhockey-pwhl-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-pwhl-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![fastRhockey-pwhl-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-pwhl-data%2Fstatus.json)](https://sportsdataverse.org/status#fastRhockey-pwhl-data) | [![fastRhockey-pwhl-data daily_pwhl_python](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-pwhl-data%2Fwf-daily_pwhl_python.json)](https://github.com/sportsdataverse/fastRhockey-pwhl-data/actions/workflows/daily_pwhl_python.yml) [![fastRhockey-pwhl-data pwhl_xg_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2FfastRhockey-pwhl-data%2Fwf-pwhl_xg_cron.json)](https://github.com/sportsdataverse/fastRhockey-pwhl-data/actions/workflows/pwhl_xg_cron.yml) |
| [MLB and college baseball](https://github.com/sportsdataverse/baseballr-data) | [![baseballr-data data updated](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fbaseballr-data%2Fupdated.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![baseballr-data through season](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fbaseballr-data%2Fthrough.json)](https://github.com/sportsdataverse/sportsdataverse-data/releases) | [![baseballr-data pipeline state](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fbaseballr-data%2Fstatus.json)](https://sportsdataverse.org/status#baseballr-data) | [![baseballr-data mlb_models_cron](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fbaseballr-data%2Fwf-mlb_models_cron.json)](https://github.com/sportsdataverse/baseballr-data/actions/workflows/mlb_models_cron.yml) [![baseballr-data daily_ncaa_baseball](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsportsdataverse%2F.github%2Fmain%2Fstatus%2Fbadges%2Fbaseballr-data%2Fwf-daily_ncaa_baseball.json)](https://github.com/sportsdataverse/baseballr-data/actions/workflows/daily_ncaa_baseball.yml) |

## Installation

The package metadata lives entirely in [`pyproject.toml`](pyproject.toml)
(PEP 621 `[project]` table). There is no `setup.py`. sportsdataverse needs Python 3.10+
(3.10–3.14 are supported).

### Standard install (pip)

0.2.0 ships as the PyPI pre-release `0.2.0b1`, so pass `--pre`; without it pip installs the
previous stable release (0.1.4), which this README does not describe.

```bash
pip install --pre sportsdataverse
```

With optional extras (defined in `[project.optional-dependencies]` in
`pyproject.toml`):

```bash
pip install --pre "sportsdataverse[all]"      # everything below EXCEPT mcp
pip install --pre "sportsdataverse[models]"   # no-op (kept so sportsdataverse[models] installs keep working)
pip install --pre "sportsdataverse[tests]"    # adds pytest, mypy, ruff, etc.
pip install --pre "sportsdataverse[nflpro]"   # NFL Pro (pro.nfl.com) Next Gen Stats
pip install --pre "sportsdataverse[pff]"      # Playwright, for the optional PFF Premium (LEGACY) headless login; the PFF Developer API needs no extra
pip install --pre "sportsdataverse[mcp]"      # the sdv-docs MCP server
pip install --pre "sportsdataverse[soccer]"   # kloppy: soccer event / tracking data (soccer_open_events)
pip install --pre "sportsdataverse[sdvplot]"  # sdvplot: team logos, colors and chart themes
pip install --pre "sportsdataverse[sportypy]" # sportypy: draw pbp / shot coordinates on rulebook fields, rinks, courts (GPL-3.0)
```

### Modern install (uv — recommended)

[uv](https://docs.astral.sh/uv/) is the fast, drop-in package manager we use day to day. A
version specifier that names a pre-release lets uv pick the beta:

```bash
# Add to a uv-managed project:
uv add "sportsdataverse>=0.2.0b1"

# With extras:
uv add "sportsdataverse[all]>=0.2.0b1"

# Or install the latest dev snapshot from GitHub:
uv add "sportsdataverse @ git+https://github.com/sportsdataverse/sportsdataverse-py"
```

### Development install

For contributing or running the test suite:

```bash
git clone https://github.com/sportsdataverse/sportsdataverse-py.git
cd sportsdataverse-py

# uv (recommended): every extra plus the dev dependency groups, editable
uv sync --all-extras --dev

# Plain pip works too if uv isn't available (every extra except mcp, no dev groups):
pip install -e ".[all]"
```

> Note: dev dependencies (`test`, `lint`, `dev`) live in a PEP 735 `[dependency-groups]` block
> beside the PEP 621 `[project.optional-dependencies]` extras.
> CI runs `uv sync --all-extras --all-groups`, which installs both in one step.

Run the test suite:

```bash
uv run pytest                       # offline tests only
SDV_PY_LIVE_TESTS=1 uv run pytest   # include live API tests (slower; hits ESPN / nflverse)
```

For deeper dev-environment detail (lint, mypy, dep-bumping workflow), see
[CONTRIBUTING.md](CONTRIBUTING.md).

### Notes

- **DataFrame engine:** polars 2.x (`polars>=2.0,<3`). Most loaders accept `return_as_pandas=True`
  if you prefer pandas.
- **NFL caching:** loaders cache to memory by default. Set
  `SDV_PY_NFL_CACHE=filesystem` for cross-session reuse, or
  `SDV_PY_NFL_CACHE=off` to disable. See
  `sportsdataverse.nfl.config.update_config()` for runtime control.
- **stats.nba.com / stats.wnba.com surface (`nba_stats_*` / `wnba_stats_*` / `nbagl_stats_*`):**
  128 NBA (+ Summer League via `league_id`), 111 WNBA wrappers and 94 G League
  wrappers (`sportsdataverse.nbagl.nbagl_stats`, `LeagueID=20` by default) are
  available: the endpoints a capture sweep confirmed **live and not deprecated**.
  The generic parser also handles the family's non-uniform shapes, such as the
  shot-location endpoints' grouped (2-level) headers and the `scoreboardv3` game
  feed. Live calls to `stats.nba.com` require the `curl_cffi` package (TLS
  fingerprint protection); install it via `pip install --pre "sportsdataverse[all]"`
  or `pip install curl_cffi` separately.

### Errors

Three outcomes, never collapsed into one:

- `NoDataError` — the fetch SUCCEEDED and there is nothing there (a 404, or ESPN's
  200-with-`code:404` body). Skip the season.
- `AssetFetchError` — the fetch FAILED and the answer is unknown (403, rate limit,
  exhausted retries). Surface it; do not record it as an empty season.
- `ValueError` — a 400 / 422: the request itself is wrong and retrying cannot help.

From 0.2.0 the hand-written ESPN scrapers and every generated flat-API getter
**raise** instead of returning an error body or an empty dict.

## Examples and tutorials

Every public function ships a runnable `Example:` block in its docstring
showing a quick-start call, common parameter combinations, and a one-line
pipeline next-step. Regenerate the API reference locally with
`uv run python tools/codegen/generate.py --docs` (then `cd docs && yarn build`
to preview the Docusaurus site) or browse the live docs at
[py.sportsdataverse.org](https://py.sportsdataverse.org).

For longer-form walkthroughs, see the intro/intermediate Jupyter notebooks
under [`examples/notebooks/`](examples/notebooks):

| Notebook | Covers |
|---|---|
| `01_quickstart.ipynb` | Cross-sport intro — package layout, polars vs pandas, the `download()` retry layer |
| `02_cfb_intro.ipynb` | College football PBP, schedule, teams, `espn_cfb_play_participants` |
| `03_nfl_intro.ipynb` | NFL — nflreadpy parity surface, caching layer, current-season helpers |
| `04_nba_intro.ipynb` | NBA — PBP, schedule, teams, game rosters, shot distribution |
| `05_wbb_intro.ipynb` | Women's college basketball — PBP, schedule, multi-table player stats |
| `06_mbb_intro.ipynb` | Men's college basketball — PBP, schedule, conference standings |
| `07_nhl_intro.ipynb` | NHL — PBP, schedule, teams, shot-event filter |
| `08_wnba_intro.ipynb` | WNBA — PBP, schedule, rosters, player stats |
| `09_mlb_intro.ipynb` | MLB — Stats API + Statcast search / leaderboards / gamefeed |
| `10_pwhl_intro.ipynb` | PWHL — HockeyTech schedule, PBP, shifts, xG |
| `11_junior_hockey_intro.ipynb` | HockeyTech minor/junior leagues — one family shape across 19 leagues |
| `12_odds_intro.ipynb` | Odds & betting lines |
| `13_soccer_intro.ipynb` | ESPN soccer — league-parameterized wrappers |
| `14_cricket_intro.ipynb` | ESPN cricket + win-probability models |
| `15_other_espn_leagues_intro.ipynb` | UFL/XFL/CFL, college hockey, college baseball/softball ESPN families |
| `16_international_basketball_intro.ipynb` | EuroLeague, Genius Sports (SLB), NBL Australia, ACB; a EuroLeague shot chart on a FIBA court (sdvplot) |
| `17_european_college_hockey_intro.ipynb` | SHL, Liiga, DEL, KHL and NCAA hockey; a DEL shot map, college team colors (sdvplot) |
| `18_prediction_markets_intro.ipynb` | Kalshi and Polymarket market data: events, markets, order books |
| `19_more_sources_intro.ipynb` | NBA G League (ESPN), Formula 1, TheSportsDB, ESPN content |

## Using with AI agents

- **[Context7](https://context7.com)** indexes these docs (`context7.json` scopes it to `docs/docs/`) and the SDV R packages (from their pkgdown `llms.txt`).
- **llms.txt:** <https://py.sportsdataverse.org/llms.txt> links a Markdown copy of every docs page.
- **sdv-docs MCP server:** exact answers about returned columns, Python and R functions, provider API endpoints (ESPN, stats.nba.com, NHL, MLB, …) and released datasets.

  ```bash
  claude mcp add sdv-docs -- uvx --from 'sportsdataverse[mcp]>=0.2.0b1' sdv-docs
  ```

  Needs 0.2.0b1 or newer (`pip install --pre "sportsdataverse[mcp]"`). The
  server ships in the same wheel but never imports `sportsdataverse`, so it starts
  in about 1.5 s instead of the 4-12 s a full league import costs.

  The server downloads its index (`sdv_docs_v1.sqlite.gz`, about 10 MB) from the [`docs-index` release](https://github.com/sportsdataverse/sportsdataverse-py/releases/tag/docs-index), checking at most once a day, and sends no queries anywhere. Set `SDV_DOCS_DB=/path/to/sdv_docs_v1.sqlite` to use a local build (`uv run python tools/codegen/build_docs_index.py`). `sdv-docs --version` prints the installed version.

## Companion packages

`sportsdataverse-py` is one corner of the broader [SportsDataverse](https://www.sportsdataverse.org)
ecosystem. The R sister packages cover the same data sources with deeper
sport-specific coverage:

- [wehoop](https://wehoop.sportsdataverse.org) — women's basketball (WNBA + NCAA)
- [hoopR](https://hoopR.sportsdataverse.org) — men's basketball (NBA + NCAA)
- [cfbfastR](https://cfbfastR.sportsdataverse.org) — college football
- [baseballr](https://baseballr.sportsdataverse.org) — baseball (MLB + MiLB + NCAA)
- [fastRhockey](https://fastRhockey.sportsdataverse.org) — hockey (NHL + PWHL)

For plots and simulation: [sdvplot](https://sdvplot.sportsdataverse.org) (team logos, wordmarks,
headshots and colors for Python plots and tables) and its R and JavaScript counterparts
[sdvplotR](https://sdvplotR.sportsdataverse.org) and [sdvplot-js](https://plot.sportsdataverse.org)
(`@sportsdataverse/sdvplot`, `@sportsdataverse/sporty`, `@sportsdataverse/sdvtables`),
[sportypy](https://sportypy.sportsdataverse.org) (playing surfaces), and
[cfbseedR](https://cfbseedR.sportsdataverse.org) (college football season simulation and CFP
seeding). The full directory is at [sportsdataverse.org/packages](https://sportsdataverse.org/packages).

The NFL submodule is a near drop-in replacement for [nflreadpy](https://github.com/nflverse/nflreadpy);
the broader [nflverse](https://nflverse.nflverse.com) ecosystem is the
upstream data source for many of those loaders.

## Follow the SportsDataverse (@SportsDataverse) on X and star this repo

[![X Follow](https://img.shields.io/badge/%40SportsDataverse-000000?logo=x&style=for-the-badge)](https://x.com/SportsDataverse)

[![GitHub stars](https://img.shields.io/github/stars/sportsdataverse/sportsdataverse-py.svg?color=eee&logo=github&style=for-the-badge&label=Star%20sportsdataverse-py&maxAge=2592000)](https://github.com/sportsdataverse/sportsdataverse-py)

<a href="https://github.com/sportsdataverse/sportsdataverse-py/stargazers">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/star-history/star-history-dark.svg">
    <img alt="Star history of sportsdataverse/sportsdataverse-py" src="https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/star-history/star-history.png" width="600">
  </picture>
</a>

## Our authors

-   [Saiem Gilani](https://twitter.com/saiemgilani)
<a href="https://twitter.com/saiemgilani" target="blank"><img src="https://img.shields.io/badge/%40saiemgilani-000000?logo=x&style=for-the-badge" alt="@saiemgilani" /></a>
<a href="https://github.com/saiemgilani" target="blank"><img src="https://img.shields.io/github/followers/saiemgilani?color=eee&logo=Github&style=for-the-badge" alt="@saiemgilani" /></a>


<!-- cheatsheet-section -->
## Cheat sheet

A printable six-page reference, in four layouts (24 pages in all), for **sportsdataverse (Python)** — the loaders, the wrapper
families, and what each one returns.

📄 **[Download the sportsdataverse (Python) cheat sheet (PDF)](https://sportsdataverse.org/cheatsheets/sportsdataverse-py.pdf)**

Browse the other SportsDataverse cheat sheets at
**[sportsdataverse.org/cheatsheets](https://sportsdataverse.org/cheatsheets)**.

## Citations

To cite the [**`sportsdataverse-py`**](https://py.sportsdataverse.org) Python package in publications, use:

BibTeX Citation

```bibtex
@misc{gilani_sdvpy_2021,
  author = {Gilani, Saiem},
  title = {sportsdataverse-py: The SportsDataverse's Python Package for Sports Data.},
  url = {https://py.sportsdataverse.org},
  year = {2021}
}
```
