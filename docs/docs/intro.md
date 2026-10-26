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

:::caution Deprecated
`sportsdataverse.parsed.*` is deprecated. The league wrappers already default to
`return_parsed=True`, so call `sportsdataverse.nba` (etc.) directly; the `parsed`
namespace is a thin alias kept for back-compat.
:::

```python
# Today's NBA scoreboard: a polars DataFrame by default
from sportsdataverse.nba import espn_nba_scoreboard
df = espn_nba_scoreboard()                              # → polars
print(df.select(["game_id", "home_name", "away_name",
                 "home_score", "away_score"]).head())
raw = espn_nba_scoreboard(return_parsed=False)          # → the raw Dict

# Aaron Judge's 2024 season stats from the official MLB API
from sportsdataverse.mlb import mlb_person_stats, parse_mlb_api_person_stats
judge = parse_mlb_api_person_stats(
    mlb_person_stats(person_id=592450, stats="season", season=2024)
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

<!-- BEGIN generated: leagues-and-sources -->
| League | Module | Data sources |
|---|---|---|
| [NBA](nba/) | `sportsdataverse.nba` | ESPN (123), sportsdataverse-data releases (53), NBA Stats API (130), Fox Sports API (26), Basketball-Reference (9), RealGM (18), Public model datasets (7) |
| [WNBA](wnba/) | `sportsdataverse.wnba` | ESPN (127), sportsdataverse-data releases (39), WNBA Stats API (113), Fox Sports API (26) |
| [NBA G League](nbagl/) | `sportsdataverse.nbagl` | ESPN (112) |
| [NBL](nbl/) | `sportsdataverse.nbl` | ESPN (112) |
| [FIBA](fiba/) | `sportsdataverse.fiba` | ESPN (112) |
| [Olympics men's basketball](olympics_mbk/) | `sportsdataverse.olympics_mbk` | ESPN (112) |
| [Olympics women's basketball](olympics_wbk/) | `sportsdataverse.olympics_wbk` | ESPN (112) |
| [NBA Summer League (Las Vegas)](nbasl_lv/) | `sportsdataverse.nbasl_lv` | ESPN (112) |
| [NBA Summer League (Utah)](nbasl_ut/) | `sportsdataverse.nbasl_ut` | ESPN (112) |
| [NBA Summer League (California)](nbasl_ca/) | `sportsdataverse.nbasl_ca` | ESPN (112) |
| [NBA Summer League (Sacramento)](nbasl_sac/) | `sportsdataverse.nbasl_sac` | ESPN (112) |
| [NBA Summer League (Orlando)](nbasl_orl/) | `sportsdataverse.nbasl_orl` | ESPN (112) |
| [NBA Summer League (Golden State)](nbasl_gs/) | `sportsdataverse.nbasl_gs` | ESPN (112) |
| [MBB](mbb/) | `sportsdataverse.mbb` | ESPN (129), sportsdataverse-data releases (34), stats.ncaa.org (136), KenPom (32), Bart Torvik T-Rank (5), Fox Sports API (27) |
| [WBB](wbb/) | `sportsdataverse.wbb` | ESPN (132), sportsdataverse-data releases (34), stats.ncaa.org (114), Bart Torvik Women's T-Rank (1), Fox Sports API (27), Her Hoop Stats (5) |
| [CFB](cfb/) | `sportsdataverse.cfb` | ESPN (132), sportsdataverse-data releases (74), stats.ncaa.org (1), On3 Recruit Database (82), 247Sports Recruit Database (47), Yahoo Sports Shangrila (7), Fox Sports API (29) |
| [NFL](nfl/) | `sportsdataverse.nfl` | ESPN (125), NFL.com Shield API (22), NFL Pro (32), Sleeper fantasy API (15), PFF Developer API (68), PFF Premium Stats (LEGACY) (46), nflverse data releases (48), sportsdataverse-data releases (21), Fox Sports API (25) |
| [MLB](mlb/) | `sportsdataverse.mlb` | ESPN (123), sportsdataverse-data releases (34), MLB Stats API (79), Baseball Savant (Statcast) (43), Fox Sports API (23) |
| [NHL](nhl/) | `sportsdataverse.nhl` | ESPN (119), sportsdataverse-data releases (32), NHL Web API (28), NHL EDGE (35), NHL Stats REST (21), NHL Records (50), Fox Sports API (25) |
| [MCH](mch/) | `sportsdataverse.hockey.mch` | ESPN (118) |
| [WCH](wch/) | `sportsdataverse.hockey.wch` | ESPN (118) |
| [College baseball](college_baseball/) | `sportsdataverse.baseball.college_baseball` | ESPN (122), stats.ncaa.org (3) |
| [College softball](college_softball/) | `sportsdataverse.baseball.college_softball` | ESPN (121) |
| [UFL](ufl/) | `sportsdataverse.football.ufl` | ESPN (114) |
| [XFL](xfl/) | `sportsdataverse.football.xfl` | ESPN (112) |
| [CFL](cfl/) | `sportsdataverse.football.cfl` | ESPN (112) |
| [Soccer (all)](soccer/) | `sportsdataverse.soccer` | ESPN (112), American Soccer Analysis (16), FotMob (14), UEFA (7), FIFA (9), Football-Data.co.uk (3), OpenLigaDB (11), kloppy open event data (8) |
| [EPL](epl/) | `sportsdataverse.soccer.epl` | ESPN (113) |
| [LaLiga](laliga/) | `sportsdataverse.soccer.laliga` | ESPN (112) |
| [Bundesliga](bundesliga/) | `sportsdataverse.soccer.bundesliga` | ESPN (112) |
| [Serie A](seriea/) | `sportsdataverse.soccer.seriea` | ESPN (112) |
| [Ligue 1](ligue1/) | `sportsdataverse.soccer.ligue1` | ESPN (112) |
| [MLS](mls/) | `sportsdataverse.soccer.mls` | ESPN (113), MLS official web API (12) |
| [Liga MX](ligamx/) | `sportsdataverse.soccer.ligamx` | ESPN (112) |
| [UCL](ucl/) | `sportsdataverse.soccer.ucl` | ESPN (113) |
| [UEL](uel/) | `sportsdataverse.soccer.uel` | ESPN (112) |
| [NWSL](nwsl/) | `sportsdataverse.soccer.nwsl` | ESPN (112), NWSL official web API (9) |
| [WWC](wwc/) | `sportsdataverse.soccer.wwc` | ESPN (112) |
| [WC](wc/) | `sportsdataverse.soccer.wc` | ESPN (112) |
| [Cricket](cricket/) | `sportsdataverse.cricket` | ESPN (112) |
| [PWHL](pwhl/) | `sportsdataverse.pwhl` | sportsdataverse-data releases (27), HockeyTech / LeagueStat (25) |
| [AHL](ahl/) | `sportsdataverse.hockey.ahl` | HockeyTech / LeagueStat (11) |
| [OHL](ohl/) | `sportsdataverse.hockey.ohl` | HockeyTech / LeagueStat (11) |
| [WHL](whl/) | `sportsdataverse.hockey.whl` | HockeyTech / LeagueStat (11) |
| [QMJHL](qmjhl/) | `sportsdataverse.hockey.qmjhl` | HockeyTech / LeagueStat (11) |
| [ECHL](echl/) | `sportsdataverse.hockey.echl` | HockeyTech / LeagueStat (12) |
| [SPHL](sphl/) | `sportsdataverse.hockey.sphl` | HockeyTech / LeagueStat (12) |
| [CHL](chl/) | `sportsdataverse.hockey.chl` | HockeyTech / LeagueStat (12) |
| [USHL](ushl/) | `sportsdataverse.hockey.ushl` | HockeyTech / LeagueStat (12) |
| [BCHL](bchl/) | `sportsdataverse.hockey.bchl` | HockeyTech / LeagueStat (12) |
| [AJHL](ajhl/) | `sportsdataverse.hockey.ajhl` | HockeyTech / LeagueStat (12) |
| [SJHL](sjhl/) | `sportsdataverse.hockey.sjhl` | HockeyTech / LeagueStat (12) |
| [OJHL](ojhl/) | `sportsdataverse.hockey.ojhl` | HockeyTech / LeagueStat (12) |
| [CCHL](cchl/) | `sportsdataverse.hockey.cchl` | HockeyTech / LeagueStat (12) |
| [GOJHL](gojhl/) | `sportsdataverse.hockey.gojhl` | HockeyTech / LeagueStat (12) |
| [MHL](mhl/) | `sportsdataverse.hockey.mhl` | HockeyTech / LeagueStat (12) |
| [NOJHL](nojhl/) | `sportsdataverse.hockey.nojhl` | HockeyTech / LeagueStat (12) |
| [VIJHL](vijhl/) | `sportsdataverse.hockey.vijhl` | HockeyTech / LeagueStat (12) |
| [KIJHL](kijhl/) | `sportsdataverse.hockey.kijhl` | HockeyTech / LeagueStat (12) |
| [MJHL](mjhl/) | `sportsdataverse.hockey.mjhl` | HockeyTech / LeagueStat (12) |
| [Betting odds](odds/) | `sportsdataverse.odds` | The Odds API (11), Polymarket (8), Kalshi (9) |
| [CBS Sports](cbs/) | `sportsdataverse.cbs` | CBS Sports NAPI (82) |
| [Yahoo Sports](yahoo/) | `sportsdataverse.yahoo` | Yahoo Sports Shangrila (107) |
| [Fox Sports](fox/) | `sportsdataverse.fox` | Fox Sports API (33) |
| [EuroLeague](euroleague/) | `sportsdataverse.euroleague` | EuroLeague Competition Engine (15) |
| [Formula 1](f1/) | `sportsdataverse.f1` | Jolpica F1 API (Ergast-compatible) (16) |
| [ESPN content (news)](espn_content/) | `sportsdataverse.espn_content` | ESPN (3) |
| [TheSportsDB](thesportsdb/) | `sportsdataverse.thesportsdb` | TheSportsDB (12) |
| [SIHF (Swiss hockey)](sihf/) | `sportsdataverse.hockey.sihf` | SIHF statistics API (33) |
| [DEL](del/) | `sportsdataverse.hockey.del_` | eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) (12) |
| [ICE Hockey League](icehl/) | `sportsdataverse.hockey.icehl` | eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) (12) |
| [EIHL](eihl/) | `sportsdataverse.hockey.eihl` | eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) (9) |
| [Metal Ligaen](metal/) | `sportsdataverse.hockey.metal` | eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) (8) |
| [Czech Extraliga](elh/) | `sportsdataverse.hockey.elh` | hokej.cz (Czech Extraliga) (9) |
| [Liiga](liiga/) | `sportsdataverse.hockey.liiga` | Liiga API (23) |
| [SHL](shl/) | `sportsdataverse.hockey.shl` | SHL site API (29) |
| [HockeyAllsvenskan](ha/) | `sportsdataverse.hockey.ha` | HockeyAllsvenskan site API (12) |
| [National League](nl/) | `sportsdataverse.hockey.nl` | National League app API (17) |
| [Ligue Magnus](magnus/) | `sportsdataverse.hockey.magnus` | Ligue Magnus (11) |
| [KHL](khl/) | `sportsdataverse.hockey.khl` | KHL (site REST + webcaster API) (25) |
| [VHL](vhl/) | `sportsdataverse.hockey.vhl` | VHL (14) |
| [FIBA (digital API and game pages)](fiba_hub/) | `sportsdataverse.basketball.fiba_hub` | FIBA digital API and game pages (173) |
| [Genius Sports LiveStats](genius/) | `sportsdataverse.basketball.genius` | Genius Sports LiveStats (59) |
| [Sportradar Atrium](atrium/) | `sportsdataverse.basketball.atrium` | Sportradar Atrium embed API (63) |
| [NBL Australia (Rosetta)](nbl_au/) | `sportsdataverse.basketball.nbl_au` | NBL Rosetta proxy (NBL, WNBL, NBL1) (22) |
| [Spanish FEB](feb/) | `sportsdataverse.basketball.feb` | Spanish FEB LiveStats + baloncestoenvivo (11) |
| [BBL / DBBL (SCB)](scb/) | `sportsdataverse.basketball.scb` | SCB basketball API (BBL, DBBL) (23) |
| [Lega Basket Serie A](lba/) | `sportsdataverse.basketball.lba` | Lega Basket Serie A (19) |
| [ACB](acb/) | `sportsdataverse.basketball.acb` | ACB (Liga Endesa) (45) |
| [Israeli Premier League](ibba/) | `sportsdataverse.basketball.ibba` | IBBA (Israeli Premier League) and Segev Stats (22) |
| [W League (Japan)](wjbl/) | `sportsdataverse.basketball.wjbl` | W League (Japan) (19) |
| [VTB](vtb/) | `sportsdataverse.basketball.vtb` | VTB United League (38) |
| [KBL](kbl/) | `sportsdataverse.basketball.kbl` | KBL (Korea) (22) |
| [B.LEAGUE](bleague/) | `sportsdataverse.basketball.bleague` | B.LEAGUE (Japan) (14) |
| [KZS](kzs/) | `sportsdataverse.basketball.kzs` | KZS (Basketball Federation of Slovenia) (23) |
| [LKL](lkl/) | `sportsdataverse.basketball.lkl` | LKL (Lithuania) (14) |
| [TBF](tbf/) | `sportsdataverse.basketball.tbf` | TBF app API (BSL, TBL, KBSL) (32) |
| [CEBL](cebl/) | `sportsdataverse.basketball.cebl` | CEBL stats API (12) |
| [ABA League](aba/) | `sportsdataverse.basketball.aba` | ABA League (12) |
| [EASL](easl/) | `sportsdataverse.basketball.easl` | EASL (third-party sync server over Sportradar DataCore) (18) |
| [BNXT League](bnxt/) | `sportsdataverse.basketball.bnxt` | BNXT League (Sportpress) (28) |
| [TPBL](tpbl/) | `sportsdataverse.basketball.tpbl` | TPBL (Taiwan) (16) |
| [P. LEAGUE+](plg/) | `sportsdataverse.basketball.plg` | P. LEAGUE+ (Taiwan) and Preciser (19) |
| [CBA](cba/) | `sportsdataverse.basketball.cba` | CBA data API (China) (27) |
| [Basketligaen](dbl/) | `sportsdataverse.basketball.dbl` | Basketligaen (Denmark, Sportality) (27) |
<!-- END generated: leagues-and-sources -->

## Errors (0.1.5)

- `NoDataError` — the fetch SUCCEEDED and there is nothing there (a 404, or ESPN's
  200-with-`code:404` body).
- `AssetFetchError` — the fetch FAILED and the answer is unknown (403, rate limit,
  exhausted retries). Never record this as an empty season.
- `ValueError` — a 400 / 422: the request itself is wrong, so retrying cannot help.

As of 0.1.5 the hand-written ESPN scrapers and every generated flat-API getter
raise rather than returning an error body or an empty dict.

## Asking the package about itself

`sdv-docs` is an MCP server over a prebuilt index of this surface — exact columns,
function signatures, provider endpoints and released datasets:

```bash
claude mcp add sdv-docs -- uvx --from 'sportsdataverse[mcp]' sdv-docs
```

Needs 0.1.5 or newer and Python >= 3.10.

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

BibTeX Citation
```bibtex
@misc{gilani_sdvpy_2021,
  author = {Gilani, Saiem},
  title = {sportsdataverse-py: The SportsDataverse's Python Package for Sports Data.},
  url = {https://py.sportsdataverse.org},
  season = {2021}
}
```
