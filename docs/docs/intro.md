---
title: Getting Started
sidebar_label: Getting Started
sidebar_position: 1
description: "Install sportsdataverse-py and pull tidy NBA, NFL, MLB, NHL, college and 100+ more leagues' data as polars DataFrames."
---

# sportsdataverse-py <a href='https://py.sportsdataverse.org'><img src='https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/main/docs/static/img/sdv-py-logo.png' align="right" width="20%" min-width="100px" alt="sportsdataverse-py logo" /></a>

![Lifecycle: beta](https://img.shields.io/badge/lifecycle-beta-blue.svg?style=for-the-badge&logo=github)
[![PyPI](https://img.shields.io/pypi/v/sportsdataverse?label=sportsdataverse&logo=python&style=for-the-badge)](https://pypi.org/project/sportsdataverse/)

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

> **New here?** Read [Ecosystem & philosophy](https://py.sportsdataverse.org/docs/ecosystem)
> for the design philosophy, the full function-naming paradigm, and how the
> Python and R packages line up.

## Quickstart

```bash
pip install --pre sportsdataverse
```

`--pre` installs the 0.2.0 beta, which these docs describe. Without it pip picks the
last stable release, 0.1.4.

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

Parser-backed wrappers return a polars DataFrame by default; see
[Polars / pandas parser layer](#polars--pandas-parser-layer) below.

## Supported leagues and data sources

<!-- BEGIN generated: leagues-and-sources -->
| Sport | League | Module | Data sources |
|---|---|---|---|
| Football | [College football](cfb/) | `sportsdataverse.cfb` | ESPN (132), sportsdataverse-data releases (74), stats.ncaa.org (1), On3 Recruit Database (82), 247Sports Recruit Database (47), Yahoo Sports Shangrila (7), Fox Sports API (29) |
| Football | [NFL](nfl/) | `sportsdataverse.nfl` | ESPN (125), NFL.com Shield API (22), NFL Pro (32), Sleeper fantasy API (15), PFF Developer API (68), PFF Premium Stats (LEGACY) (46), nflverse data releases (48), sportsdataverse-data releases (21), Fox Sports API (25) |
| Football (Spring & Canadian) | [UFL](ufl/) | `sportsdataverse.football.ufl` | ESPN (114): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1, CDN API (cdn.espn.com) 1, Hand-written wrappers 1 |
| Football (Spring & Canadian) | [XFL](xfl/) | `sportsdataverse.football.xfl` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Football (Spring & Canadian) | [CFL](cfl/) | `sportsdataverse.football.cfl` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball | [NBA](nba/) | `sportsdataverse.nba` | ESPN (123), sportsdataverse-data releases (54), NBA Stats API (130), NBA Official Injury Report (3), Fox Sports API (26), Basketball-Reference (9), RealGM (18), Public model datasets (7) |
| Basketball | [WNBA](wnba/) | `sportsdataverse.wnba` | ESPN (127), sportsdataverse-data releases (39), WNBA Stats API (113), Fox Sports API (26) |
| Basketball | [NBA G League](nbagl/) | `sportsdataverse.nbagl` | ESPN (112), G League Stats API (94) |
| Basketball | [Men's college basketball](mbb/) | `sportsdataverse.mbb` | ESPN (129), sportsdataverse-data releases (34), stats.ncaa.org (136), KenPom (32), Bart Torvik T-Rank (5), Fox Sports API (27) |
| Basketball | [Women's college basketball](wbb/) | `sportsdataverse.wbb` | ESPN (132), sportsdataverse-data releases (34), stats.ncaa.org (114), Bart Torvik Women's T-Rank (1), Fox Sports API (27), Her Hoop Stats (5) |
| Basketball (International (ESPN)) | [NBL](nbl/) | `sportsdataverse.basketball.nbl` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (International (ESPN)) | [FIBA](fiba/) | `sportsdataverse.basketball.fiba` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (International (ESPN)) | [Olympics men's basketball](olympics_mbk/) | `sportsdataverse.basketball.olympics_mbk` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (International (ESPN)) | [Olympics women's basketball](olympics_wbk/) | `sportsdataverse.basketball.olympics_wbk` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (NBA Summer League) | [NBA Summer League (Las Vegas)](nbasl_lv/) | `sportsdataverse.basketball.nbasl_lv` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (NBA Summer League) | [NBA Summer League (Utah)](nbasl_ut/) | `sportsdataverse.basketball.nbasl_ut` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (NBA Summer League) | [NBA Summer League (California)](nbasl_ca/) | `sportsdataverse.basketball.nbasl_ca` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (NBA Summer League) | [NBA Summer League (Sacramento)](nbasl_sac/) | `sportsdataverse.basketball.nbasl_sac` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (NBA Summer League) | [NBA Summer League (Orlando)](nbasl_orl/) | `sportsdataverse.basketball.nbasl_orl` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (NBA Summer League) | [NBA Summer League (Golden State)](nbasl_gs/) | `sportsdataverse.basketball.nbasl_gs` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Basketball (International leagues) | [EuroLeague](euroleague/) | `sportsdataverse.basketball.euroleague` | EuroLeague Competition Engine (15) |
| Basketball (International leagues) | [FIBA (digital API and game pages)](fiba_hub/) | `sportsdataverse.basketball.fiba_hub` | FIBA digital API and game pages (173): FIBA digital API (GDAP, digital-api.fiba.basketball) 31, FIBA game pages (fiba.basketball, RSC payload parsed) 6, Hand-written wrappers 136 |
| Basketball (International leagues) | [Genius Sports LiveStats](genius/) | `sportsdataverse.basketball.genius` | Genius Sports LiveStats (59): (fibalivestats.dcd.shared.geniussports.com, 20+ competitions) 11, Hand-written wrappers 48 |
| Basketball (International leagues) | [Sportradar Atrium](atrium/) | `sportsdataverse.basketball.atrium` | Sportradar Atrium embed API (63): (embed-api.eui.connect.sportradar.com) 22, Hand-written wrappers 41 |
| Basketball (International leagues) | [NBL Australia (Rosetta)](nbl_au/) | `sportsdataverse.basketball.nbl_au` | NBL Rosetta proxy (NBL, WNBL, NBL1) (22) |
| Basketball (International leagues) | [Spanish FEB](feb/) | `sportsdataverse.basketball.feb` | Spanish FEB LiveStats + baloncestoenvivo (11) |
| Basketball (International leagues) | [BBL / DBBL (SCB)](scb/) | `sportsdataverse.basketball.scb` | SCB basketball API (BBL, DBBL) (23) |
| Basketball (International leagues) | [Lega Basket Serie A](lba/) | `sportsdataverse.basketball.lba` | Lega Basket Serie A (19) |
| Basketball (International leagues) | [ACB](acb/) | `sportsdataverse.basketball.acb` | ACB (Liga Endesa) (45) |
| Basketball (International leagues) | [Israeli Premier League](ibba/) | `sportsdataverse.basketball.ibba` | IBBA (Israeli Premier League) and Segev Stats (22): IBBA ASMX web service (basket.co.il, Israel) 16, Segev Stats live-stats API (stats.segevstats.com, Israel play-by-play) 6 |
| Basketball (International leagues) | [W League (Japan)](wjbl/) | `sportsdataverse.basketball.wjbl` | W League (Japan) (19) |
| Basketball (International leagues) | [VTB](vtb/) | `sportsdataverse.basketball.vtb` | VTB United League (38): Infobasket (Russia) 35, API (api.vtb-league.com) 3 |
| Basketball (International leagues) | [KBL](kbl/) | `sportsdataverse.basketball.kbl` | KBL (Korea) (22) |
| Basketball (International leagues) | [B.LEAGUE](bleague/) | `sportsdataverse.basketball.bleague` | B.LEAGUE (Japan) (14) |
| Basketball (International leagues) | [KZS](kzs/) | `sportsdataverse.basketball.kzs` | KZS (Basketball Federation of Slovenia) (23) |
| Basketball (International leagues) | [LKL](lkl/) | `sportsdataverse.basketball.lkl` | LKL (Lithuania) (14) |
| Basketball (International leagues) | [TBF](tbf/) | `sportsdataverse.basketball.tbf` | TBF app API (BSL, TBL, KBSL) (32) |
| Basketball (International leagues) | [CEBL](cebl/) | `sportsdataverse.basketball.cebl` | CEBL stats API (12) |
| Basketball (International leagues) | [ABA League](aba/) | `sportsdataverse.basketball.aba` | ABA League (12) |
| Basketball (International leagues) | [EASL](easl/) | `sportsdataverse.basketball.easl` | EASL (third-party sync server over Sportradar DataCore) (18) |
| Basketball (International leagues) | [BNXT League](bnxt/) | `sportsdataverse.basketball.bnxt` | BNXT League (Sportpress) (28) |
| Basketball (International leagues) | [TPBL](tpbl/) | `sportsdataverse.basketball.tpbl` | TPBL (Taiwan) (16) |
| Basketball (International leagues) | [P. LEAGUE+](plg/) | `sportsdataverse.basketball.plg` | P. LEAGUE+ (Taiwan) and Preciser (19): P. LEAGUE+ site (pleagueofficial.com, Taiwan) 11, P. LEAGUE+ vendor API (api.preciser.io, Taiwan) 7, Hand-written wrappers 1 |
| Basketball (International leagues) | [CBA](cba/) | `sportsdataverse.basketball.cba` | CBA data API (China) (27) |
| Basketball (International leagues) | [Basketligaen](dbl/) | `sportsdataverse.basketball.dbl` | Basketligaen (Denmark, Sportality) (27): Basketligaen site API (www.basketligaen.dk/api, Denmark; Sportality) 25, Hand-written wrappers 2 |
| Hockey | [NHL](nhl/) | `sportsdataverse.nhl` | ESPN (119), sportsdataverse-data releases (32), NHL Web API (28), NHL EDGE (35), NHL Stats REST (21), NHL Records (50), Fox Sports API (25) |
| Hockey | [PWHL](pwhl/) | `sportsdataverse.pwhl` | sportsdataverse-data releases (27), HockeyTech / LeagueStat (25) |
| Hockey (College hockey) | [Men's college hockey](mch/) | `sportsdataverse.hockey.mch` | ESPN (118): site API (v2) 25, web API (v3) 5, core API (v2) 87, FPI API (fitt v3) 1 |
| Hockey (College hockey) | [Women's college hockey](wch/) | `sportsdataverse.hockey.wch` | ESPN (118): site API (v2) 25, web API (v3) 5, core API (v2) 87, FPI API (fitt v3) 1 |
| Hockey (Minor & junior (HockeyTech)) | [AHL](ahl/) | `sportsdataverse.hockey.ahl` | HockeyTech / LeagueStat (11) |
| Hockey (Minor & junior (HockeyTech)) | [OHL](ohl/) | `sportsdataverse.hockey.ohl` | HockeyTech / LeagueStat (11) |
| Hockey (Minor & junior (HockeyTech)) | [WHL](whl/) | `sportsdataverse.hockey.whl` | HockeyTech / LeagueStat (11) |
| Hockey (Minor & junior (HockeyTech)) | [QMJHL](qmjhl/) | `sportsdataverse.hockey.qmjhl` | HockeyTech / LeagueStat (11) |
| Hockey (Minor & junior (HockeyTech)) | [ECHL](echl/) | `sportsdataverse.hockey.echl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [SPHL](sphl/) | `sportsdataverse.hockey.sphl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [CHL](chl/) | `sportsdataverse.hockey.chl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [USHL](ushl/) | `sportsdataverse.hockey.ushl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [BCHL](bchl/) | `sportsdataverse.hockey.bchl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [AJHL](ajhl/) | `sportsdataverse.hockey.ajhl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [SJHL](sjhl/) | `sportsdataverse.hockey.sjhl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [OJHL](ojhl/) | `sportsdataverse.hockey.ojhl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [CCHL](cchl/) | `sportsdataverse.hockey.cchl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [GOJHL](gojhl/) | `sportsdataverse.hockey.gojhl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [MHL](mhl/) | `sportsdataverse.hockey.mhl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [NOJHL](nojhl/) | `sportsdataverse.hockey.nojhl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [VIJHL](vijhl/) | `sportsdataverse.hockey.vijhl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [KIJHL](kijhl/) | `sportsdataverse.hockey.kijhl` | HockeyTech / LeagueStat (12) |
| Hockey (Minor & junior (HockeyTech)) | [MJHL](mjhl/) | `sportsdataverse.hockey.mjhl` | HockeyTech / LeagueStat (12) |
| Hockey (Europe) | [SIHF (Swiss hockey)](sihf/) | `sportsdataverse.hockey.sihf` | SIHF statistics API (33) |
| Hockey (Europe) | [DEL](del/) | `sportsdataverse.hockey.del_` | eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) (12) |
| Hockey (Europe) | [ICE Hockey League](icehl/) | `sportsdataverse.hockey.icehl` | eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) (12) |
| Hockey (Europe) | [EIHL](eihl/) | `sportsdataverse.hockey.eihl` | eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) (9) |
| Hockey (Europe) | [Metal Ligaen](metal/) | `sportsdataverse.hockey.metal` | eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) (8) |
| Hockey (Europe) | [Czech Extraliga](elh/) | `sportsdataverse.hockey.elh` | hokej.cz (Czech Extraliga) (9): Czech Extraliga (hokej.cz S3 bucket + eSports Media scoreboard) 8, Hand-written wrappers 1 |
| Hockey (Europe) | [Liiga](liiga/) | `sportsdataverse.hockey.liiga` | Liiga API (23) |
| Hockey (Europe) | [SHL](shl/) | `sportsdataverse.hockey.shl` | SHL site API (29): (www.shl.se/api, Sweden) 27, Hand-written wrappers 2 |
| Hockey (Europe) | [HockeyAllsvenskan](ha/) | `sportsdataverse.hockey.ha` | HockeyAllsvenskan site API (12): (hockeyallsvenskan.se, Sweden) 11, Hand-written wrappers 1 |
| Hockey (Europe) | [National League](nl/) | `sportsdataverse.hockey.nl` | National League app API (17) |
| Hockey (Europe) | [Ligue Magnus](magnus/) | `sportsdataverse.hockey.magnus` | Ligue Magnus (11) |
| Hockey (Europe) | [KHL](khl/) | `sportsdataverse.hockey.khl` | KHL (site REST + webcaster API) (25): KHL site REST (www.khl.ru/rest) 14, KHL webcaster mobile API (khl.api.webcaster.pro) 9, Hand-written wrappers 2 |
| Hockey (Europe) | [VHL](vhl/) | `sportsdataverse.hockey.vhl` | VHL (14): (vhlru.ru server-rendered pages, parsed) 13, Hand-written wrappers 1 |
| Baseball | [MLB](mlb/) | `sportsdataverse.mlb` | ESPN (123), sportsdataverse-data releases (34), MLB Stats API (79), Baseball Savant (Statcast) (43), Fox Sports API (23) |
| Baseball (College baseball & softball) | [College baseball](college_baseball/) | `sportsdataverse.baseball.college_baseball` | ESPN (122), stats.ncaa.org (3) |
| Baseball (College baseball & softball) | [College softball](college_softball/) | `sportsdataverse.baseball.college_softball` | ESPN (121): site API (v2) 25, web API (v3) 5, core API (v2) 87, FPI API (fitt v3) 1, CDN API (cdn.espn.com) 3 |
| Soccer | [Soccer (all)](soccer/) | `sportsdataverse.soccer` | ESPN (112), American Soccer Analysis (16), FotMob (14), UEFA (7), FIFA (9), Football-Data.co.uk (3), OpenLigaDB (11), kloppy open event data (8) |
| Soccer (Leagues) | [EPL](epl/) | `sportsdataverse.soccer.epl` | ESPN (113): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1, CDN API (cdn.espn.com) 1 |
| Soccer (Leagues) | [LaLiga](laliga/) | `sportsdataverse.soccer.laliga` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Soccer (Leagues) | [Bundesliga](bundesliga/) | `sportsdataverse.soccer.bundesliga` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Soccer (Leagues) | [Serie A](seriea/) | `sportsdataverse.soccer.seriea` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Soccer (Leagues) | [Ligue 1](ligue1/) | `sportsdataverse.soccer.ligue1` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Soccer (Leagues) | [MLS](mls/) | `sportsdataverse.soccer.mls` | ESPN (113), MLS official web API (12) |
| Soccer (Leagues) | [Liga MX](ligamx/) | `sportsdataverse.soccer.ligamx` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Soccer (Leagues) | [UEFA Champions League](ucl/) | `sportsdataverse.soccer.ucl` | ESPN (113): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1, CDN API (cdn.espn.com) 1 |
| Soccer (Leagues) | [UEFA Europa League](uel/) | `sportsdataverse.soccer.uel` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Soccer (Leagues) | [NWSL](nwsl/) | `sportsdataverse.soccer.nwsl` | ESPN (112), NWSL official web API (9) |
| Soccer (Leagues) | [FIFA Women's World Cup](wwc/) | `sportsdataverse.soccer.wwc` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| Soccer (Leagues) | [FIFA World Cup](wc/) | `sportsdataverse.soccer.wc` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| More sports & sources (Cricket) | [Cricket](cricket/) | `sportsdataverse.cricket` | ESPN (112): site API (v2) 24, web API (v3) 5, core API (v2) 82, FPI API (fitt v3) 1 |
| More sports & sources (Betting & prediction markets) | [Betting odds](odds/) | `sportsdataverse.odds` | The Odds API (11), Polymarket (8), Kalshi (9) |
| More sports & sources (Cross-sport providers) | [CBS Sports](cbs/) | `sportsdataverse.providers.cbs` | CBS Sports NAPI (82) |
| More sports & sources (Cross-sport providers) | [Yahoo Sports](yahoo/) | `sportsdataverse.providers.yahoo` | Yahoo Sports Shangrila (107) |
| More sports & sources (Cross-sport providers) | [Fox Sports](fox/) | `sportsdataverse.providers.fox` | Fox Sports API (33) |
| More sports & sources (Cross-sport providers) | [ESPN content (news)](espn_content/) | `sportsdataverse.providers.espn_content` | ESPN (3) |
| More sports & sources (Cross-sport providers) | [TheSportsDB](thesportsdb/) | `sportsdataverse.providers.thesportsdb` | TheSportsDB (12) |
| More sports & sources (Motorsport) | [Formula 1](f1/) | `sportsdataverse.f1` | Jolpica F1 API (Ergast-compatible) (16): Jolpica F1 API (api.jolpi.ca, Ergast-compatible; CC BY-NC-SA 4.0, 500 requests/hour) 15, Hand-written wrappers 1 |
<!-- END generated: leagues-and-sources -->

## Errors

- `NoDataError` — the fetch SUCCEEDED and there is nothing there (a 404, or ESPN's
  200-with-`code:404` body).
- `AssetFetchError` — the fetch FAILED and the answer is unknown (403, rate limit,
  exhausted retries). Never record this as an empty season.
- `ValueError` — a 400 / 422: the request itself is wrong, so retrying cannot help.

The hand-written ESPN scrapers and every generated flat-API getter raise rather
than returning an error body or an empty dict.

## Asking the package about itself

`sdv-docs` is an MCP server over a prebuilt index of this surface — exact columns,
function signatures, provider endpoints and released datasets:

```bash
claude mcp add sdv-docs -- uvx --from 'sportsdataverse[mcp]' sdv-docs
```

It needs the `mcp` extra and Python >= 3.10.

## Polars / pandas parser layer

The parser layer in
[`sportsdataverse._common_espn_parsers`](parsers/index.md)
(plus matching modules for the MLB and NHL sibling APIs) turns raw
payloads into tidy polars (or pandas) DataFrames. ESPN wrappers with a parser
default to `return_parsed=True`. Pass `return_parsed=False` to recover the
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

See the [Architecture](architecture/espn-cross-league.md)
and [Parsers](parsers/index.md)
pages for full details.

:::caution[Deprecated]
`sportsdataverse.parsed.*` is deprecated. The league wrappers already default to
`return_parsed=True`, so call `sportsdataverse.nba` (etc.) directly; the `parsed`
namespace is a thin alias kept for back-compat. See
[Migrating to 0.2.0](guides/migration-0-2-0.md).
:::

## Installation

`pip install --pre sportsdataverse` (or `uv add --prerelease allow sportsdataverse`) while
0.2.0 is in beta. The
[Install & CLI guide](guides/install-and-cli.md) covers uv, the optional extras, the
supported Python and polars versions, the `sdv` command line and the `sdv-docs` MCP
server.

## Authors

- Saiem Gilani ([GitHub](https://github.com/saiemgilani), [X](https://x.com/saiemgilani))

## Citation

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
