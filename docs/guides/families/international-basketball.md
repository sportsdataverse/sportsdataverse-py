# International basketball

> The 25 international basketball homes under sportsdataverse.basketball, from EuroLeague and FIBA to the B.LEAGUE and CBA, and the conventions they share.

Twenty-five international basketball families live under
`sportsdataverse.basketball.<home>`. A home is named for a **provider family**, not
always a league: some providers serve many competitions, and some leagues are served
by a provider that also serves others. The international competitions ESPN covers
are separate, in the ESPN league modules (`fiba`, `olympics_mbk`, `olympics_wbk`,
`nbl`).

## The homes

| Home | Provider | Covers | Function prefix |
|---|---|---|---|
| `euroleague` | EuroLeague Competition Engine | EuroLeague and EuroCup | `euroleague_*` |
| `fiba_hub` | FIBA's digital API and game pages | FIBA competitions and national teams | `fiba_gdap_*`, `fiba_gamepage_*`, and per competition (`fiba_eurobasket_*`, `fiba_wc_*`, `fiba_bcl_*`, ...) |
| `genius` | Genius Sports LiveStats | many competitions that publish through LiveStats, including the WBBL | `genius_*`, `wbbl_*` |
| `atrium` | Sportradar Atrium | many competitions | `atrium_*` |
| `nbl_au` | NBL Australia (Rosetta) | NBL Australia | `nbl_rosetta_*` |
| `feb` | Spanish federation (FEB) | the FEB competitions | `feb_*` |
| `scb` | the German leagues' stats provider | BBL and DBBL | `scb_*` |
| `lba` | Lega Basket | Serie A (Italy) | `lba_*` |
| `acb` | Liga ACB | Liga Endesa (Spain) | `acb_*` |
| `ibba` | Israel Basketball Association | the Israeli leagues | `ibba_*` |
| `wjbl` | W League | the Japanese women's league | `wjbl_*` |
| `vtb` | Infobasket and the league site | VTB United League | `vtb_infobasket_*` and others |
| `kbl` | KBL | the Korean Basketball League | `kbl_*` |
| `bleague` | B.LEAGUE | the Japanese men's league | `bleague_*` |
| `kzs` | KZS | the Slovenian leagues | `kzs_*` |
| `lkl` | LKL | the Lithuanian league | `lkl_*` |
| `tbf` | Turkish federation (TBF) | the Turkish leagues | `tbf_*` |
| `cebl` | CEBL Stats Hub | the Canadian Elite Basketball League | `cebl_*` |
| `aba` | ABA League | the Adriatic league | `aba_*` |
| `easl` | EASL, through an unofficial proxy | the East Asia Super League | `easl_*` |
| `bnxt` | Sportpress | the BNXT League | `sportpress_*` |
| `tpbl` | TPBL | the Taiwan Professional Basketball League | `tpbl_*` |
| `plg` | P. LEAGUE+ site and its Preciser feed | P. LEAGUE+ (Taiwan) | `pleague_*`, `pleague_preciser_*` |
| `cba` | CBA | the Chinese Basketball Association | `cba_*` |
| `dbl` | Sportality | Basketligaen (Denmark) | `sportality_*` |

Two consequences of naming homes for providers: the function prefix is the
provider's (`sportpress_schedule` in `bnxt`, `sportality_pbp` in `dbl`,
`nbl_rosetta_match_pbp` in `nbl_au`), and one home can reach several competitions
through a league or competition argument (`scb_games(league="dbbl")`,
`genius_competition_games(comp_id)`, `atrium_fixtures(website_id)`).

## What each home covers

Most homes have a schedule or results, standings, box scores and play-by-play;
many add shot charts, leaders and player pages. Each also exports its parsers
(`parse_<home>_*`). The league pages list every function with its parameters and
Returns table: [EuroLeague](../../euroleague/index.md), [FIBA](../../fiba_hub/index.md), [Genius](../../genius/index.md),
[Atrium](../../atrium/index.md), [CEBL](../../cebl/index.md) and the rest under
Basketball in the sidebar.

## Shared conventions

- **Seasons are END years** at the public API (`season=2026` is 2025-26 for a
  winter league; a summer league's season is its calendar year). Each runtime maps
  it to the provider's own key.
- **Polite pacing.** Requests go through one throttled session that spaces them at
  least one second apart.
- **Proxies are optional and named.** A few providers block some networks; set
  `GENIUS_PROXY` or `CBA_PROXY` to route those families through a proxy. The value is
  never logged.
- **Some sites need `curl_cffi`.** Live FIBA game pages and P. LEAGUE+ sit behind
  bot protection, so their runtimes use `curl_cffi` (`pip install curl_cffi`, or the
  `all` extra).
- **Tidy by default.** Wrappers return a polars DataFrame; `return_parsed=False`
  gives the raw payload and `return_as_pandas=True` gives pandas.

```python
from sportsdataverse.basketball.cebl import cebl_games, cebl_standings
from sportsdataverse.basketball.easl import easl_standings

standings = cebl_standings(season=2025)       # CEBL plays in summer: the 2025 season
games = cebl_games(season=2025)
east_asia = easl_standings(season=2026)       # 2025-26
print(standings.height, games.height, east_asia.height)
```

Several of these sources are unofficial uses of public sites, and some docstrings
carry a release flag saying the data may be used for reference but not
redistributed. Read the function's docstring before republishing anything.
