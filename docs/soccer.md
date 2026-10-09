# SOCCER

> sdv-py SOCCER: endpoint references, dataset loaders and parsers for SOCCER in the SportsDataverse Python package.

# SOCCER (`sportsdataverse.soccer`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `site.api.espn.com`, `site.web.api.espn.com`, `sports.core.api.espn.com` | 112 | none |
| [American Soccer Analysis](#american-soccer-analysis) | `app.americansocceranalysis.com` | 16 | none |
| [FotMob](#fotmob) | `www.fotmob.com` | 14 | none (unofficial) |
| [UEFA](#uefa) | `comp.uefa.com` | 7 | none |
| [FIFA](#fifa) | `api.fifa.com` | 9 | none |
| [Football-Data.co.uk](#football-data-co-uk) | `www.football-data.co.uk` | 3 | none (CSV archive) |
| [OpenLigaDB](#openligadb) | `api.openligadb.de` | 11 | none |
| [kloppy open event data](#kloppy-open-event-data) | `kloppy.pysport.org` | 8 | none (optional `soccer` extra) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 8 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 82 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |

## American Soccer Analysis {#american-soccer-analysis}

| Reference | Functions |
|---|---:|
| [American Soccer Analysis (app.americansocceranalysis.com)](reference/asa) | 16 |

## FotMob {#fotmob}

| Reference | Functions |
|---|---:|
| [FotMob data API (fotmob.com, unofficial)](reference/fotmob) | 14 |

## UEFA {#uefa}

| Reference | Functions |
|---|---:|
| [UEFA front-end APIs (comp/match/standings/matchstats.uefa.com)](reference/uefa) | 7 |

## FIFA {#fifa}

| Reference | Functions |
|---|---:|
| [FIFA public API v3 (api.fifa.com)](reference/fifa) | 9 |

## Football-Data.co.uk {#football-data-co-uk}

| Reference | Functions |
|---|---:|
| [Football-Data.co.uk CSV archive (football-data.co.uk)](reference/football_data) | 3 |

## OpenLigaDB {#openligadb}

| Reference | Functions |
|---|---:|
| [OpenLigaDB (api.openligadb.de, community German football)](reference/openligadb) | 11 |

## kloppy open event data {#kloppy-open-event-data}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 8 |

## See also

- [kloppy](https://kloppy.pysport.org) — reads ~15 event and tracking providers behind the `soccer` extra's open-data loader
- [socceraction](https://github.com/ML-KULeuven/socceraction) — the SPADL and xT reference implementation these ports follow
- [sdvplot](https://github.com/sportsdataverse/sdvplot) — `pitch_coords()` puts any provider's events on the 105 x 68 pitch
- [sdvplotR](https://github.com/sportsdataverse/sdvplotR) — the R twin (`sdv_pitch_coords()`)
- [itscalledsoccer](https://github.com/American-Soccer-Analysis/itscalledsoccer) — American Soccer Analysis's own client for the API behind `asa_*`
- [soccerdata](https://github.com/probberechts/soccerdata) — FBref, Understat, WhoScored and Sofascore scrapers that sdv-py does not wrap
- [mplsoccer](https://mplsoccer.readthedocs.io) — matplotlib pitches and StatsBomb helpers that sdvplot's pitch frame interoperates with

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
