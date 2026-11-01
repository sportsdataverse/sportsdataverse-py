# UFL

> sdv-py UFL: endpoint references, dataset loaders and parsers for UFL in the SportsDataverse Python package.

# UFL (`sportsdataverse.football.ufl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 114 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 1 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 24 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 82 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 1 |
| [Hand-written wrappers](reference/additional) | 1 |

## See also

- [sdvplot](https://sdvplot.sportsdataverse.org/) — `surface("ufl")` draws the field with a team's end zones
- [sportypy](https://sportypy.sportsdataverse.org/) — the field sdvplot draws with

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
