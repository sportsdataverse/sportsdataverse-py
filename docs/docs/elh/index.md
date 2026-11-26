---
title: Czech Extraliga
sidebar_label: Czech Extraliga
description: "sdv-py Czech Extraliga: endpoint references, dataset loaders and parsers for Czech Extraliga in the SportsDataverse Python package."
---
# Czech Extraliga (`sportsdataverse.hockey.elh`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [hokej.cz (Czech Extraliga)](#hokej-cz-czech-extraliga) | `json.esports.cz`, `s3-eu-west-1.amazonaws.com` | 9 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 1 | — |

## hokej.cz (Czech Extraliga) {#hokej-cz-czech-extraliga}

| Reference | Functions |
|---|---:|
| [Czech Extraliga (hokej.cz S3 bucket + eSports Media scoreboard)](reference/elh) | 8 |
| [Hand-written wrappers](reference/additional) | 1 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://fastRhockey.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.elh` (Python) | `fastRhockey` (R) |
|---|---|
| [`elh_events`](reference/elh#elh_events) | [`elh_events`](https://fastRhockey.sportsdataverse.org/reference/elh_events.html) |
| [`elh_game_ids`](reference/additional#elh_game_ids) | [`elh_game_ids`](https://fastRhockey.sportsdataverse.org/reference/elh_game_ids.html) |
| [`elh_list_objects`](reference/elh#elh_list_objects) | [`elh_list_objects`](https://fastRhockey.sportsdataverse.org/reference/elh_list_objects.html) |
| [`elh_pbp`](reference/elh#elh_pbp) | [`elh_pbp`](https://fastRhockey.sportsdataverse.org/reference/elh_pbp.html) |
| [`elh_pbp_goals`](reference/elh#elh_pbp_goals) | [`elh_pbp_goals`](https://fastRhockey.sportsdataverse.org/reference/elh_pbp_goals.html) |
| [`elh_pbp_latest`](reference/elh#elh_pbp_latest) | [`elh_pbp_latest`](https://fastRhockey.sportsdataverse.org/reference/elh_pbp_latest.html) |
| [`elh_pbp_mobile`](reference/elh#elh_pbp_mobile) | [`elh_pbp_mobile`](https://fastRhockey.sportsdataverse.org/reference/elh_pbp_mobile.html) |
| [`elh_scoreboard`](reference/elh#elh_scoreboard) | [`elh_scoreboard`](https://fastRhockey.sportsdataverse.org/reference/elh_scoreboard.html) |
| [`elh_shots`](reference/elh#elh_shots) | [`elh_shots`](https://fastRhockey.sportsdataverse.org/reference/elh_shots.html) |
