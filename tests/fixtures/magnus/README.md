<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Ligue Magnus fixtures](#ligue-magnus-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Ligue Magnus fixtures

Real responses from `https://liguemagnus.com` (keyless), all captured on **2026-10-12**. Competition `240` is
2026-27 (season key `2027`, the END year) and phase `714` is its regular season.

| File | Request | Rows kept |
|---|---|---|
| `ajax__get_competition_by_type.json` | `/wp-admin/admin-ajax.php?action=get_competition_by_type&id_type=1` | 3 of 7 |
| `ajax__get_phases.json` | `...?action=get_phases&competition_id=240` | 1 of 1 |
| `ajax__get_equipes.json` | `...?action=get_equipes&phase_id=714` | 3 of 12 |
| `ajax__get_rencontres.json` | `...?action=get_rencontres&competition_id=240&phase_id=714&par_page=700` | 3 of 264 |
| `ajax__get_rencontres@round2.json` | `...?action=get_rencontres&par_page=700&competition_id=240&phase_id=714&journee=2` | 6 of 6 (whole) |
| `ajax__get_rencontres@round44.json` | `...?action=get_rencontres&par_page=700&competition_id=240&phase_id=714&journee=44` | 6 of 6 (whole; unplayed) |
| `ajax__get_liste_tours.json` | `...?action=get_liste_tours&phase_id=714` | 3 of 44 |
| `ajax__get_rencontrestours.json` | `...?action=get_rencontrestours&phase_id=714&journee=1` | 3 of 6 |
| `ajax__get_classementsphase.json` | `...?action=get_classementsphase&phase_id=714` | 3 of 12 |
| `rencontre__id.json` | game sheet of `/rencontre/77779/` (2026-27; shootout, MVPs) | arrays cut to 3, one event per type |
| `rencontre__id@68838.json` | game sheet of `/rencontre/68838/` (2025-26; goalie change) | arrays cut to 3, one event per type |
| `rencontre__id@865.json` | game sheet of `/rencontre/865/` (2020-21; timeout, no faceoffs) | arrays cut to 3, one event per type |
| `full/rencontre__77779.json` | game sheet of `/rencontre/77779/` | **whole** (159 events, 41 players, 10 shootout attempts) |

## Provenance

- The `ajax__*.json` files without `@` and the three `rencontre__id*.json` game sheets are copies of
  `sdv-internal-refs/ligue-magnus/captures/` (commit `92520e6`): truncated samples, every array cut to its first 3
  elements (a game sheet's `evenements` keeps the first event of each type).
- `ajax__get_rencontres@round2.json` and `@round44.json` are whole bodies fetched through
  `magnus_runtime._get` with the query `magnus_schedule(season=2027, phase_id=714, round=2|44)` sends.
  Round 2 holds the `score[]` trap: games 77751, 77753 and 77754 list extra 0-goal score rows for teams that did not
  play, so scores are matched by team id, never by position. Round 44 is unplayed (`etat` null, a 0-0 placeholder).
- `full/rencontre__77779.json` is the whole game sheet extracted from the page's `<live-rencontre-container :data>`
  attribute (regex -> `html.unescape` -> `json.loads`), written as compact JSON. It is the full body the returns
  schemas and the dtype test are derived from. Nothing in the bodies is a float: every number is an integer.

## Redaction

Staff birth dates (`staffs[].ddn`) are masked to the literal `DD/MM/YYYY` in every game sheet here, as in the refs
captures. The runtime drops the field before returning a sheet, and no parser reads it.
