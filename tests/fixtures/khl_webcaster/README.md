<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [KHL webcaster fixtures (`khl_webcaster`)](#khl-webcaster-fixtures-khl_webcaster)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# KHL webcaster fixtures (`khl_webcaster`)

Real responses from the KHL TV app's backend,
`GET https://khl.api.webcaster.pro/api/khl_mobile/<route>.json?locale=en` (keyless).

- `*.json` (top level): copies of the maintainers' webcaster captures,
  captured 2026-10-12 by their capture tool. Stage `370` (= khl.ru tournament 1369),
  event `2786714` (= game 898236), team `18` (= club 7), players `33405` and `1144`, `events_v2`
  page 1. TRUNCATED: every array keeps its first 3 entries.
- `full/data.json`: ONE complete `data.json` body, fetched 2026-10-12 (`?locale=en`), untruncated,
  written compact. It holds the whole season table (`stages_v2`, 2008-09 to 2026-27) that maps a
  season END year to a stage; the `stages` returns schema and the season-mapping tests come from it.

Redactions: `remote_ip` (the caller's IP) and `bets` / `fonbet` are removed at any depth, as the
runtime does. Nothing else is edited. Owner flag (the maintainers' notes): fine as test fixtures; no
published data release without sanctions sign-off.
