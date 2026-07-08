<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sports247_site_pages fixtures](#sports247_site_pages-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sports247_site_pages fixtures

Real captures of the auth-free `247sports.com/*.json` front-end page-model
routes (the surface wrapped by the `sports247_site_pages` flat-API stem —
distinct from the guest-JWT `ipa.247sports.com/rdb/v1` RDB behind `sports247`).

- **Provenance:** copied verbatim from
  `sdv-internal-refs/247sports/captures/site-pages/` (captured 2026-07-08).
- **Source URLs:** each capture corresponds to an `x-example-url` in
  `sdv-internal-refs/247sports/site-pages.openapi.yaml` — e.g.
  `institution.json` ← `https://247sports.com/Institution/24099.json`,
  `recruits_season.json` ← `https://247sports.com/Season/2026-Football/Recruits.json?Items=15&Page=1`.
- **Do not hand-edit.** Re-capture from the live routes if a schema drifts;
  the parser tests are payload-agnostic and assert only on stable columns.
