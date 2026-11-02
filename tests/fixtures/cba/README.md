<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [tests/fixtures/cba](#testsfixturescba)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# tests/fixtures/cba

Real responses of `data-server.cbaleague.com` (the Chinese Basketball Association data API), captured 2026-10-12 through a
residential proxy by the maintainers' capture tool (URL base
`https://data-server.cbaleague.com/api`). The wire bodies are AES-128-ECB encrypted; everything here is the DECRYPTED JSON
except `full/wire_getMatchType.txt`, the raw wire body of `GET /api/com-code-tables/getMatchType` (proves the decrypt path
against the real service).

- `captures/*.json`: the maintainers' truncated samples (arrays cut to 3 rows), one per (route, job). Per-route parser fixtures.
  Personal data was redacted by the capture (operator ids, one mobile number).
- `full/`: untruncated bodies of game `100105799` (2025-26 round 1, Zhejiang Zheshang Securities 88-74 Shanxi Fenjiu):
  header, player box, period scores, best player / best quota, report status, and `events_100105799.json` (all four
  periods' play-by-play pages, 74 + 92 + 76 + 77 rows, keyed by period); plus `season_list.json` (32 seasons) and
  `getTeam_2009.json` / `getTeam_2025.json`. Parser dtypes come from the capture's `presence` sidecars (type sets over
  FULL bodies), not from the 3-row cuts.
- `spec_paths.json`: the 27 route paths of `cba.openapi.yaml`, so tests never read the maintainers' private reference checkout.

Regenerate the schemas, dtypes and descriptions with `tools/codegen/gen_cba.py` (needs the maintainers' private reference checkout).
