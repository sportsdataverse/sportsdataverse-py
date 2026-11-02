<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CEBL fixtures](#cebl-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CEBL fixtures

Source: the maintainers' captures of 2026-10-12, host
`https://api.data.cebl.ca` (the CEBL Stats Hub backend). **CEBL's own API, owner-approved gray area.**

- `captures/`: the 28 truncated captures, copied unchanged (captured live 2026-10-12, arrays cut to 3 rows; a
  games sample keeps a finished REGULAR game, a finished FINALS game and a cancelled one).
- `full/`: untruncated bodies fetched live on 2026-10-13 with the Stats Hub's own header key (never committed) and
  its `Origin`/`Referer`, 1.5 s apart: `games__season__2025` (`/games/2025/`, 128 rows), `players__statistics__
  aggregated__PER_GAME_2025` (175 rows), `teams__statistics__aggregated__TOTALS_2025_REGULAR` (11 rows, one league
  row) and `teams__season__2025` (10 rows). Numeric dtypes in the returns schemas come from these bodies.
- `spec_paths.json`: written by `tools/codegen/gen_cebl.py` (the spec's 12 routes).

The Stats Hub page chunk (`/_next/static/chunks/app/league-leaders/page-<hash>.js`) holds the header key; the runtime
reads it from there and a test fails if any fixture holds the key.
