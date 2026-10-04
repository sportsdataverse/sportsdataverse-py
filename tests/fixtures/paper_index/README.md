<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [paper_index fixtures](#paper_index-fixtures)
  - [`paper_index_oracle.json` and `paper_index_oracle_nfl.json`](#paper_index_oraclejson-and-paper_index_oracle_nfljson)
  - [`oracle_games_pbp_cfb.parquet` and `oracle_games_pbp_nfl.parquet`](#oracle_games_pbp_cfbparquet-and-oracle_games_pbp_nflparquet)
  - [`gop_compute_cfb.json` and `gop_compute_nfl.json`](#gop_compute_cfbjson-and-gop_compute_nfljson)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# paper_index fixtures

Real-game oracles for `sportsdataverse/paper_index.py`, the port of Game on
Paper's `python/paper_index.py`.

## `paper_index_oracle.json` and `paper_index_oracle_nfl.json`

Verbatim copies of Game on Paper's trainer fixtures, written by
`python/tools/fit_paper_index.py` (the script that fitted `WEIGHTS`).

- Source: `game-on-paper-app` commit `770ba849533f13ed0ecb72cb7c16bb0b51ee5946`
  (2026-09-15, "feat(paper-index): NFL deserved-win fit (#256)"), files
  `python/tests/fixtures/paper_index_oracle.json` (cfb) and
  `python/tests/fixtures/paper_index_oracle_nfl.json` (nfl). Unchanged on GOP
  `main` as of `0492b1c7`.
- Contents: the full-precision fitted `weights`, the fit `provenance` (train and
  holdout seasons, per-season row accounting, holdout Brier and the paired
  EPA-only gate, the field-position curve fingerprint, holdout contamination),
  and 12 real holdout `games` per league: each side's eight model inputs as the
  trainer aggregated them from the released `espn_{league}_pbp`, and the
  trainer's `expectedHomeShare`.
- The shipped weights are rounded to 4 decimals, so GOP's own oracle test
  asserts shares to `5e-4`; `tests/test_paper_index.py` keeps that tolerance.

## `oracle_games_pbp_cfb.parquet` and `oracle_games_pbp_nfl.parquet`

The released play-by-play rows of the same 24 games, projected to
`paper_index.PBP_COLUMNS` (23 columns), sorted by `game_id, game_play_number`;
captured 2026-10-03. Ids are as released: `game_id`, `pos_team_id`, `homeTeamId`,
`awayTeamId` all Int64.

- cfb (1,993 rows, 12 games): `cfbfastR-cfb-data` HEAD `b47631f6`,
  `cfb/pbp/parquet/play_by_play_{2024,2025}.parquet` (sha256 `9b4bfe6a...`,
  `8444ac72...`), the files published as `espn_cfb_pbp`.
- nfl (2,061 rows, 12 games): the `nfl-data` build output
  `out/espn_nfl/pbp/play_by_play_{2022..2025}.parquet` written 2026-09-15 (the
  `espn_nfl_pbp` assets the NFL fit read; sha256 2022 `26459cef...`, 2023
  `6a6d4513...`, 2024 `ecb9a48a...`, 2025 `8fbeffed...`).

The NFL rows reproduce the trainer's inputs exactly (max abs difference
`9e-16`). The college rows do NOT: `espn_cfb_pbp` 2024-2025 was rebuilt after
the fit (the cfb pbp repairs of sportsdataverse-py #643-#651), so these rows'
inputs differ from the trainer's by up to `0.095` and the served share of game
401636915 moves from 0.157 to 0.291. The college real-row test therefore asserts
against the GOP module run on these same rows (below), and the trainer's numbers
are asserted through the input replay only.

## `gop_compute_cfb.json` and `gop_compute_nfl.json`

The GOP python oracle run on the parquet rows above: `paper_index.compute` from
`game-on-paper-app` `770ba849` (shipped 4-decimal weights; the sportsdataverse
0.1.4 field-position curves, whose sha256 match the oracle JSONs' `fp_curve`),
per game `homeId`, `awayId`, `homeShare`, `margins` and both sides' inputs
(`home`, `away`), in oracle-JSON game order. Both files were written by this
script, run from `game-on-paper-app/python` with GOP's venv and the two oracle
JSONs beside the output:

```python
import json, polars as pl, paper_index as pi

SRC = {"cfb": ("paper_index_oracle.json", "<cfbfastR-cfb-data>/cfb/pbp/parquet/play_by_play_{s}.parquet"),
       "nfl": ("paper_index_oracle_nfl.json", "<nfl-data>/out/espn_nfl/pbp/play_by_play_{s}.parquet")}
COLS = [...]  # sportsdataverse.paper_index.PBP_COLUMNS
for lg, (fx, src) in SRC.items():
    frames, oracle = [], []
    for g in json.load(open(fx))["games"]:
        rows = pl.read_parquet(src.format(s=g["season"]), columns=COLS).filter(pl.col("game_id") == int(g["gameId"]))
        frames.append(rows)
        last = rows.sort("game_play_number").tail(1)
        h, a = int(last["homeTeamId"][0]), int(last["awayTeamId"][0])
        out = pi.compute(rows.rename({"pos_team_id": "pos_team"}), h, a, lg)
        oracle.append({"gameId": g["gameId"], "homeId": h, "awayId": a, "homeShare": out["homeShare"],
                       "margins": out["margins"], "home": out["teams"][str(h)], "away": out["teams"][str(a)]})
    pl.concat(frames).sort("game_id", "game_play_number").write_parquet(f"oracle_games_pbp_{lg}.parquet")
    json.dump({"league": lg, "games": oracle}, open(f"gop_compute_{lg}.json", "w"), indent=1)
```
