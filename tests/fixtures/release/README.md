<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Release-utility parity fixtures](#release-utility-parity-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Release-utility parity fixtures

Golden fixtures for the `sportsdataverse.release` module — the Python port of
the `sportsdataversedata` R package (sportsdataverse/sportsdataverse-data,
v0.0.11, `R/upload.R` + `R/gh_cli.R` + `R/zzz.R`).

All fixtures were produced by running the **real R functions** (sourced from
the sibling checkout) via `make_fixtures.R` in this directory, with only the
network upload stubbed out.

Provenance: R 4.5.3 | arrow 23.0.1.2 | data.table 1.18.2.1 | jsonlite 2.0.0 |
rlang 1.2.0 | captured 2026-07-12 on Windows.

| File | Produced by | Notes |
|---|---|---|
| `parity_frame.csv` / `.csv.gz` / `.parquet` | `sportsdataverse_save()` (upload.R L100-188) | character `season` + double `week` coerced to integer (Int32 in parquet); `sportsdataverse_type` / `sportsdataverse_timestamp` in parquet file metadata |
| `timestamp.txt` / `timestamp.json` | `create_timestamp_file()` (upload.R L46-59) | values are capture-time-dependent; tests assert shape/keys only |
| `package_function.txt` / `.json` | `create_package_function()` (upload.R L62-80) | input: `"sportsdataverse::load_parity_frame()"` |
| `assets_raw.json` | `gh release view espn_cfb_pbp -R sportsdataverse/sportsdataverse-data --json assets` (gh 2.31.0) | raw payload replayed into both R and Python parsers; tag chosen because it carries timestamp assets (exercises the filter) |
| `assets_expected.csv` | `gh_cli_release_assets()` (gh_cli.R L97-128) on `assets_raw.json` | `size_string` is right-justified across the vector by R's format method — Python emits unpadded values; tests compare stripped |
| `sizes_expected.csv` | `as.character(rlang::as_bytes(x))` per-value | oracle for the `_size_string()` helper (per-value ⇒ no vector padding) |

Regenerate with (repo root): `Rscript tests/fixtures/release/make_fixtures.R`
(requires the sportsdataverse-data checkout at the sibling path and gh CLI
authed for one read-only `gh release view` call).
