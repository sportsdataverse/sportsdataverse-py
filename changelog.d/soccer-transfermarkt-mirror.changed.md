- **Soccer:** `load_soccer_transfermarkt_*()` read the five tables from a new `soccer_transfermarkt` release mirror
  (parquet, sha256s, README with source, CC0 1.0 and the 2026-07-06 snapshot date) and fall back, with a warning,
  to the original gzip CSVs when it cannot be read. (#1360)
