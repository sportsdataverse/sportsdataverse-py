- **Generated wrappers:** `typing.get_type_hints()` resolves every generated ESPN and flat-API wrapper (it raised
  `NameError` on `pl`), and wrappers that return a dict of frames (`espn_*_summary`, the CDN game payloads,
  multi-result-set `nba_stats` endpoints) are annotated `Dict[str, pl.DataFrame]`, as their docstrings say.
