- **ESPN soccer/cricket:** the 68 wrappers that parse through `parse_soccer_*` / `parse_cricket_*` now document
  those parsers and their returns tables (the 12 headline soccer leagues showed the generic tables), and every
  ESPN wrapper is annotated `Union[pl.DataFrame, pd.DataFrame, Dict]` instead of `Dict`. (#858)
