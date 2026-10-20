- **Runtimes:** the Data API id-map lookup no longer returns an error body as the row (dispatch falls back);
  `pff_api_*` returns `{}` on a 204/205; with the opt-in cache on, statcast / torvik return the same JSON on a hit as on
  the first call; the On3, The Odds API and statcast player parsers return zero rows for a hostile body. (#TBD)
