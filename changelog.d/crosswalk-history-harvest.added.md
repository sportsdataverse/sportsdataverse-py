- **Crosswalk history harvest:** private `sportsdataverse._crosswalk_history` captures Fox, Yahoo and CBS raw game
  listings and per-game box payloads into the `*-alt-raw` repos: resumable, atomic, season-checked, and paced by
  `XWALK_RPS` / `XWALK_<PROVIDER>_RPS` only; a 403 or 429 stops the run. (#805)
