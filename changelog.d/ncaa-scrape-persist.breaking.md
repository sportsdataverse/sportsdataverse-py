- **`ncaa_{mbb,wbb}_play_by_play` / `ncaa_{mbb,wbb}_box_scores`** raise `AssetFetchError` when a fetch fails (a ban, an
  exhausted pool, an uncleared challenge) instead of dropping that game, which made a ban read as a smaller season;
  `errors="skip"` keeps going and adds a `scrape_status` column with one row per dropped game saying why. (#924)
