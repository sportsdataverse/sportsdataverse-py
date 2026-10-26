- **Basketball:** the minted per-competition game functions refuse a game of another competition with
  `AssetFetchError`, and need `season` or `comp` (their docs say so). The parameterised `genius_*` /
  `fiba_gamepage_*` / `atrium_*` wrappers are otherwise unchanged. (#864)
- **Genius:** `esake_pbp` and the rest read the game's LiveStats `/u/` shell (one cached request per game) and check its
  competition name; when that request fails twice or the shell names none, the call raises `AssetFetchError` saying
  the scope check could not run, rather than returning the game unchecked. (#864)
- **FIBA:** a `comp=` override must be an edition of the bound family; a competition GDAP gives no family is refused
  with that reason. (#864)
- **Basketball:** the per-process listing cache keeps at most 1,024 entries (least recently used evicted), so a
  backfill does not grow it without bound. (#864)
- **Genius:** `lfb_schedule(season=2019)` (and `genius_schedule` of any page with an unfilled play-off slot) passes its
  scope check: a `To be determined` block takes the competition the page's other blocks prove, and a page of only such
  blocks raises `AssetFetchError`. `lfb_*` covers the two seasons Genius hosts, 2018 and 2019. (#864)
