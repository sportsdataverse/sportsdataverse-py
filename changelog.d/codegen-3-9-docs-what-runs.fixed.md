- **Docs:** a function counts as documented only by its own reference block, not by a mention in a table or note, so 19
  public functions (`load_pbp`, `atrium_end_year`, `shot_value`, ...) and two classes now have entries. (#901)
- **Docs:** `espn_<lg>_calendar` in seven leagues, and the wbb/wnba `standings` / `team_roster` / `draft`, are
  documented as the hand-written functions those names run, not as the generated wrappers they shadow. (#901)
- **Docs:** `nba_live_boxscore` / `wnba_live_boxscore` document their six returned frames, one table each, and
  the `nba_stats` / `wnba_stats` wrappers list the `NoDataError` / `ValueError` / `AssetFetchError` they raise. (#901)
- **Docs:** Examples and page titles name each league's home module (`sportsdataverse.soccer.mls`), not its
  deprecated alias; the NFL Pro page has a title, and `parse_summary` gives the right section count. (#901)
- **Fox:** an empty standings (or polls, events, nav, header, search, trending, roster) table parses to a
  zero-row frame instead of rows from another list in the body. (#901)
