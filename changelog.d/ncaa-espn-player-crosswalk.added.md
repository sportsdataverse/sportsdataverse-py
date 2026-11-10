- **Basketball:** `ncaa_espn_player_crosswalk(season, league)` maps the stats.ncaa.org player id to the ESPN athlete
  id for one season, from published releases, with `match_method`, `match_confidence` and `unmatched_reason`
  (BB-D37). (#1157)
- **Basketball:** it matches 99.7-99.9% of 2023-24 and 2024-25 roster players in both leagues and agrees row for row
  with the `source == "ncaa"` rows of `load_{mbb,wbb}_player_aliases`. (#1157)
