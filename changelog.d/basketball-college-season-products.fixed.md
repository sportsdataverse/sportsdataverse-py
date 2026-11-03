- **College basketball:** `mbb_strength_of_schedule` and `mbb_bracketology` (and the `wbb_` shims)
  failed on every real season with an Int32-vs-String team-id join; ids are now normalized (#1079)
- **College basketball:** the women's quads use the women's NET ranges, and bracketology no longer
  reads the end-of-season standings for an as-of auto-bid (#1079)
