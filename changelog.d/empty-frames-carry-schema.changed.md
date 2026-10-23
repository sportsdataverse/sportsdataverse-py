- **Empty results keep their columns:** the ESPN league wrappers of 45 dedicated parsers and the soccer / cricket
  scoreboards, generated and multi-file NBA stats loaders, 3 HockeyTech parsers and `enrich_pbp` return their table,
  not (0, 0); `parse_items`, `parse_single_entity` and cdn rankings still return (0, 0). (#978)
