library(dplyr)

# Grouped pipe, but the lag is wrapped in an inline {} block expression.
# TODAY this is FALSE-flagged: the inline brace re-roots the lag away from the
# group_by. After the fix (only function-definition braces are statement
# boundaries) the lag roots to the grouped mutate statement -> clean (0 findings).
out <- df |>
  group_by(game_id) |>
  mutate(prev = { tmp <- ep; lag(tmp) })
