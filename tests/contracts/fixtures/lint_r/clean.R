library(dplyr)

# clean: pipe group_by sibling
out1 <- df |>
  group_by(game_id) |>
  mutate(prev_ep = lag(ep))

# clean: .by argument
out2 <- df |>
  mutate(prev_wp = lag(wp), .by = game_id)

# clean: cumulative under group_by
out3 <- df |>
  group_by(game_id) |>
  mutate(run_pts = cumsum(points))
