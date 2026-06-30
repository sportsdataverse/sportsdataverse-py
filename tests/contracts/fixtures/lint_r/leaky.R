library(dplyr)

# leak: lag with no grouping
bad1 <- df |>
  mutate(prev_ep = lag(ep))

# leak: cumsum with no grouping
bad2 <- df |>
  mutate(run_pts = cumsum(points))
