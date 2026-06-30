library(dplyr)

fn <- \(df) {
  a <- df |>
    group_by(g) |>
    mutate(x = lag(v))      # grouped -> clean
  b <- df |>
    mutate(y = lag(w))      # UNGROUPED -> leak (must be flagged, like fnwrap)
  b
}
