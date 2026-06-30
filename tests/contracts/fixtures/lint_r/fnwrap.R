library(dplyr)

process <- function(df) {
  a <- df |>
    group_by(g) |>
    mutate(x = lag(v))      # grouped -> clean
  b <- df |>
    mutate(y = lag(w))      # UNGROUPED -> leak (masked by top-level-root today)
  b
}

nested <- function(df) {
  out <- df |> group_by(g) |> mutate(z = cumsum(p))  # grouped -> clean
  out
}
