# Capture the 2021 CFBD advanced-stats + SP+ oracle corpus (T2.3 Phase 0).
# Season 2021 because the hosted load_cfb_pbp parquet covers 2002-2021 only
# (2022+ 404s -- producer gap, escalated to cfb-data backfill).
# Reads CFBD_API_KEY from ~/.Renviron automatically.
suppressPackageStartupMessages({
  library(cfbfastR)
  library(dplyr)
  library(arrow)
})

season <- 2021L
out_dir <- "tests/fixtures/cfb_advanced"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

teams <- cfbd_team_info(year = season) |>
  transmute(team_id = as.character(team_id), school)

adv <- cfbd_stats_season_advanced(season, excl_garbage_time = TRUE)
cat("advanced cols:", paste(names(adv), collapse = ", "), "\n")
adv_out <- adv |>
  inner_join(teams, by = c("team" = "school")) |>
  transmute(
    season = as.integer(season),
    team_id,
    team,
    off_success_rate = off_success_rate,
    def_success_rate = def_success_rate,
    off_explosiveness = off_explosiveness,
    off_ppa = off_ppa,
    def_ppa = def_ppa,
    def_havoc_total = def_havoc_total,
    # CFBD field position avg_start is yards FROM OWN GOAL (higher = better start)
    avg_start_yardline = off_field_pos_avg_start,
    off_plays = off_plays,
    def_plays = def_plays
  )
stopifnot(nrow(adv_out) > 100)
write_parquet(adv_out, file.path(out_dir, "cfbd_advanced_2021.parquet"))
cat("cfbd_advanced_2021 rows:", nrow(adv_out), "\n")

sp <- cfbd_ratings_sp(season) |> filter(team != "nationalAverages")
cat("sp cols:", paste(names(sp), collapse = ", "), "\n")
sp_out <- sp |>
  inner_join(teams, by = c("team" = "school")) |>
  transmute(
    season = as.integer(season),
    team_id,
    team,
    sp_overall = rating,
    sp_offense = offense_rating,
    sp_defense = defense_rating,
    sp_offense_rank = as.integer(offense_ranking),
    sp_defense_rank = as.integer(defense_ranking)
  )
stopifnot(nrow(sp_out) > 100)
write_parquet(sp_out, file.path(out_dir, "sp_plus_2021.parquet"))
cat("sp_plus_2021 rows:", nrow(sp_out), "\n")

# per-team game counts (for the tempo plays/GAME oracle -- the released pbp
# is missing games for some teams, so totals are not comparable)
gi <- cfbd_game_info(season, season_type = "both")
gc <- rbind(
  data.frame(team = gi$home_team, id = as.character(gi$home_id)),
  data.frame(team = gi$away_team, id = as.character(gi$away_id))
) |>
  count(team, id, name = "games") |>
  inner_join(teams, by = c("team" = "school")) |>
  transmute(season = as.integer(season), team_id, games = as.integer(games))
stopifnot(nrow(gc) > 100)
write_parquet(gc, file.path(out_dir, "cfbd_games_2021.parquet"))
cat("cfbd_games_2021 rows:", nrow(gc), "\n")
