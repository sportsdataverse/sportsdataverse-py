# Capture the cfbseedR 0.2.0 oracle for tests/cfb/test_cfb_standings.py::TestRegistry2026Parity.
#
# Run from the sdv-py repo root with the cfbseedR checkout as a sibling:
#   Rscript tests/fixtures/seedr/cfb_toy_registry_2026/capture.R ../cfbseedR
#
# Writes the inputs first, then re-reads them, so the R oracle and the Python
# test consume byte-identical CSVs.
args <- commandArgs(trailingOnly = TRUE)
pkgload::load_all(if (length(args)) args[[1]] else "../cfbseedR", quiet = TRUE)
out <- "tests/fixtures/seedr/cfb_toy_registry_2026"

g <- function(case, season, week, home, away, result) {
  data.frame(case = case, season = season, week = week, game_type = "REG",
             home_team = home, away_team = away, result = result)
}
tm <- function(case, team, conference, conf_division = NA, postseason_eligible = NA) {
  data.frame(case = case, team = team, conference = conference,
             conf_division = conf_division, postseason_eligible = postseason_eligible)
}
tb <- function(case, kind, team, value) data.frame(case = case, kind = kind, team = team, value = value)

# A 2-0, B 2-0 (never met), C 2-2, D 1-2, E 0-3 (cfbseedR test-registry-2026.R)
epoch <- function(case, season) {
  rbind(g(case, season, 1, "A", "C", 7), g(case, season, 2, "A", "D", 7),
        g(case, season, 1, "B", "C", 7), g(case, season, 2, "B", "E", 7),
        g(case, season, 3, "C", "D", 7), g(case, season, 4, "C", "E", 7),
        g(case, season, 3, "D", "E", 7))
}
five <- function(case, conf) tm(case, c("A", "B", "C", "D", "E"), conf)
amer_base <- function(case) {
  rbind(g(case, 2026, 1, "A", "C", 7), g(case, 2026, 1, "B", "D", 7),
        g(case, 2026, 2, "A", "D", 7), g(case, 2026, 2, "B", "C", 7))
}

games <- rbind(
  epoch("acc_2025", 2025),
  epoch("acc_2026", 2026),
  # L 3-1; Y 2-1 (L's loss count); X 3-2 (L's win count); M 2-3; F4 2-2
  g("acc_pool_2026", 2026, c(1, 2, 3, 4, 1, 2, 3, 1, 2, 3, 4, 5, 5, 6, 7, 6, 7),
    c("L", "L", "L", "M", "Y", "Y", "M", "X", "X", "X", "F4", "F4", "F1", "F2", "F3", "F1", "F2"),
    c("F1", "F2", "F3", "L", "F1", "F2", "Y", "F1", "F2", "F3", "X", "X", "M", "M", "M", "F4", "F4"),
    7),
  g("sunbelt_div", 2026, c(1, 1, 2, 2), c("E1", "W1", "E1", "E2"), c("E2", "W2", "W1", "W2"), 7),
  # E1 3-0, E2 2-1, W1 1-2, W2 0-3: the West champion W1 (1-2) still takes
  # conf_rank 2 over the better-record E2 - only the division format does that
  g("sunbelt_div_champ", 2026, 1:6, c("E1", "E1", "E1", "E2", "E2", "W1"),
    c("E2", "W1", "W2", "W1", "W2", "W2"), 7),
  amer_base("american_final_win"),
  rbind(amer_base("american_final_loss"),
        g("american_final_loss", 2026, 3, c("C", "D"), c("A", "B"), 7)),
  # same, plus a LATER non-conference win for A: the clause reads the final
  # CONFERENCE game, so A still falls through
  rbind(amer_base("american_final_loss_nonconf"),
        g("american_final_loss_nonconf", 2026, 3, c("C", "D"), c("A", "B"), 7),
        g("american_final_loss_nonconf", 2026, 4, "A", "NC1", 7)),
  # East E1/E2/E3 all 1-1, E2-E3 never met: div_pct seeds E2 (1-0 in division)
  # as East champion; W1 (2-0) is the West champion and ranks first
  g("sunbelt_div_pct", 2026, 1:5, c("E2", "E1", "W1", "E3", "W1"), c("E1", "E3", "E2", "W2", "W2"), 7),
  epoch("cusa_apr", 2026),
  # A 2-1 and B 2-1, A beat B; the composite still puts B first
  g("mw_composite", 2026, 1:6, c("A", "A", "D", "B", "B", "C"), c("B", "C", "A", "C", "D", "D"), 7),
  epoch("mac_official", 2026),
  g("ineligible", 2026, 1:3, c("A", "A", "B"), c("B", "C", "C"), 7)
)
teams <- rbind(
  five("acc_2025", "ACC"),
  five("acc_2026", "ACC"),
  tm("acc_pool_2026", c("L", "X", "Y", "M", "F1", "F2", "F3", "F4"), "ACC"),
  tm("sunbelt_div", c("E1", "E2", "W1", "W2"), "Sun Belt", c("East", "East", "West", "West")),
  tm("sunbelt_div_champ", c("E1", "E2", "W1", "W2"), "Sun Belt", c("East", "East", "West", "West")),
  tm("american_final_win", c("A", "B", "C", "D"), "American Athletic"),
  tm("american_final_loss", c("A", "B", "C", "D"), "American Athletic"),
  tm("american_final_loss_nonconf", c("A", "B", "C", "D"), "American Athletic"),
  tm("sunbelt_div_pct", c("E1", "E2", "E3", "W1", "W2"), "Sun Belt", c("East", "East", "East", "West", "West")),
  five("cusa_apr", "Conference USA"),
  tm("mw_composite", c("A", "B", "C", "D"), "Mountain West"),
  five("mac_official", "MAC"),
  tm("ineligible", c("A", "B", "C"), "Big Ten", postseason_eligible = c(FALSE, NA, NA))
)
tiebreak <- rbind(
  tb("acc_2025", "analytics_ratings", c("A", "B", "C"), c(1, 2, 0.5)),
  tb("acc_2026", "analytics_ratings", c("A", "B", "C"), c(1, 2, 0.5)),
  # every tail tie is rated too, so no case reaches a coin toss (the oracle
  # must be deterministic in both engines)
  tb("acc_pool_2026", "analytics_ratings", c("L", "X", "Y", "F1", "F2", "M"), c(100, 90, 80, 70, 60, 50)),
  tb("american_final_win", "cfp_rankings", "A", 20),
  tb("american_final_win", "analytics_ratings", c("A", "B", "C", "D"), c(1, 2, 0.5, 0.25)),
  tb("american_final_loss", "cfp_rankings", "A", 20),
  tb("american_final_loss", "analytics_ratings", c("A", "B", "C", "D"), c(1, 2, 0.5, 0.25)),
  tb("american_final_loss_nonconf", "cfp_rankings", "A", 20),
  tb("american_final_loss_nonconf", "analytics_ratings", c("A", "B", "C", "D"), c(1, 2, 0.5, 0.25)),
  tb("cusa_apr", "analytics_ratings", c("A", "B"), c(1, 1)),
  tb("cusa_apr", "apr", c("A", "B"), c(980, 990)),
  tb("mw_composite", "analytics_ratings", c("A", "B"), c(1, 2)),
  tb("mac_official", "analytics_ratings", c("A", "B", "C"), c(1, 2, 0.5))
)

# CFP selection: 16 ranked teams. P4 champions T01 (SEC), T02 (Big Ten), T06
# (ACC) and T14 (Big 12, ranked outside the top 12); the best G6 team is the
# Sun Belt champion T13 (the MAC champion T16 ranks behind it); Notre Dame 12th.
conf <- c("SEC", "Big Ten", "Big Ten", "Other", "Big 12", "ACC", "SEC", "Other",
          "Other", "Other", "Other", "FBS Independents", "Sun Belt", "Big 12", "Other", "MAC")
seed_st <- data.frame(
  sim = 1, team = c(sprintf("T%02d", 1:11), "Notre Dame", sprintf("T%02d", 13:16)),
  conference = conf, conf_champ = seq_len(16) %in% c(1, 2, 6, 13, 14, 16),
  win_pct = 1 - (1:16) / 100, sov = 0.5, sos = 0.5, pd = 100 - (1:16)
)
seed_rank <- data.frame(team = seed_st$team, rank = 1:16)

write.csv(games, file.path(out, "games.csv"), row.names = FALSE, na = "")
write.csv(teams, file.path(out, "teams.csv"), row.names = FALSE, na = "")
write.csv(tiebreak, file.path(out, "tiebreaker_data.csv"), row.names = FALSE, na = "")
write.csv(seed_st, file.path(out, "seeds_standings.csv"), row.names = FALSE, na = "")
write.csv(seed_rank, file.path(out, "seeds_rankings.csv"), row.names = FALSE, na = "")

games <- read.csv(file.path(out, "games.csv"))
teams <- read.csv(file.path(out, "teams.csv"), na.strings = "")
tiebreak <- read.csv(file.path(out, "tiebreaker_data.csv"))
expected <- do.call(rbind, lapply(unique(games$case), function(cs) {
  gm <- games[games$case == cs, setdiff(names(games), "case")]
  tt <- teams[teams$case == cs, setdiff(names(teams), "case")]
  tt <- tt[, colSums(!is.na(tt)) > 0, drop = FALSE]
  if ("postseason_eligible" %in% names(tt)) tt$postseason_eligible <- as.logical(tt$postseason_eligible)
  td <- tiebreak[tiebreak$case == cs, ]
  data <- list()
  for (k in unique(td$kind)) {
    sub <- td[td$kind == k, ]
    data[[k]] <- data.frame(team = sub$team)
    data[[k]][[c(analytics_ratings = "rating", cfp_rankings = "rank", apr = "apr")[[k]]]] <- sub$value
  }
  st <- cfb_standings(gm, tt, verbosity = "NONE",
                      tiebreaker_data = if (length(data)) data else NULL)
  data.frame(case = cs, team = st$team, conference = st$conference,
             conf_rank = st$conf_rank, conf_champ = st$conf_champ)
}))
expected <- expected[order(expected$case, expected$team), ]
write.csv(expected, file.path(out, "expected_standings.csv"), row.names = FALSE, na = "")

seed_st <- read.csv(file.path(out, "seeds_standings.csv"))
seed_rank <- read.csv(file.path(out, "seeds_rankings.csv"))
seeds <- do.call(rbind, lapply(c("2025", "2026"), function(rule) {
  s <- cfb_playoff_seeds(seed_st, rankings = seed_rank, playoff_seeds = 12L, autobid = rule)
  data.frame(autobid = as.integer(rule), team = s$team, seed = s$seed)
}))
seeds <- seeds[order(seeds$autobid, seeds$team), ]
write.csv(seeds, file.path(out, "expected_seeds.csv"), row.names = FALSE, na = "")
cat("cfbseedR", as.character(packageVersion("cfbseedR")), "\n")
