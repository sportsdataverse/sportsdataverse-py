# Golden-fixture generator for the sportsdataversedata -> sportsdataverse.release port.
#
# Sources the canonical R package files directly from the sibling checkout
# (sportsdataverse/sportsdataverse-data @ v0.0.11) and runs the REAL functions:
#   - sportsdataverse_save()  (upload.R lines 100-188) with the upload stubbed out,
#     capturing the csv / csv.gz / parquet files it writes to tempdir()
#   - create_timestamp_file() / create_package_function() (upload.R lines 46-80)
#   - gh_cli_release_assets() (gh_cli.R lines 97-128) with .invoke_cli_command
#     stubbed to replay a captured `gh release view --json assets` payload
#
# Usage (from the sdv-py repo root):
#   Rscript tests/fixtures/release/make_fixtures.R
#
# Requires: arrow, data.table, jsonlite, rlang, cli; gh CLI on PATH (one
# read-only `gh release view` call to capture assets_raw.json).

library(data.table) # gh_cli.R relies on `@import data.table` in the pkg namespace

r_pkg_dir <- "c:/Users/saiem/Documents/GitHub-Data/sdv-dev/sportsdataverse-data"
out_dir <- "tests/fixtures/release"

source(file.path(r_pkg_dir, "R", "upload.R"))
source(file.path(r_pkg_dir, "R", "gh_cli.R"))

# ---- 1. sportsdataverse_save file outputs -----------------------------------
# Stub the upload tail so the real save() runs fully offline.
sportsdataverse_upload <- function(files, ...) invisible(TRUE)

# Representative frame: character season (as.integer coercion — including a
# float-stringified and a space-padded value, which R parses via double),
# double week, strings with comma/quote, NA in numeric + string + logical.
test_df <- data.frame(
  season = c("2023", "2024.0", " 2025"),
  week = c(1, 2, 18),
  game_id = c(401547401L, 401547402L, 401547403L),
  team = c("Green Bay", "St. Louis, MO", 'The "Team"'),
  epa = c(0.123456789, -1.5, NA),
  home = c(TRUE, FALSE, NA),
  note = c("plain", NA, "trailing space "),
  stringsAsFactors = FALSE
)

sportsdataverse_save(
  data_frame = test_df,
  file_name = "parity_frame",
  sportsdataverse_type = "Parity fixture frame",
  release_tag = "test-tag",
  pkg_function = "sportsdataverse::load_parity_frame()",
  .token = "unused",
  file_types = c("rds", "csv", "csv.gz", "parquet")
)

for (f in paste0("parity_frame.", c("rds", "csv", "csv.gz", "parquet"))) {
  stopifnot(file.copy(file.path(tempdir(), f), file.path(out_dir, f), overwrite = TRUE))
}

# ---- 1b. rds byte-golden oracle ----------------------------------------------
# The exact frame sportsdataverse_save() serializes (post season/week
# coercion), with a FIXED timestamp attribute and no compression, so the
# Python writer (sportsdataverse/_rds.py) can be byte-compared in CI without
# an R installation. The serialization header (14 bytes: "X\n" + 3 version
# ints) is skipped in the comparison, so R version drift here is harmless.
golden_df <- test_df
golden_df$season <- as.integer(golden_df$season)
golden_df$week <- as.integer(golden_df$week)
attr(golden_df, "sportsdataverse_type") <- "Parity fixture frame"
# bare-epoch POSIXct (no tzone attr), like Sys.time() in the real save();
# epoch = 2026-07-12 14:00:00 UTC
attr(golden_df, "sportsdataverse_timestamp") <- structure(
  as.numeric(as.POSIXct("2026-07-12 14:00:00", tz = "UTC")),
  class = c("POSIXct", "POSIXt")
)
saveRDS(
  golden_df,
  file.path(out_dir, "rds_golden.rds"),
  compress = FALSE,
  version = 2
)

# ---- 2. timestamp + package_function sidecar files --------------------------
ts_files <- create_timestamp_file()
pf_files <- create_package_function("test-tag", "sportsdataverse::load_parity_frame()")
for (f in c(ts_files, pf_files)) {
  stopifnot(file.copy(f, file.path(out_dir, basename(f)), overwrite = TRUE))
}

# ---- 3. gh_cli_release_assets parsing ---------------------------------------
# Capture the raw JSON once, then replay it through the real parser so the
# Python side parses the identical payload.
tag <- "espn_cfb_pbp" # carries timestamp assets => exercises the filter branch
# system(intern = TRUE) splits lines > 8095 bytes on Windows; collapse before
# writing (same as R's .cli_parse_json does before parsing) so the fixture is
# the byte-exact single-line gh payload
raw_lines <- system(
  paste("gh release view", tag, "-R sportsdataverse/sportsdataverse-data --json assets"),
  intern = TRUE
)
raw_json <- paste0(raw_lines, collapse = "")
writeLines(raw_json, file.path(out_dir, "assets_raw.json"))

.invoke_cli_command <- function(cli_command) raw_json
assets_df <- gh_cli_release_assets(tag)
data.table::fwrite(assets_df, file.path(out_dir, "assets_expected.csv"))

# ---- 4. rlang::as_bytes size-format oracle ----------------------------------
# Formatted per-value (a vector call right-justifies across elements).
# 1998 / 999999 pin the promote-on-round behavior ("2.00 kB", "1.00 MB").
sizes <- c(
  0, 1, 999, 1000, 1500, 1234, 1998, 10000, 38392, 331907, 999999,
  1048576, 5000000, 123456789, 9876543210
)
size_strings <- vapply(
  sizes, function(s) as.character(rlang::as_bytes(s)), character(1)
)
write.csv(
  data.frame(size = sizes, expected = size_strings),
  file.path(out_dir, "sizes_expected.csv"),
  row.names = FALSE
)

# ---- provenance --------------------------------------------------------------
cat(
  "R", paste0(R.version$major, ".", R.version$minor),
  "| arrow", as.character(packageVersion("arrow")),
  "| data.table", as.character(packageVersion("data.table")),
  "| jsonlite", as.character(packageVersion("jsonlite")),
  "| rlang", as.character(packageVersion("rlang")),
  "| tag:", tag,
  "\n"
)
