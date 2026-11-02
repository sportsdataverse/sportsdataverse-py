# Byte oracle for the RDS writer's non-parity-frame branches (C-L58): Date, POSIXct with and
# without a time zone, an Int64 column outside R's int32 range (R has no 64-bit integer: character, exact),
# an all-null column (logical NA) and a Categorical column (written as character).
#
# Usage (sdv-py repo root):
#   "C:/Program Files/R/R-4.6.1/bin/Rscript.exe" tests/fixtures/release/make_branch_golden.R
# Each column is built exactly as R would hold the value the Python frame carries, then
# readRDS() round-trips it and the script stops unless every column reads back identical.

out <- "tests/fixtures/release/rds_golden_branches.rds"
# Attribute order names -> row.names -> class, the order the published release frames carry
# (rds_golden.rds); data.frame() itself would write class before row.names.
df <- structure(
  list(
    d = as.Date(c("2024-01-02", NA, "1969-12-31")),
    t_utc = structure(c(1704164400, NA, -86400.5), class = c("POSIXct", "POSIXt"), tzone = "UTC"),
    t_naive = structure(c(1704164400, 0, NA), class = c("POSIXct", "POSIXt")),
    big = c("3000000000", NA, "-3000000001"),
    nul = c(NA, NA, NA),
    cat = c("a", "b", NA)
  ),
  row.names = c(NA, -3L),
  class = "data.frame"
)
saveRDS(df, out, compress = FALSE, version = 2)
back <- readRDS(out)
stopifnot(identical(back, df))
stopifnot(inherits(back$d, "Date"), identical(attr(back$t_utc, "tzone"), "UTC"), is.character(back$big), is.logical(back$nul), is.character(back$cat))
cat(R.version.string, "\n")
str(back)
