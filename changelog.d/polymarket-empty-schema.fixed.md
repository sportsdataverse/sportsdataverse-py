- **Odds:** an empty Polymarket answer (an expired market's book, an empty page) is a zero-row frame with the route's
  documented columns and dtypes, not a frame with no columns. A flat-API parser opts in with `_documented_empty`, and
  its generated wrappers then pass each route's returns table as `documented_table=`.
