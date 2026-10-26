- **polars 2 required.** The runtime pin is now `polars>=2.0,<3` (conda recipe mirrored); polars 1.x is no
  longer supported, so code may use 2.x-only API such as the `bin_*` replacements for `cut`/`qcut`. The lock
  already resolved polars 2.0 on every supported Python, so nothing tested 1.x. (#776)
