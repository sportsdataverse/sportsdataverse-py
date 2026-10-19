- **Generated loaders and wrappers:** `seasons=np.int64(2024)` and `seasons=2024.0` load the 2024 season (they raised
  `TypeError`), and a fractional year raises `ValueError` instead of loading the truncated one. `bool_str("false")`
  is `"false"`; ESPN path params are URL-quoted; a misspelled ESPN wrapper kwarg names the wrapper.
