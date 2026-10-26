- **sdv-docs:** the server opens an index whose path contains `?`, `#` or `%`; such a path used to end
  the SQLite URI early and silently open a different, empty file. A corrupt or foreign `SDV_DOCS_DB`
  now gets an `index unreadable: …` answer instead of a raised `DatabaseError`. (#746)
- **sdv-docs:** when the `mcp` import fails, `sdv-docs` prints the import error after the install hint,
  so an installed but incompatible `mcp` is visible. (#746)
- **sdv-docs index:** stdlib and third-party objects a module re-exports (`typing.Any`, `xgboost.Booster`,
  `tqdm`, `functools.lru_cache`, 14 in all) are no longer indexed as sdv-py functions. SDV partials and
  type aliases whose `__module__` is `functools` or `typing` stay indexed. (#746)
