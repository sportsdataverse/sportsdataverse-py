- **Internals:** the private `sportsdataverse._common_espn` module (host constants plus a `_get` / `_csv` re-export
  nothing read) leaves the package and the wheel; import `_get` from `sportsdataverse._codegen_runtime`. The committed
  `tools/codegen/_generated/` staging copies and the retired `extract*.py` bootstraps are deleted. (#985)
