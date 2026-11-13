- **CFB:** the fourth-down model's trimmed-install fallback now downloads `fd_model.ubj` from `cfb_model_artifacts`, the
  bundle the package ships and cfbfastR reads, instead of the retired `espn_cfb_model_artifacts` copy, a different and older
  model (2026-06-22, sha256 `59e937ab`, vs the bundled `31db81a0`). Installs with the bundled model are unchanged.
  The default download cache is now keyed by the tag (`~/.cache/sportsdataverse/cfb_models/cfb_model_artifacts/`), so a
  copy cached from the old tag is not reused; an explicit `SDV_PY_CFB_MODEL_DIR` is still read as `<dir>/fd_model.ubj`.
