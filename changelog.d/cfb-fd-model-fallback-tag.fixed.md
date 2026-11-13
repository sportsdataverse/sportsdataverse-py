- **CFB:** the fourth-down model's trimmed-install fallback now downloads `fd_model.ubj` from `cfb_model_artifacts`, the
  bundled model cfbfastR reads, not the retired, older `espn_cfb_model_artifacts` copy (sha256 `59e937ab` vs
  `31db81a0`). Installs with the bundled model are unchanged. (#1178)
- **CFB:** the default model cache is keyed by the tag (`~/.cache/sportsdataverse/cfb_models/cfb_model_artifacts/`),
  so a copy cached from the old tag is not reused; `SDV_PY_CFB_MODEL_DIR` is still read as `<dir>/fd_model.ubj`. (#1178)
