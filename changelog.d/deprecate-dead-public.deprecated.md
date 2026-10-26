- **Utilities:** `sportsdataverse.dl_utils.ESPNHTTP` / `ESPNResponse`, the polars `df.janitor.to_pascal_case()` /
  `to_camel_case()` / `to_kebab_case()` methods and `sportsdataverse.decorators.record_mem_usage` /
  `record_time_usage` had no caller; they still work, warn `DeprecationWarning`, and are removed in 0.4.0. (#987)
