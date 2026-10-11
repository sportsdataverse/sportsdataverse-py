- **Dependencies:** `urllib3 >= 2.8.0` is a direct runtime floor (three open advisories on every
  `dl_utils.download()` path below it) and the `tests`/`all` extras need `curl_cffi >= 0.15.0` (redirect
  SSRF). `recipe/meta.yaml` is re-mirrored from `pyproject.toml` (version 0.1.5; `scikit-learn`, `urllib3`
  and `tomli` were missing) and `tools/check_recipe_mirror.py` now diffs the two. (#751)
