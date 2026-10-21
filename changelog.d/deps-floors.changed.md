- **Dependencies:** `urllib3 >= 2.8.0` is a direct runtime floor (three open advisories below it) and the
  `tests`/`all` extras need `curl_cffi >= 0.15.0` (redirect SSRF). `recipe/meta.yaml` is re-mirrored from
  `pyproject.toml` (`scikit-learn`, `urllib3`, `tomli` were missing); `tools/check_recipe_mirror.py` diffs the two. (#751)
