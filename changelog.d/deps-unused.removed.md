- **Dependencies:** `matplotlib`, `pyjanitor`, `inflection` and `attrs` are no longer runtime dependencies
  (nothing imported them; `tests/test_import_sweep.py` imports every module with them blocked). `black`, `flake8`,
  `isort`, `pycln` and `pydocstyle` leave the `tests`/`all` extras; the `models` extra is empty (already base deps). (#752)
